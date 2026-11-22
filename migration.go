package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sql/*.sql
var migrations embed.FS

const (
	stageLegacy     = "legacy"
	stageExpanded   = "expanded"
	stageValidated  = "validated"
	stageContracted = "contracted"
)

var schemaName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

type progress struct {
	Stage      string `json:"stage"`
	SeedRows   int    `json:"seed_rows"`
	Cutoff     int64  `json:"cutoff_id"`
	Checkpoint int64  `json:"checkpoint_id"`
	Copied     int64  `json:"copied_rows"`
	Batches    int64  `json:"committed_batches"`
}

type store struct{ pool *pgxpool.Pool }

func openStore(ctx context.Context, dsn, schema string) (store, error) {
	if !schemaName.MatchString(schema) {
		return store{}, errors.New("invalid PostgreSQL schema name")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return store{}, errors.New("invalid DATABASE_URL")
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	cfg.MaxConns = 20
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return store{}, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return store{}, err
	}
	return store{pool: pool}, nil
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readProgress(ctx context.Context, db rowQuerier, lock bool) (progress, error) {
	query := `SELECT stage,seed_rows,cutoff_id,checkpoint_id,copied_rows,batches
	          FROM migration_progress WHERE singleton`
	if lock {
		query += " FOR UPDATE"
	}
	var p progress
	err := db.QueryRow(ctx, query).Scan(&p.Stage, &p.SeedRows, &p.Cutoff, &p.Checkpoint, &p.Copied, &p.Batches)
	return p, err
}

func setLockTimeout(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '500ms'")
	return err
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s store) init(ctx context.Context, seed int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := setLockTimeout(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || ':payments-migration', 0))"); err != nil {
		return err
	}
	if err := runSQL(ctx, tx, "init"); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `INSERT INTO migration_progress(stage, seed_rows)
	                             VALUES('legacy',$1) ON CONFLICT DO NOTHING`, seed)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		p, err := readProgress(ctx, tx, false)
		if err != nil {
			return err
		}
		if seed > 0 && seed != p.SeedRows {
			return errors.New("schema was already initialized with a different seed size")
		}
	} else if seed > 0 {
		_, err := tx.Exec(ctx, `INSERT INTO payments(external_id,amount_minor,currency,status)
			SELECT 'seed-' || n,1000+n % 100000,'USD',
			       (ARRAY['PENDING','CAPTURED','REFUNDED'])[1+n % 3]
			FROM generate_series(1,$1::int) AS n ORDER BY n`, seed)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func runSQL(ctx context.Context, tx pgx.Tx, name string) error {
	script, err := migrations.ReadFile("sql/" + name + ".sql")
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, string(script))
	return err
}

func (s store) transition(ctx context.Context, name string) error {
	var from, to string
	switch name {
	case "expand":
		from, to = stageLegacy, stageExpanded
	case "validate":
		from, to = stageExpanded, stageValidated
	case "contract":
		from, to = stageValidated, stageContracted
	default:
		return fmt.Errorf("unknown migration step %q", name)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := setLockTimeout(ctx, tx); err != nil {
		return err
	}
	p, err := readProgress(ctx, tx, true)
	if err != nil {
		return err
	}
	if p.Stage == to {
		return tx.Commit(ctx)
	}
	if p.Stage != from {
		return fmt.Errorf("%s requires stage %s, current stage is %s", name, from, p.Stage)
	}
	if name == "validate" && p.Checkpoint != p.Cutoff {
		return fmt.Errorf("backfill is incomplete at %d of %d", p.Checkpoint, p.Cutoff)
	}
	if err := runSQL(ctx, tx, name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s store) copyBatch(ctx context.Context, size int) (progress, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return progress{}, err
	}
	defer rollback(tx)
	if err := setLockTimeout(ctx, tx); err != nil {
		return progress{}, err
	}
	p, err := readProgress(ctx, tx, true)
	if err != nil {
		return p, err
	}
	if p.Stage == stageLegacy {
		return p, errors.New("expand the schema before starting the backfill")
	}
	if p.Checkpoint == p.Cutoff {
		return p, tx.Commit(ctx)
	}
	var end int64
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(id),$2) FROM (
			SELECT id FROM payments WHERE id > $1 AND id <= $2 ORDER BY id LIMIT $3
		) AS ids`, p.Checkpoint, p.Cutoff, size).Scan(&end)
	if err != nil {
		return p, err
	}
	result, err := tx.Exec(ctx, `UPDATE payments SET state=lower(status)
	                            WHERE id > $1 AND id <= $2 AND state IS NULL`, p.Checkpoint, end)
	if err != nil {
		return p, err
	}
	p.Checkpoint = end
	p.Copied += result.RowsAffected()
	p.Batches++
	_, err = tx.Exec(ctx, `UPDATE migration_progress SET checkpoint_id=$1,copied_rows=$2,batches=$3
	                      WHERE singleton`, p.Checkpoint, p.Copied, p.Batches)
	if err != nil {
		return p, err
	}
	return p, tx.Commit(ctx)
}

func (s store) backfill(ctx context.Context, size int, pause time.Duration) (progress, error) {
	for {
		p, err := s.copyBatch(ctx, size)
		if err == nil && p.Checkpoint == p.Cutoff {
			return p, nil
		}
		if err != nil {
			var dbError *pgconn.PgError
			if !errors.As(err, &dbError) || dbError.Code != "55P03" {
				return p, err
			}
			// A busy row can delay the batch. Its checkpoint still covers only committed work.
			if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
				return p, err
			}
			continue
		}
		if err := sleepContext(ctx, pause); err != nil {
			return p, err
		}
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

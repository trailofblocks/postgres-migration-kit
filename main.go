package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

const defaultDSN = "postgresql://migration:migration-local@127.0.0.1:55496/postgres?sslmode=disable"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migration-kit:", err)
		os.Exit(1)
	}
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("choose init, serve, expand, backfill, validate, status, contract, or benchmark")
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	var runCommand func(context.Context, store) (any, error)
	schema := env("MIGRATION_SCHEMA", "public")
	dsn := env("DATABASE_URL", defaultDSN)

	switch command {
	case "init":
		seed := flags.Int("seed", 0, "number of synthetic payments to seed during initial setup")
		runCommand = func(ctx context.Context, s store) (any, error) {
			if *seed < 0 || *seed > 2_000_000 {
				return nil, errors.New("seed must be between 0 and 2000000")
			}
			if _, err := s.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize()); err != nil {
				return nil, err
			}
			if err := s.init(ctx, *seed); err != nil {
				return nil, err
			}
			return readProgress(ctx, s.pool, false)
		}
	case "serve":
		version := flags.String("api", "v1", "payment API version: v1 or v2")
		listen := flags.String("listen", "127.0.0.1:8897", "HTTP listen address")
		runCommand = func(ctx context.Context, s store) (any, error) {
			if *version != "v1" && *version != "v2" {
				return nil, errors.New("api version must be v1 or v2")
			}
			a := paymentAPI{store: s, version: *version}
			p, err := readProgress(ctx, s.pool, false)
			if err != nil {
				return nil, err
			}
			if !a.ready(p.Stage) {
				return nil, fmt.Errorf("API %s cannot start in stage %s", *version, p.Stage)
			}
			return nil, serve(ctx, *listen, a.handler())
		}
	case "expand", "validate":
		runCommand = func(ctx context.Context, s store) (any, error) {
			if err := s.transition(ctx, command); err != nil {
				return nil, err
			}
			return readProgress(ctx, s.pool, false)
		}
	case "contract":
		confirmed := flags.Bool("confirm-v1-drained", false, "confirm v1 traffic and its rollback window are finished before dropping status")
		runCommand = func(ctx context.Context, s store) (any, error) {
			if !*confirmed {
				return nil, errors.New("contract drops v1 support and requires --confirm-v1-drained")
			}
			if err := s.transition(ctx, command); err != nil {
				return nil, err
			}
			return readProgress(ctx, s.pool, false)
		}
	case "backfill":
		size := flags.Int("batch-size", 400, "maximum number of existing IDs per transaction")
		pause := flags.Duration("pause", 0, "throttle between committed batches")
		once := flags.Bool("once", false, "commit a single batch")
		runCommand = func(ctx context.Context, s store) (any, error) {
			if *size < 1 || *size > 5000 || *pause < 0 {
				return nil, errors.New("batch size must be 1–5000 and pause must be nonnegative")
			}
			if *once {
				return s.copyBatch(ctx, *size)
			}
			return s.backfill(ctx, *size, *pause)
		}
	case "status":
		runCommand = func(ctx context.Context, s store) (any, error) {
			return readProgress(ctx, s.pool, false)
		}
	case "benchmark":
		var cfg benchmarkConfig
		flags.IntVar(&cfg.SeedRows, "seed", 20000, "synthetic payments to load before starting traffic")
		flags.IntVar(&cfg.Batch, "batch-size", 400, "maximum number of existing IDs per backfill transaction")
		flags.IntVar(&cfg.Rate, "rate", 200, "target HTTP write requests per second")
		flags.IntVar(&cfg.Clients, "clients", 8, "number of concurrent traffic workers")
		flags.DurationVar(&cfg.Hold, "hold", 600*time.Millisecond, "minimum observation time per phase")
		output := flags.String("output", "", "also write the JSON report to this path")
		if err := parseFlags(flags, args[1:]); err != nil {
			return err
		}
		report, err := runBenchmark(ctx, dsn, cfg)
		if report != nil {
			if writeErr := printResult(report, *output); writeErr != nil {
				return errors.Join(err, writeErr)
			}
		}
		return err
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	if err := parseFlags(flags, args[1:]); err != nil {
		return err
	}
	s, err := openStore(ctx, dsn, schema)
	if err != nil {
		return err
	}
	defer s.pool.Close()
	value, err := runCommand(ctx, s)
	if err != nil {
		return err
	}
	if value != nil {
		return printResult(value, "")
	}
	return nil
}

func parseFlags(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	return nil
}

func printResult(result any, output string) error {
	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if output != "" {
		if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(output, raw, 0o644); err != nil {
			return err
		}
	}
	_, err = os.Stdout.Write(raw)
	return err
}

func newServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler: handler, ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout: 4 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 40 * time.Second,
	}
}

func serve(ctx context.Context, address string, handler http.Handler) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := newServer(handler)
	ended := make(chan error, 1)
	go func() { ended <- server.Serve(listener) }()
	fmt.Fprintf(os.Stderr, "Payments API listening on %s\n", listener.Addr())
	select {
	case err := <-ended:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

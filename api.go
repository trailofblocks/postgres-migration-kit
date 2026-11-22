package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	paymentID       = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)
	currencyCode    = regexp.MustCompile(`^[A-Z]{3}$`)
	errAlreadyUsed  = errors.New("payment ID already has a different amount or currency")
	errInvalidState = errors.New("payment state transition is not allowed")
)

type payment struct {
	ID       string `json:"id"`
	Amount   int64  `json:"amount_minor"`
	Currency string `json:"currency"`
	State    string `json:"state"`
}

type paymentAPI struct {
	store
	version string
}

func (a paymentAPI) column() (column, value, read string) {
	if a.version == "v1" {
		return "status", "upper($1)", "lower(status)"
	}
	return "state", "$1", "state"
}

func scanPayment(row pgx.Row) (payment, error) {
	var p payment
	err := row.Scan(&p.ID, &p.Amount, &p.Currency, &p.State)
	return p, err
}

func (a paymentAPI) find(ctx context.Context, db rowQuerier, id string) (payment, error) {
	_, _, read := a.column()
	return scanPayment(db.QueryRow(ctx, "SELECT external_id,amount_minor,currency,"+read+
		" FROM payments WHERE external_id=$1", id))
}

func (a paymentAPI) create(ctx context.Context, p payment) (payment, bool, error) {
	column, _, read := a.column()
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return payment{}, false, err
	}
	defer rollback(tx)
	initial := "pending"
	if a.version == "v1" {
		initial = "PENDING"
	}
	saved, err := scanPayment(tx.QueryRow(ctx, "INSERT INTO payments (external_id,amount_minor,currency,"+column+
		") VALUES($1,$2,$3,$4) ON CONFLICT(external_id) DO NOTHING RETURNING external_id,amount_minor,currency,"+read,
		p.ID, p.Amount, p.Currency, initial))
	created := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		saved, err = a.find(ctx, tx, p.ID)
		if err == nil && (saved.Amount != p.Amount || saved.Currency != p.Currency) {
			return payment{}, false, errAlreadyUsed
		}
	}
	if err != nil {
		return payment{}, false, err
	}
	return saved, created, tx.Commit(ctx)
}

func (a paymentAPI) change(ctx context.Context, id, state string) (payment, error) {
	column, value, read := a.column()
	prior := "pending"
	if state == "refunded" {
		prior = "captured"
	}
	saved, err := scanPayment(a.pool.QueryRow(ctx, "UPDATE payments SET "+column+"="+value+
		" WHERE external_id=$2 AND "+read+"=$3 RETURNING external_id,amount_minor,currency,"+read,
		state, id, prior))
	if !errors.Is(err, pgx.ErrNoRows) {
		return saved, err
	}
	saved, err = a.find(ctx, a.pool, id)
	if err == nil && saved.State != state {
		err = errInvalidState
	}
	return saved, err
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func readInput(w http.ResponseWriter, r *http.Request, dst any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil || decoder.Decode(new(any)) != io.EOF {
		writeError(w, http.StatusBadRequest, "expected one JSON object with supported fields")
		return false
	}
	return true
}

func paymentError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		writeError(w, http.StatusNotFound, "payment not found")
	case errors.Is(err, errAlreadyUsed), errors.Is(err, errInvalidState):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, "payment storage is temporarily unavailable")
	}
}

func (a paymentAPI) ready(stage string) bool {
	if a.version == "v1" {
		return stage == stageLegacy || stage == stageExpanded || stage == stageValidated
	}
	return stage == stageValidated || stage == stageContracted
}

func (a paymentAPI) handler() http.Handler {
	router := http.NewServeMux()
	router.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		p, err := readProgress(r.Context(), a.pool, false)
		if err != nil || !a.ready(p.Stage) {
			writeError(w, http.StatusServiceUnavailable, "schema is not ready for this API version")
			return
		}
		writeJSON(w, http.StatusOK, p)
	})
	router.HandleFunc("POST /payments", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID       string `json:"id"`
			Amount   int64  `json:"amount_minor"`
			Currency string `json:"currency"`
		}
		if !readInput(w, r, &req) {
			return
		}
		if !paymentID.MatchString(req.ID) || !currencyCode.MatchString(req.Currency) || req.Amount < 1 || req.Amount > 9_000_000_000_000 {
			writeError(w, http.StatusBadRequest, "supply a valid payment ID, positive amount in minor units, and three-letter currency")
			return
		}
		saved, created, err := a.create(r.Context(), payment{ID: req.ID, Amount: req.Amount, Currency: req.Currency})
		if err != nil {
			paymentError(w, err)
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		writeJSON(w, status, saved)
	})
	router.HandleFunc("GET /payments/{id}", func(w http.ResponseWriter, r *http.Request) {
		result, err := a.find(r.Context(), a.pool, r.PathValue("id"))
		if err != nil {
			paymentError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	router.HandleFunc("PUT /payments/{id}/state", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			State string `json:"state"`
		}
		if !readInput(w, r, &req) {
			return
		}
		if req.State != "captured" && req.State != "refunded" {
			writeError(w, http.StatusBadRequest, "state must be captured or refunded")
			return
		}
		result, err := a.change(r.Context(), r.PathValue("id"), req.State)
		if err != nil {
			paymentError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Payments-API", a.version)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		router.ServeHTTP(w, r.WithContext(ctx))
	})
}

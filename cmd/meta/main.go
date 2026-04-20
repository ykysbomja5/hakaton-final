package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	port := getenv("PORT", "8084")
	dsn := os.Getenv("PG_DSN")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := shared.OpenPostgres(ctx, dsn)
	if err != nil {
		log.Printf("meta service started without postgres connection: %v", err)
	}
	if pool != nil {
		defer pool.Close()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/meta/schema", func(w http.ResponseWriter, r *http.Request) {
		if shared.HandlePreflight(w, r) {
			return
		}
		if r.Method != http.MethodGet {
			shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		layer := shared.DefaultSemanticLayer()
		if pool != nil {
			mergeSemanticTerms(r.Context(), pool, &layer)
		}
		shared.WriteJSON(w, http.StatusOK, layer)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "meta"})
	})

	log.Printf("meta listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func mergeSemanticTerms(ctx context.Context, pool *pgxpool.Pool, layer *shared.SemanticLayer) {
	rows, err := pool.Query(ctx, `
		select term, kind, canonical_value, description
		from app.semantic_terms
		order by kind, term
	`)
	if err != nil {
		return
	}
	defer rows.Close()

	terms := make([]shared.BusinessTerm, 0)
	for rows.Next() {
		var term shared.BusinessTerm
		if err := rows.Scan(&term.Term, &term.Kind, &term.Canonical, &term.Description); err != nil {
			return
		}
		terms = append(terms, term)
	}
	if len(terms) > 0 {
		layer.Terms = terms
	}
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

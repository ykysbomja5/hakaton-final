package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5/pgxpool"
)

type application struct {
	db       *pgxpool.Pool
	client   *http.Client
	queryURL string
}

func main() {
	port := getenv("PORT", "8083")
	dsn := os.Getenv("PG_DSN")
	if strings.TrimSpace(dsn) == "" {
		log.Fatal("PG_DSN is required for reports service")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := shared.OpenPostgres(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect postgres: %v", err)
	}
	defer pool.Close()

	app := application{
		db:       pool,
		client:   &http.Client{Timeout: 25 * time.Second},
		queryURL: getenv("QUERY_SERVICE_URL", "http://localhost:8081"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/reports", app.handleReports)
	mux.HandleFunc("/api/v1/reports/", app.handleReportActions)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "reports"})
	})

	log.Printf("reports listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func (app application) handleReports(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	switch r.Method {
	case http.MethodPost:
		app.saveReport(w, r)
	case http.MethodGet:
		app.listReports(w, r)
	default:
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (app application) handleReportActions(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/reports/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[1] != "run" {
		shared.WriteError(w, http.StatusNotFound, "report route not found")
		return
	}

	reportID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid report id")
		return
	}
	app.runReport(w, r, reportID)
}

func (app application) saveReport(w http.ResponseWriter, r *http.Request) {
	var req shared.SaveReportRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.QueryText) == "" || strings.TrimSpace(req.SQLText) == "" {
		shared.WriteError(w, http.StatusBadRequest, "name, query_text and sql_text are required")
		return
	}

	var report shared.SavedReport
	report.Name = req.Name
	report.QueryText = req.QueryText
	report.SQLText = req.SQLText
	report.Intent = req.Intent

	err := app.db.QueryRow(r.Context(), `
		insert into app.saved_reports (name, query_text, sql_text, intent)
		values ($1, $2, $3, $4::jsonb)
		returning id, created_at, updated_at
	`, req.Name, req.QueryText, req.SQLText, shared.MustJSON(req.Intent)).Scan(&report.ID, &report.CreatedAt, &report.UpdatedAt)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusCreated, report)
}

func (app application) listReports(w http.ResponseWriter, r *http.Request) {
	rows, err := app.db.Query(r.Context(), `
		select id, name, query_text, sql_text, intent, created_at, updated_at
		from app.saved_reports
		order by updated_at desc
	`)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer rows.Close()

	reports := make([]shared.SavedReport, 0)
	for rows.Next() {
		var report shared.SavedReport
		var intentRaw []byte
		if err := rows.Scan(&report.ID, &report.Name, &report.QueryText, &report.SQLText, &intentRaw, &report.CreatedAt, &report.UpdatedAt); err != nil {
			shared.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
		_ = json.Unmarshal(intentRaw, &report.Intent)
		reports = append(reports, report)
	}
	shared.WriteJSON(w, http.StatusOK, reports)
}

func (app application) runReport(w http.ResponseWriter, r *http.Request, reportID int64) {
	var queryText string
	if err := app.db.QueryRow(r.Context(), `select query_text from app.saved_reports where id = $1`, reportID).Scan(&queryText); err != nil {
		shared.WriteError(w, http.StatusNotFound, "report not found")
		return
	}

	payload := shared.QueryRequest{Text: queryText}
	rawBody, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, app.queryURL+"/api/v1/query/run", bytes.NewReader(rawBody))
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")

	started := time.Now()
	resp, err := app.client.Do(req)
	if err != nil {
		app.logExecution(r.Context(), reportID, "failed", 0, err.Error())
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		app.logExecution(r.Context(), reportID, "failed", 0, string(body))
		shared.WriteError(w, http.StatusBadGateway, string(body))
		return
	}

	var runResp shared.RunResponse
	if err := json.Unmarshal(body, &runResp); err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	app.logExecution(r.Context(), reportID, "ok", runResp.Result.Count, "")
	log.Printf("report %d executed in %s", reportID, time.Since(started))
	shared.WriteJSON(w, http.StatusOK, runResp)
}

func (app application) logExecution(ctx context.Context, reportID int64, status string, rowCount int, errorText string) {
	_, _ = app.db.Exec(ctx, `
		insert into app.report_runs (report_id, status, row_count, error_text)
		values ($1, $2, $3, $4)
	`, reportID, status, rowCount, nullableText(errorText))
}

func nullableText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type application struct {
	auditDB *pgxpool.Pool
	execDB  *pgxpool.Pool
	client  *http.Client
	llmURL  string
	metaURL string
}

func main() {
	if err := shared.LoadDotEnv(".env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}

	port := getenv("PORT", getenv("QUERY_PORT", "8081"))
	auditDSN := os.Getenv("PG_DSN")
	if strings.TrimSpace(auditDSN) == "" {
		log.Fatal("PG_DSN is required for query service logging")
	}

	execDSN := os.Getenv("PG_READONLY_DSN")
	if strings.TrimSpace(execDSN) == "" {
		log.Fatal("PG_READONLY_DSN is required so generated SQL runs under a read-only database user")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	auditPool, err := shared.OpenPostgres(ctx, auditDSN)
	if err != nil {
		log.Fatalf("failed to connect postgres for logging: %v", err)
	}
	defer auditPool.Close()

	execPool, err := openVerifiedReadOnlyPool(ctx, auditPool, execDSN)
	if err != nil {
		log.Fatalf("failed to connect postgres with read-only user: %v", err)
	}
	defer execPool.Close()

	app := application{
		auditDB: auditPool,
		execDB:  execPool,
		client:  &http.Client{Timeout: 25 * time.Second},
		llmURL:  getenv("LLM_SERVICE_URL", "http://localhost:8082"),
		metaURL: getenv("META_SERVICE_URL", "http://localhost:8084"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query/parse", app.handleParse)
	mux.HandleFunc("/api/v1/query/run", app.handleRun)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "query"})
	})

	log.Printf("query listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func (app application) handleParse(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req shared.QueryRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	plan, err := app.generatePlan(r.Context(), req.Text)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	shared.WriteJSON(w, http.StatusOK, shared.ParseResponse{
		Intent:        plan.Intent,
		Preview:       plan.Preview,
		SemanticLayer: plan.SemanticLayer,
		SQL:           plan.SQL,
		Provider:      plan.Provider,
	})
}

func (app application) handleRun(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req shared.QueryRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	plan, err := app.generatePlan(r.Context(), req.Text)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	if strings.TrimSpace(plan.SQL) == "" {
		shared.WriteJSON(w, http.StatusOK, shared.RunResponse{
			Intent:        plan.Intent,
			Preview:       plan.Preview,
			SQL:           "",
			Result:        shared.QueryResult{},
			Chart:         shared.ChartSpec{},
			SemanticLayer: plan.SemanticLayer,
			Provider:      plan.Provider,
		})
		return
	}

	started := time.Now()
	result, err := app.executeQuery(r.Context(), plan.SQL)
	if err != nil {
		app.logRun(r.Context(), req.Text, plan.Intent, plan.SQL, "failed", time.Since(started), err.Error())
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	app.logRun(r.Context(), req.Text, plan.Intent, plan.SQL, "ok", time.Since(started), "")
	shared.WriteJSON(w, http.StatusOK, shared.RunResponse{
		Intent:        plan.Intent,
		Preview:       plan.Preview,
		SQL:           plan.SQL,
		Result:        result,
		Chart:         chooseChart(plan.Intent, result),
		SemanticLayer: plan.SemanticLayer,
		Provider:      plan.Provider,
	})
}

type generatedPlan struct {
	Intent        shared.Intent
	Preview       shared.QueryPreview
	SemanticLayer shared.SemanticLayer
	SQL           string
	Provider      string
}

func (app application) generatePlan(ctx context.Context, text string) (generatedPlan, error) {
	layer, err := app.fetchSemanticLayer(ctx)
	if err != nil {
		return generatedPlan{}, err
	}

	payload := shared.SQLGenerationRequest{
		Text:          text,
		SemanticLayer: layer,
	}
	rawBody, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, app.llmURL+"/v1/query", bytes.NewReader(rawBody))
	if err != nil {
		return generatedPlan{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.client.Do(req)
	if err != nil {
		return generatedPlan{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return generatedPlan{}, fmt.Errorf("llm service error: %s", string(body))
	}

	var llmResp shared.SQLGenerationResponse
	if err := json.NewDecoder(resp.Body).Decode(&llmResp); err != nil {
		return generatedPlan{}, err
	}

	llmResp.SQL = strings.TrimSpace(llmResp.SQL)
	if llmResp.SQL != "" {
		if err := shared.ValidateGeneratedSQL(llmResp.SQL); err != nil {
			return generatedPlan{}, err
		}
	}

	preview := shared.BuildPreview(llmResp.Intent, layer)
	return generatedPlan{
		Intent:        llmResp.Intent,
		Preview:       preview,
		SemanticLayer: layer,
		SQL:           llmResp.SQL,
		Provider:      llmResp.Provider,
	}, nil
}

func (app application) fetchSemanticLayer(ctx context.Context) (shared.SemanticLayer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, app.metaURL+"/api/v1/meta/schema", nil)
	if err != nil {
		return shared.SemanticLayer{}, err
	}
	resp, err := app.client.Do(req)
	if err != nil {
		return shared.SemanticLayer{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return shared.SemanticLayer{}, fmt.Errorf("meta service error: %s", string(body))
	}

	var layer shared.SemanticLayer
	if err := json.NewDecoder(resp.Body).Decode(&layer); err != nil {
		return shared.SemanticLayer{}, err
	}
	return layer, nil
}

func verifyReadOnlyConnection(ctx context.Context, pool *pgxpool.Pool) error {
	var currentUser string
	var canRead bool
	var hasAppSchemaUsage bool
	err := pool.QueryRow(ctx, `
		select
			current_user,
			has_table_privilege(current_user, 'analytics.v_ride_metrics', 'SELECT'),
			has_schema_privilege(current_user, 'app', 'USAGE')
	`).Scan(&currentUser, &canRead, &hasAppSchemaUsage)
	if err != nil {
		return err
	}
	if !canRead {
		return fmt.Errorf("user %q cannot read analytics.v_ride_metrics", currentUser)
	}
	if hasAppSchemaUsage {
		return fmt.Errorf("user %q has access to schema app, expected an isolated read-only user", currentUser)
	}
	return nil
}

func openVerifiedReadOnlyPool(ctx context.Context, auditPool *pgxpool.Pool, execDSN string) (*pgxpool.Pool, error) {
	execPool, err := shared.OpenPostgres(ctx, execDSN)
	if err != nil {
		repaired, repairErr := tryRepairReadOnlyRole(ctx, auditPool, execDSN, err)
		if repairErr != nil {
			log.Printf("read-only role repair attempt failed: %v", repairErr)
		}
		if repaired {
			execPool, err = shared.OpenPostgres(ctx, execDSN)
		}
		if err != nil {
			return nil, err
		}
	}

	if err := verifyReadOnlyConnection(ctx, execPool); err != nil {
		execPool.Close()
		repaired, repairErr := repairReadOnlyRole(ctx, auditPool, execDSN)
		if repairErr != nil {
			return nil, err
		}
		if !repaired {
			return nil, err
		}

		execPool, err = shared.OpenPostgres(ctx, execDSN)
		if err != nil {
			return nil, err
		}
		if err := verifyReadOnlyConnection(ctx, execPool); err != nil {
			execPool.Close()
			return nil, err
		}
	}

	return execPool, nil
}

func tryRepairReadOnlyRole(ctx context.Context, auditPool *pgxpool.Pool, execDSN string, connectErr error) (bool, error) {
	if !looksLikeReadOnlyAuthError(connectErr) {
		return false, nil
	}
	return repairReadOnlyRole(ctx, auditPool, execDSN)
}

func repairReadOnlyRole(ctx context.Context, auditPool *pgxpool.Pool, execDSN string) (bool, error) {
	cfg, err := pgxpool.ParseConfig(execDSN)
	if err != nil {
		return false, err
	}

	roleName := strings.TrimSpace(cfg.ConnConfig.User)
	rolePassword := cfg.ConnConfig.Password
	databaseName := strings.TrimSpace(cfg.ConnConfig.Database)
	if roleName == "" {
		return false, fmt.Errorf("PG_READONLY_DSN must include a username")
	}
	if rolePassword == "" {
		return false, fmt.Errorf("PG_READONLY_DSN must include a password so the read-only role can be provisioned")
	}

	statements := []string{
		fmt.Sprintf(`
do $$
begin
    if not exists (select 1 from pg_roles where rolname = %s) then
        execute format('create role %%I login password %%L', %s, %s);
    else
        execute format('alter role %%I with login password %%L', %s, %s);
    end if;
end $$;`, quoteSQLLiteral(roleName), quoteSQLLiteral(roleName), quoteSQLLiteral(rolePassword), quoteSQLLiteral(roleName), quoteSQLLiteral(rolePassword)),
	}

	if databaseName != "" {
		statements = append(statements, fmt.Sprintf("grant connect on database %s to %s", quoteSQLIdentifier(databaseName), quoteSQLIdentifier(roleName)))
	}
	statements = append(statements,
		fmt.Sprintf("grant usage on schema analytics to %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant select on analytics.ride_metrics_daily to %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant select on analytics.v_ride_metrics to %s", quoteSQLIdentifier(roleName)),
	)

	for _, stmt := range statements {
		if _, err := auditPool.Exec(ctx, stmt); err != nil {
			return false, err
		}
	}

	log.Printf("read-only role %q was repaired from PG_DSN and PG_READONLY_DSN settings", roleName)
	return true, nil
}

func looksLikeReadOnlyAuthError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "28p01") ||
		strings.Contains(text, "password authentication failed") ||
		strings.Contains(text, "failed sasl auth") ||
		strings.Contains(text, "authentication failed")
}

func quoteSQLIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func (app application) executeQuery(ctx context.Context, sqlText string) (shared.QueryResult, error) {
	execCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	rows, err := app.execDB.Query(execCtx, shared.WrapQueryForExecution(sqlText))
	if err != nil {
		return shared.QueryResult{}, err
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, 0, len(fieldDescriptions))
	for _, field := range fieldDescriptions {
		columns = append(columns, string(field.Name))
	}

	result := shared.QueryResult{Columns: columns, Rows: make([][]string, 0)}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return shared.QueryResult{}, err
		}
		row := make([]string, len(values))
		for i, value := range values {
			row[i] = formatCell(value)
		}
		result.Rows = append(result.Rows, row)
	}
	result.Count = len(result.Rows)
	return result, rows.Err()
}

func chooseChart(intent shared.Intent, result shared.QueryResult) shared.ChartSpec {
	if len(result.Columns) < 2 {
		return shared.ChartSpec{Type: "metric"}
	}
	switch intent.GroupBy {
	case "day", "week", "month":
		return shared.ChartSpec{Type: "line", XKey: result.Columns[0], YKey: result.Columns[1]}
	default:
		return shared.ChartSpec{Type: "bar", XKey: result.Columns[0], YKey: result.Columns[1]}
	}
}

func (app application) logRun(ctx context.Context, queryText string, intent shared.Intent, sqlText, status string, latency time.Duration, errorText string) {
	_, _ = app.auditDB.Exec(ctx, `
		insert into app.query_logs (query_text, intent, sql_text, confidence, status, latency_ms, error_text)
		values ($1, $2::jsonb, $3, $4, $5, $6, $7)
	`, queryText, shared.MustJSON(intent), sqlText, intent.Confidence, status, latency.Milliseconds(), nullableText(errorText))
}

func formatCell(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case time.Time:
		return v.Format("2006-01-02")
	case float64:
		return strconv.FormatFloat(v, 'f', 2, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', 2, 64)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case pgtype.Numeric:
		return formatNumeric(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func formatNumeric(value pgtype.Numeric) string {
	if !value.Valid || value.Int == nil {
		return ""
	}

	if value.NaN {
		return "NaN"
	}

	digits := value.Int.String()
	if value.Exp == 0 {
		return digits
	}

	negative := strings.HasPrefix(digits, "-")
	if negative {
		digits = strings.TrimPrefix(digits, "-")
	}

	var rendered string
	switch {
	case value.Exp > 0:
		rendered = digits + strings.Repeat("0", int(value.Exp))
	default:
		scale := int(-value.Exp)
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		split := len(digits) - scale
		rendered = digits[:split] + "." + digits[split:]
	}

	rendered = trimNumeric(rendered)
	if negative && rendered != "0" {
		return "-" + rendered
	}
	return rendered
}

func trimNumeric(value string) string {
	if !strings.Contains(value, ".") {
		return value
	}
	value = strings.TrimRight(value, "0")
	value = strings.TrimRight(value, ".")
	if value == "" || value == "-" {
		return "0"
	}
	return value
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

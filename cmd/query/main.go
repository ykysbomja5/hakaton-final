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
	db      *pgxpool.Pool
	client  *http.Client
	llmURL  string
	metaURL string
}

func main() {
	port := getenv("PORT", "8081")
	dsn := os.Getenv("PG_DSN")
	if strings.TrimSpace(dsn) == "" {
		log.Fatal("PG_DSN is required for query service")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := shared.OpenPostgres(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect postgres: %v", err)
	}
	defer pool.Close()

	app := application{
		db:      pool,
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

	intent, layer, preview, err := app.parseIntent(r.Context(), req.Text)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	shared.WriteJSON(w, http.StatusOK, shared.ParseResponse{
		Intent:        intent,
		Preview:       preview,
		SemanticLayer: layer,
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

	intent, layer, preview, err := app.parseIntent(r.Context(), req.Text)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	if intent.Metric == "" || intent.Clarification != "" && intent.Confidence < 0.4 {
		shared.WriteJSON(w, http.StatusOK, shared.RunResponse{
			Intent:        intent,
			Preview:       preview,
			SQL:           "",
			Result:        shared.QueryResult{},
			Chart:         shared.ChartSpec{},
			SemanticLayer: layer,
		})
		return
	}

	sqlText, args, err := buildSQL(intent)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateSQL(sqlText); err != nil {
		shared.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	started := time.Now()
	result, err := app.executeQuery(r.Context(), sqlText, args...)
	if err != nil {
		app.logRun(r.Context(), req.Text, intent, sqlText, "failed", time.Since(started), err.Error())
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	app.logRun(r.Context(), req.Text, intent, sqlText, "ok", time.Since(started), "")
	shared.WriteJSON(w, http.StatusOK, shared.RunResponse{
		Intent:        intent,
		Preview:       preview,
		SQL:           sqlText,
		Result:        result,
		Chart:         chooseChart(intent, result),
		SemanticLayer: layer,
	})
}

func (app application) parseIntent(ctx context.Context, text string) (shared.Intent, shared.SemanticLayer, shared.QueryPreview, error) {
	layer, err := app.fetchSemanticLayer(ctx)
	if err != nil {
		return shared.Intent{}, shared.SemanticLayer{}, shared.QueryPreview{}, err
	}

	payload := shared.IntentRequest{
		Text:          text,
		SemanticLayer: layer,
	}
	rawBody, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, app.llmURL+"/v1/intent", bytes.NewReader(rawBody))
	if err != nil {
		return shared.Intent{}, shared.SemanticLayer{}, shared.QueryPreview{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.client.Do(req)
	if err != nil {
		return shared.Intent{}, shared.SemanticLayer{}, shared.QueryPreview{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return shared.Intent{}, shared.SemanticLayer{}, shared.QueryPreview{}, fmt.Errorf("llm service error: %s", string(body))
	}

	var llmResp shared.IntentResponse
	if err := json.NewDecoder(resp.Body).Decode(&llmResp); err != nil {
		return shared.Intent{}, shared.SemanticLayer{}, shared.QueryPreview{}, err
	}

	preview := shared.BuildPreview(llmResp.Intent, layer)
	return llmResp.Intent, layer, preview, nil
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

func buildSQL(intent shared.Intent) (string, []any, error) {
	metricExpr, metricAlias, err := metricSQL(intent.Metric)
	if err != nil {
		return "", nil, err
	}

	args := []any{intent.Period.From, intent.Period.To}
	where := []string{"stat_date between $1 and $2"}
	for _, filter := range intent.Filters {
		column, err := filterColumn(filter.Field)
		if err != nil {
			return "", nil, err
		}
		args = append(args, filter.Value)
		where = append(where, fmt.Sprintf("%s = $%d", column, len(args)))
	}

	selectParts := []string{}
	groupParts := []string{}
	orderBy := metricAlias + " " + normalizeSort(intent.Sort)
	if intent.GroupBy != "" {
		groupExpr, alias, err := groupSQL(intent.GroupBy)
		if err != nil {
			return "", nil, err
		}
		selectParts = append(selectParts, fmt.Sprintf("%s as %s", groupExpr, alias))
		selectParts = append(selectParts, fmt.Sprintf("%s as %s", metricExpr, metricAlias))
		groupParts = append(groupParts, groupExpr)
		if intent.GroupBy == "day" || intent.GroupBy == "week" || intent.GroupBy == "month" {
			orderBy = alias + " asc"
		}
	} else {
		selectParts = append(selectParts, fmt.Sprintf("%s as %s", metricExpr, metricAlias))
	}

	sqlBuilder := strings.Builder{}
	sqlBuilder.WriteString("select ")
	sqlBuilder.WriteString(strings.Join(selectParts, ", "))
	sqlBuilder.WriteString(" from analytics.v_ride_metrics where ")
	sqlBuilder.WriteString(strings.Join(where, " and "))
	if len(groupParts) > 0 {
		sqlBuilder.WriteString(" group by ")
		sqlBuilder.WriteString(strings.Join(groupParts, ", "))
	}
	sqlBuilder.WriteString(" order by ")
	sqlBuilder.WriteString(orderBy)
	sqlBuilder.WriteString(fmt.Sprintf(" limit %d", normalizeLimit(intent.Limit)))
	return sqlBuilder.String(), args, nil
}

func metricSQL(metric string) (string, string, error) {
	switch metric {
	case "completed_rides":
		return "sum(completed_rides)", "metric_value", nil
	case "total_rides":
		return "sum(total_rides)", "metric_value", nil
	case "cancellations":
		return "sum(cancelled_rides)", "metric_value", nil
	case "revenue":
		return "round(sum(gross_revenue_rub)::numeric, 2)", "metric_value", nil
	case "avg_fare":
		return "round(sum(gross_revenue_rub) / nullif(sum(completed_rides), 0), 2)", "metric_value", nil
	case "active_drivers":
		return "max(active_drivers)", "metric_value", nil
	default:
		return "", "", fmt.Errorf("unsupported metric %q", metric)
	}
}

func groupSQL(groupBy string) (string, string, error) {
	switch groupBy {
	case "city":
		return "city", "group_value", nil
	case "service_class":
		return "service_class", "group_value", nil
	case "source_channel":
		return "source_channel", "group_value", nil
	case "driver_segment":
		return "driver_segment", "group_value", nil
	case "day":
		return "date_trunc('day', stat_date)::date", "period_value", nil
	case "week":
		return "date_trunc('week', stat_date)::date", "period_value", nil
	case "month":
		return "date_trunc('month', stat_date)::date", "period_value", nil
	default:
		return "", "", fmt.Errorf("unsupported group_by %q", groupBy)
	}
}

func filterColumn(field string) (string, error) {
	switch field {
	case "city":
		return "city", nil
	case "service_class":
		return "service_class", nil
	case "source_channel":
		return "source_channel", nil
	case "driver_segment":
		return "driver_segment", nil
	default:
		return "", fmt.Errorf("unsupported filter field %q", field)
	}
}

func validateSQL(sqlText string) error {
	lower := strings.ToLower(strings.TrimSpace(sqlText))
	if !strings.HasPrefix(lower, "select ") {
		return fmt.Errorf("only select queries are allowed")
	}
	for _, forbidden := range []string{" insert ", " update ", " delete ", " drop ", " alter ", " truncate ", " create "} {
		if strings.Contains(" "+lower+" ", forbidden) {
			return fmt.Errorf("query did not pass security validation")
		}
	}
	return nil
}

func (app application) executeQuery(ctx context.Context, sqlText string, args ...any) (shared.QueryResult, error) {
	rows, err := app.db.Query(ctx, sqlText, args...)
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
	_, _ = app.db.Exec(ctx, `
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

func normalizeSort(sort string) string {
	if strings.ToLower(sort) == "asc" {
		return "asc"
	}
	return "desc"
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return 12
	}
	if limit > 100 {
		return 100
	}
	return limit
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

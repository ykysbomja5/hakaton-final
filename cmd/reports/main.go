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
	db       *pgxpool.Pool
	client   *http.Client
	queryURL string
	location *time.Location
}

type templateRecord struct {
	shared.ReportTemplate
	lastScheduledFor *time.Time
}

func main() {
	if err := shared.LoadDotEnv(".env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}

	port := getenv("PORT", getenv("REPORTS_PORT", "8083"))
	dsn := os.Getenv("PG_DSN")
	if strings.TrimSpace(dsn) == "" {
		log.Fatal("PG_DSN is required for reports service")
	}

	location := time.Local
	if envTZ := strings.TrimSpace(os.Getenv("APP_TIMEZONE")); envTZ != "" {
		if loaded, err := time.LoadLocation(envTZ); err == nil {
			location = loaded
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := shared.OpenPostgres(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect postgres: %v", err)
	}
	defer pool.Close()

	if err := ensureDatabaseObjects(ctx, pool); err != nil {
		log.Fatalf("failed to prepare reports schema: %v", err)
	}

	app := application{
		db:       pool,
		client:   &http.Client{Timeout: 25 * time.Second},
		queryURL: getenv("QUERY_SERVICE_URL", "http://localhost:8081"),
		location: location,
	}
	go app.startTemplateScheduler()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/reports/export", app.handleDirectExport)
	mux.HandleFunc("/api/v1/reports/templates", app.handleTemplates)
	mux.HandleFunc("/api/v1/reports/templates/", app.handleTemplateActions)
	mux.HandleFunc("/api/v1/reports", app.handleReports)
	mux.HandleFunc("/api/v1/reports/", app.handleReportActions)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "reports"})
	})

	log.Printf("reports listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func ensureDatabaseObjects(ctx context.Context, db *pgxpool.Pool) error {
	statements := []string{
		`create table if not exists app.report_templates (
			id bigserial primary key,
			name text not null,
			description text not null default '',
			query_text text not null,
			schedule_enabled boolean not null default false,
			schedule_day_of_week integer,
			schedule_hour integer,
			schedule_minute integer,
			schedule_timezone text not null default 'Europe/Moscow',
			last_run_at timestamptz,
			last_scheduled_for timestamptz,
			last_status text not null default 'idle',
			last_error_text text,
			last_result_count integer not null default 0,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now(),
			constraint report_templates_schedule_day check (schedule_day_of_week is null or schedule_day_of_week between 0 and 6),
			constraint report_templates_schedule_hour check (schedule_hour is null or schedule_hour between 0 and 23),
			constraint report_templates_schedule_minute check (schedule_minute is null or schedule_minute between 0 and 59)
		)`,
		`alter table app.saved_reports add column if not exists preview_json jsonb`,
		`alter table app.saved_reports add column if not exists result_json jsonb`,
		`alter table app.saved_reports add column if not exists provider text`,
		`alter table app.saved_reports add column if not exists source text not null default 'manual'`,
		`alter table app.saved_reports add column if not exists template_id bigint`,
		`create index if not exists idx_saved_reports_template_id on app.saved_reports (template_id, updated_at desc)`,
		`create index if not exists idx_report_templates_schedule on app.report_templates (schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute)`,
		`do $$
		begin
			if not exists (
				select 1
				from pg_constraint
				where conname = 'saved_reports_template_id_fkey'
			) then
				alter table app.saved_reports
					add constraint saved_reports_template_id_fkey
					foreign key (template_id) references app.report_templates(id) on delete set null;
			end if;
		end $$`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
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

func (app application) handleTemplates(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		app.listTemplates(w, r)
	case http.MethodPost:
		app.createTemplate(w, r)
	default:
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (app application) handleTemplateActions(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/reports/templates/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		shared.WriteError(w, http.StatusNotFound, "template route not found")
		return
	}

	templateID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid template id")
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodPut:
			app.updateTemplate(w, r, templateID)
		case http.MethodDelete:
			app.deleteTemplate(w, r, templateID)
		default:
			shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	if len(parts) == 2 && parts[1] == "run" && r.Method == http.MethodPost {
		app.runTemplateNow(w, r, templateID)
		return
	}

	shared.WriteError(w, http.StatusNotFound, "template route not found")
}

func (app application) handleReportActions(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/reports/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		shared.WriteError(w, http.StatusNotFound, "report route not found")
		return
	}

	reportID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid report id")
		return
	}

	if len(parts) == 1 {
		if r.Method == http.MethodDelete {
			app.deleteReport(w, r, reportID)
			return
		}
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if len(parts) != 2 {
		shared.WriteError(w, http.StatusNotFound, "report route not found")
		return
	}

	switch {
	case parts[1] == "run" && r.Method == http.MethodPost:
		app.runReport(w, r, reportID)
	case parts[1] == "export" && r.Method == http.MethodGet:
		app.exportSavedReport(w, r, reportID)
	default:
		shared.WriteError(w, http.StatusNotFound, "report route not found")
	}
}

func (app application) handleDirectExport(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format != "pdf" && format != "docx" {
		shared.WriteError(w, http.StatusBadRequest, "format must be pdf or docx")
		return
	}

	var req shared.ExportReportRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		req.Name = "Аналитический отчет"
	}

	payload := exportPayload{
		Name:      req.Name,
		QueryText: req.QueryText,
		Run:       req.Run,
		CreatedAt: time.Now().In(app.location),
	}
	app.writeExport(w, payload, format)
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

	report, err := app.saveRunSnapshot(r.Context(), req.Name, req.QueryText, shared.RunResponse{
		Intent:   req.Intent,
		Preview:  req.Preview,
		SQL:      req.SQLText,
		Result:   req.Result,
		Provider: req.Provider,
	}, coalesceString(req.Source, "manual"), req.TemplateID)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusCreated, report)
}

func (app application) listReports(w http.ResponseWriter, r *http.Request) {
	reports, err := app.fetchSavedReports(r.Context())
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, reports)
}

func (app application) runReport(w http.ResponseWriter, r *http.Request, reportID int64) {
	report, err := app.fetchSavedReportByID(r.Context(), reportID)
	if err != nil {
		shared.WriteError(w, http.StatusNotFound, "report not found")
		return
	}

	started := time.Now()
	runResp, err := app.executeQueryText(r.Context(), report.QueryText)
	if err != nil {
		app.logExecution(r.Context(), reportID, "failed", 0, err.Error())
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	app.logExecution(r.Context(), reportID, "ok", runResp.Result.Count, "")
	log.Printf("report %d executed in %s", reportID, time.Since(started))
	shared.WriteJSON(w, http.StatusOK, runResp)
}

func (app application) deleteReport(w http.ResponseWriter, r *http.Request, reportID int64) {
	tag, err := app.db.Exec(r.Context(), `delete from app.saved_reports where id = $1`, reportID)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		shared.WriteError(w, http.StatusNotFound, "report not found")
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

func (app application) exportSavedReport(w http.ResponseWriter, r *http.Request, reportID int64) {
	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format != "pdf" && format != "docx" {
		shared.WriteError(w, http.StatusBadRequest, "format must be pdf or docx")
		return
	}

	report, err := app.fetchSavedReportByID(r.Context(), reportID)
	if err != nil {
		shared.WriteError(w, http.StatusNotFound, "report not found")
		return
	}

	run := shared.RunResponse{
		Intent:   report.Intent,
		Preview:  report.Preview,
		SQL:      report.SQLText,
		Result:   report.Result,
		Provider: report.Provider,
	}
	if len(run.Result.Columns) == 0 && strings.TrimSpace(report.QueryText) != "" {
		run, err = app.executeQueryText(r.Context(), report.QueryText)
		if err != nil {
			shared.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
	}

	payload := exportPayload{
		Name:      report.Name,
		QueryText: report.QueryText,
		Run:       run,
		CreatedAt: time.Now().In(app.location),
	}
	app.writeExport(w, payload, format)
}

func (app application) listTemplates(w http.ResponseWriter, r *http.Request) {
	templates, err := app.fetchTemplates(r.Context())
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, templates)
}

func (app application) createTemplate(w http.ResponseWriter, r *http.Request) {
	var req shared.UpsertReportTemplateRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	template, err := app.upsertTemplate(r.Context(), 0, req)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusCreated, template)
}

func (app application) updateTemplate(w http.ResponseWriter, r *http.Request, templateID int64) {
	var req shared.UpsertReportTemplateRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	template, err := app.upsertTemplate(r.Context(), templateID, req)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		shared.WriteError(w, status, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, template)
}

func (app application) deleteTemplate(w http.ResponseWriter, r *http.Request, templateID int64) {
	tag, err := app.db.Exec(r.Context(), `delete from app.report_templates where id = $1`, templateID)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		shared.WriteError(w, http.StatusNotFound, "template not found")
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

func (app application) runTemplateNow(w http.ResponseWriter, r *http.Request, templateID int64) {
	template, err := app.fetchTemplateByID(r.Context(), templateID)
	if err != nil {
		shared.WriteError(w, http.StatusNotFound, "template not found")
		return
	}

	runResp, savedReport, err := app.executeTemplate(r.Context(), template, "template-manual", false)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	app.markTemplateManualRun(r.Context(), templateID, runResp.Result.Count)

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"run":    runResp,
		"report": savedReport,
	})
}

func (app application) executeQueryText(ctx context.Context, queryText string) (shared.RunResponse, error) {
	payload := shared.QueryRequest{Text: queryText}
	rawBody, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, app.queryURL+"/api/v1/query/run", bytes.NewReader(rawBody))
	if err != nil {
		return shared.RunResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.client.Do(req)
	if err != nil {
		return shared.RunResponse{}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return shared.RunResponse{}, fmt.Errorf("%s", strings.TrimSpace(string(body)))
	}

	var runResp shared.RunResponse
	if err := json.Unmarshal(body, &runResp); err != nil {
		return shared.RunResponse{}, err
	}
	return runResp, nil
}

func (app application) saveRunSnapshot(ctx context.Context, name, queryText string, run shared.RunResponse, source string, templateID *int64) (shared.SavedReport, error) {
	report := shared.SavedReport{
		Name:       name,
		QueryText:  queryText,
		SQLText:    run.SQL,
		Intent:     run.Intent,
		Preview:    run.Preview,
		Result:     run.Result,
		Provider:   run.Provider,
		Source:     source,
		TemplateID: templateID,
	}

	var templateValue any
	if templateID != nil && *templateID > 0 {
		templateValue = *templateID
	}

	err := app.db.QueryRow(ctx, `
		insert into app.saved_reports (
			name,
			query_text,
			sql_text,
			intent,
			preview_json,
			result_json,
			provider,
			source,
			template_id
		)
		values ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7, $8, $9)
		returning id, created_at, updated_at
	`, report.Name, report.QueryText, report.SQLText, shared.MustJSON(report.Intent), shared.MustJSON(report.Preview), shared.MustJSON(report.Result), report.Provider, source, templateValue).
		Scan(&report.ID, &report.CreatedAt, &report.UpdatedAt)
	if err != nil {
		return shared.SavedReport{}, err
	}

	return report, nil
}

func (app application) fetchSavedReports(ctx context.Context) ([]shared.SavedReport, error) {
	rows, err := app.db.Query(ctx, `
		select
			sr.id,
			sr.name,
			sr.query_text,
			sr.sql_text,
			sr.intent,
			coalesce(sr.preview_json, '{}'::jsonb),
			coalesce(sr.result_json, '{"columns":[],"rows":[],"count":0}'::jsonb),
			coalesce(sr.provider, ''),
			coalesce(sr.source, 'manual'),
			coalesce(sr.template_id, 0),
			coalesce(rt.name, ''),
			sr.created_at,
			sr.updated_at
		from app.saved_reports sr
		left join app.report_templates rt on rt.id = sr.template_id
		order by sr.updated_at desc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	reports := make([]shared.SavedReport, 0)
	for rows.Next() {
		report, err := scanSavedReport(rows)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, rows.Err()
}

func (app application) fetchSavedReportByID(ctx context.Context, reportID int64) (shared.SavedReport, error) {
	row := app.db.QueryRow(ctx, `
		select
			sr.id,
			sr.name,
			sr.query_text,
			sr.sql_text,
			sr.intent,
			coalesce(sr.preview_json, '{}'::jsonb),
			coalesce(sr.result_json, '{"columns":[],"rows":[],"count":0}'::jsonb),
			coalesce(sr.provider, ''),
			coalesce(sr.source, 'manual'),
			coalesce(sr.template_id, 0),
			coalesce(rt.name, ''),
			sr.created_at,
			sr.updated_at
		from app.saved_reports sr
		left join app.report_templates rt on rt.id = sr.template_id
		where sr.id = $1
	`, reportID)
	return scanSavedReport(row)
}

func scanSavedReport(scanner interface {
	Scan(dest ...any) error
}) (shared.SavedReport, error) {
	var report shared.SavedReport
	var intentRaw []byte
	var previewRaw []byte
	var resultRaw []byte
	var templateID int64

	err := scanner.Scan(
		&report.ID,
		&report.Name,
		&report.QueryText,
		&report.SQLText,
		&intentRaw,
		&previewRaw,
		&resultRaw,
		&report.Provider,
		&report.Source,
		&templateID,
		&report.TemplateName,
		&report.CreatedAt,
		&report.UpdatedAt,
	)
	if err != nil {
		return shared.SavedReport{}, err
	}

	_ = json.Unmarshal(intentRaw, &report.Intent)
	_ = json.Unmarshal(previewRaw, &report.Preview)
	_ = json.Unmarshal(resultRaw, &report.Result)
	if templateID > 0 {
		report.TemplateID = &templateID
	}
	return report, nil
}

func (app application) upsertTemplate(ctx context.Context, templateID int64, req shared.UpsertReportTemplateRequest) (shared.ReportTemplate, error) {
	if strings.TrimSpace(req.Name) == "" {
		return shared.ReportTemplate{}, fmt.Errorf("template name is required")
	}
	if strings.TrimSpace(req.QueryText) == "" {
		return shared.ReportTemplate{}, fmt.Errorf("template query_text is required")
	}
	if err := validateTemplateSchedule(req.Schedule); err != nil {
		return shared.ReportTemplate{}, err
	}

	scheduleTimezone := coalesceString(req.Schedule.Timezone, app.location.String())
	var row interface {
		Scan(dest ...any) error
	}

	if templateID == 0 {
		row = app.db.QueryRow(ctx, `
			insert into app.report_templates (
				name,
				description,
				query_text,
				schedule_enabled,
				schedule_day_of_week,
				schedule_hour,
				schedule_minute,
				schedule_timezone
			)
			values ($1, $2, $3, $4, $5, $6, $7, $8)
			returning id, name, description, query_text, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
		`, strings.TrimSpace(req.Name), strings.TrimSpace(req.Description), strings.TrimSpace(req.QueryText), req.Schedule.Enabled, nullableInt(req.Schedule.Enabled, req.Schedule.DayOfWeek), nullableInt(req.Schedule.Enabled, req.Schedule.Hour), nullableInt(req.Schedule.Enabled, req.Schedule.Minute), scheduleTimezone)
	} else {
		row = app.db.QueryRow(ctx, `
			update app.report_templates
			set
				name = $2,
				description = $3,
				query_text = $4,
				schedule_enabled = $5,
				schedule_day_of_week = $6,
				schedule_hour = $7,
				schedule_minute = $8,
				schedule_timezone = $9,
				updated_at = now()
			where id = $1
			returning id, name, description, query_text, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
		`, templateID, strings.TrimSpace(req.Name), strings.TrimSpace(req.Description), strings.TrimSpace(req.QueryText), req.Schedule.Enabled, nullableInt(req.Schedule.Enabled, req.Schedule.DayOfWeek), nullableInt(req.Schedule.Enabled, req.Schedule.Hour), nullableInt(req.Schedule.Enabled, req.Schedule.Minute), scheduleTimezone)
	}

	record, err := scanTemplateRecord(row)
	if err != nil {
		if templateID > 0 {
			return shared.ReportTemplate{}, fmt.Errorf("template not found")
		}
		return shared.ReportTemplate{}, err
	}

	app.decorateTemplate(&record.ReportTemplate)
	return record.ReportTemplate, nil
}

func (app application) fetchTemplates(ctx context.Context) ([]shared.ReportTemplate, error) {
	rows, err := app.db.Query(ctx, `
		select id, name, description, query_text, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
		from app.report_templates
		order by updated_at desc, id desc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	templates := make([]shared.ReportTemplate, 0)
	for rows.Next() {
		record, err := scanTemplateRecord(rows)
		if err != nil {
			return nil, err
		}
		app.decorateTemplate(&record.ReportTemplate)
		templates = append(templates, record.ReportTemplate)
	}
	return templates, rows.Err()
}

func (app application) fetchTemplateByID(ctx context.Context, templateID int64) (templateRecord, error) {
	row := app.db.QueryRow(ctx, `
		select id, name, description, query_text, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
		from app.report_templates
		where id = $1
	`, templateID)
	record, err := scanTemplateRecord(row)
	if err != nil {
		return templateRecord{}, err
	}
	app.decorateTemplate(&record.ReportTemplate)
	return record, nil
}

func scanTemplateRecord(scanner interface {
	Scan(dest ...any) error
}) (templateRecord, error) {
	var record templateRecord
	var enabled bool
	var timezone string
	var day, hour, minute pgtype.Int4
	var lastRun pgtype.Timestamptz
	var lastScheduled pgtype.Timestamptz

	err := scanner.Scan(
		&record.ID,
		&record.Name,
		&record.Description,
		&record.QueryText,
		&enabled,
		&day,
		&hour,
		&minute,
		&timezone,
		&lastRun,
		&lastScheduled,
		&record.LastStatus,
		&record.LastErrorText,
		&record.LastResultCount,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return templateRecord{}, err
	}

	record.Schedule.Enabled = enabled
	record.Schedule.Timezone = timezone
	if day.Valid {
		record.Schedule.DayOfWeek = int(day.Int32)
	}
	if hour.Valid {
		record.Schedule.Hour = int(hour.Int32)
	}
	if minute.Valid {
		record.Schedule.Minute = int(minute.Int32)
	}
	if lastRun.Valid {
		value := lastRun.Time
		record.LastRunAt = &value
	}
	if lastScheduled.Valid {
		value := lastScheduled.Time
		record.lastScheduledFor = &value
	}
	return record, nil
}

func (app application) decorateTemplate(template *shared.ReportTemplate) {
	template.Schedule.Label = humanScheduleLabel(template.Schedule)
	if nextRun := nextRunForSchedule(template.Schedule, time.Now().In(app.location), app.location); !nextRun.IsZero() {
		template.Schedule.NextRun = nextRun.Format(time.RFC3339)
	}
}

func (app application) logExecution(ctx context.Context, reportID int64, status string, rowCount int, errorText string) {
	_, _ = app.db.Exec(ctx, `
		insert into app.report_runs (report_id, status, row_count, error_text)
		values ($1, $2, $3, $4)
	`, reportID, status, rowCount, nullableText(errorText))
}

func validateTemplateSchedule(schedule shared.ReportTemplateSchedule) error {
	if !schedule.Enabled {
		return nil
	}
	if schedule.DayOfWeek < 0 || schedule.DayOfWeek > 6 {
		return fmt.Errorf("schedule day_of_week must be between 0 and 6")
	}
	if schedule.Hour < 0 || schedule.Hour > 23 {
		return fmt.Errorf("schedule hour must be between 0 and 23")
	}
	if schedule.Minute < 0 || schedule.Minute > 59 {
		return fmt.Errorf("schedule minute must be between 0 and 59")
	}
	return nil
}

func nullableText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func nullableInt(enabled bool, value int) any {
	if !enabled {
		return nil
	}
	return value
}

func coalesceString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
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

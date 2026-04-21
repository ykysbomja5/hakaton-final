package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"drivee-self-service/internal/shared"
)

func (app application) startTemplateScheduler() {
	app.runDueTemplates(context.Background())

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		app.runDueTemplates(context.Background())
	}
}

func (app application) runDueTemplates(ctx context.Context) {
	templates, err := app.fetchTemplateRecords(ctx)
	if err != nil {
		log.Printf("template scheduler fetch failed: %v", err)
		return
	}

	now := time.Now().In(app.location)
	for _, template := range templates {
		slot := dueRunForSchedule(template.Schedule, now, app.location)
		if slot.IsZero() {
			continue
		}
		if now.Sub(slot) > 2*time.Minute {
			continue
		}
		if template.lastScheduledFor != nil && !template.lastScheduledFor.Before(slot) {
			continue
		}

		runResp, savedReport, err := app.executeTemplate(ctx, template, "scheduled", true)
		if err != nil {
			log.Printf("scheduled template %d failed: %v", template.ID, err)
			app.markTemplateRun(ctx, template.ID, slot, "failed", 0, err.Error(), false)
			continue
		}

		log.Printf("scheduled template %d saved report %d with %d rows", template.ID, savedReport.ID, runResp.Result.Count)
		app.markTemplateRun(ctx, template.ID, slot, "ok", runResp.Result.Count, "", true)
	}
}

func (app application) fetchTemplateRecords(ctx context.Context) ([]templateRecord, error) {
	rows, err := app.db.Query(ctx, `
		select id, name, description, query_text, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
		from app.report_templates
		where schedule_enabled = true
		order by id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]templateRecord, 0)
	for rows.Next() {
		record, err := scanTemplateRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (app application) executeTemplate(ctx context.Context, template templateRecord, source string, scheduled bool) (shared.RunResponse, shared.SavedReport, error) {
	runResp, err := app.executeQueryText(ctx, template.QueryText)
	if err != nil {
		return shared.RunResponse{}, shared.SavedReport{}, err
	}

	reportName := template.Name
	if scheduled {
		reportName = fmt.Sprintf("%s - %s", template.Name, time.Now().In(app.location).Format("02.01.2006 15:04"))
	}

	templateID := template.ID
	savedReport, err := app.saveRunSnapshot(ctx, reportName, template.QueryText, runResp, source, &templateID)
	if err != nil {
		return shared.RunResponse{}, shared.SavedReport{}, err
	}
	return runResp, savedReport, nil
}

func (app application) markTemplateRun(ctx context.Context, templateID int64, slot time.Time, status string, rowCount int, errorText string, updateLastRun bool) {
	if updateLastRun {
		_, _ = app.db.Exec(ctx, `
			update app.report_templates
			set
				last_run_at = now(),
				last_scheduled_for = $2,
				last_status = $3,
				last_error_text = $4,
				last_result_count = $5,
				updated_at = now()
			where id = $1
		`, templateID, slot, status, nullableText(errorText), rowCount)
		return
	}

	_, _ = app.db.Exec(ctx, `
		update app.report_templates
		set
			last_scheduled_for = $2,
			last_status = $3,
			last_error_text = $4,
			last_result_count = $5,
			updated_at = now()
		where id = $1
	`, templateID, slot, status, nullableText(errorText), rowCount)
}

func (app application) markTemplateManualRun(ctx context.Context, templateID int64, rowCount int) {
	_, _ = app.db.Exec(ctx, `
		update app.report_templates
		set
			last_run_at = now(),
			last_status = 'ok',
			last_error_text = null,
			last_result_count = $2,
			updated_at = now()
		where id = $1
	`, templateID, rowCount)
}

func humanScheduleLabel(schedule shared.ReportTemplateSchedule) string {
	if !schedule.Enabled {
		return "Без регулярного запуска"
	}
	dayNames := map[int]string{
		0: "Каждое воскресенье",
		1: "Каждый понедельник",
		2: "Каждый вторник",
		3: "Каждую среду",
		4: "Каждый четверг",
		5: "Каждую пятницу",
		6: "Каждую субботу",
	}
	return fmt.Sprintf("%s в %02d:%02d", dayNames[schedule.DayOfWeek], schedule.Hour, schedule.Minute)
}

func dueRunForSchedule(schedule shared.ReportTemplateSchedule, now time.Time, fallback *time.Location) time.Time {
	if !schedule.Enabled {
		return time.Time{}
	}
	location := resolveScheduleLocation(schedule.Timezone, fallback)
	localNow := now.In(location)

	weekdayDistance := int(localNow.Weekday()) - schedule.DayOfWeek
	slotDate := localNow.AddDate(0, 0, -weekdayDistance)
	slot := time.Date(slotDate.Year(), slotDate.Month(), slotDate.Day(), schedule.Hour, schedule.Minute, 0, 0, location)
	if slot.After(localNow) {
		slot = slot.AddDate(0, 0, -7)
	}
	return slot
}

func nextRunForSchedule(schedule shared.ReportTemplateSchedule, now time.Time, fallback *time.Location) time.Time {
	if !schedule.Enabled {
		return time.Time{}
	}
	location := resolveScheduleLocation(schedule.Timezone, fallback)
	localNow := now.In(location)
	slot := dueRunForSchedule(schedule, localNow, location)
	if slot.IsZero() {
		return time.Time{}
	}
	if !slot.After(localNow) {
		slot = slot.AddDate(0, 0, 7)
	}
	return slot
}

func resolveScheduleLocation(timezone string, fallback *time.Location) *time.Location {
	if timezone != "" {
		if location, err := time.LoadLocation(timezone); err == nil {
			return location
		}
	}
	if fallback != nil {
		return fallback
	}
	return time.Local
}

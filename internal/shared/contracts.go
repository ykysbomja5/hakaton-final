package shared

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type QueryRequest struct {
	Text string `json:"text"`
}

type Filter struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type TimeRange struct {
	Label string `json:"label"`
	From  string `json:"from"`
	To    string `json:"to"`
	Grain string `json:"grain"`
}

type Intent struct {
	Metric        string    `json:"metric"`
	GroupBy       string    `json:"group_by"`
	Filters       []Filter  `json:"filters,omitempty"`
	Period        TimeRange `json:"period"`
	Sort          string    `json:"sort,omitempty"`
	Limit         int       `json:"limit,omitempty"`
	Clarification string    `json:"clarification,omitempty"`
	Assumptions   []string  `json:"assumptions,omitempty"`
	Confidence    float64   `json:"confidence"`
}

type MetricDefinition struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Format      string `json:"format"`
}

type DimensionDefinition struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Column      string   `json:"column"`
	Description string   `json:"description"`
	Values      []string `json:"values,omitempty"`
}

type BusinessTerm struct {
	Term        string `json:"term"`
	Kind        string `json:"kind"`
	Canonical   string `json:"canonical"`
	Description string `json:"description"`
}

type SemanticLayer struct {
	Metrics         []MetricDefinition    `json:"metrics"`
	Dimensions      []DimensionDefinition `json:"dimensions"`
	Terms           []BusinessTerm        `json:"terms"`
	SampleQuestions []string              `json:"sample_questions"`
}

type IntentRequest struct {
	Text          string        `json:"text"`
	SemanticLayer SemanticLayer `json:"semantic_layer"`
}

type IntentResponse struct {
	Intent   Intent `json:"intent"`
	Provider string `json:"provider"`
}

type QueryPreview struct {
	Summary         string   `json:"summary"`
	MetricLabel     string   `json:"metric_label"`
	GroupByLabel    string   `json:"group_by_label,omitempty"`
	AppliedFilters  []string `json:"applied_filters,omitempty"`
	Assumptions     []string `json:"assumptions,omitempty"`
	Clarification   string   `json:"clarification,omitempty"`
	ConfidenceLabel string   `json:"confidence_label"`
}

type ParseResponse struct {
	Intent        Intent        `json:"intent"`
	Preview       QueryPreview  `json:"preview"`
	SemanticLayer SemanticLayer `json:"semantic_layer"`
	SQL           string        `json:"sql,omitempty"`
	Provider      string        `json:"provider,omitempty"`
}

type QueryResult struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
	Count   int        `json:"count"`
}

type ChartSpec struct {
	Type string `json:"type"`
	XKey string `json:"x_key"`
	YKey string `json:"y_key"`
}

type RunResponse struct {
	Intent        Intent        `json:"intent"`
	Preview       QueryPreview  `json:"preview"`
	SQL           string        `json:"sql"`
	Result        QueryResult   `json:"result"`
	Chart         ChartSpec     `json:"chart"`
	SemanticLayer SemanticLayer `json:"semantic_layer"`
	Provider      string        `json:"provider,omitempty"`
}

type SaveReportRequest struct {
	Name       string       `json:"name"`
	QueryText  string       `json:"query_text"`
	SQLText    string       `json:"sql_text"`
	Intent     Intent       `json:"intent"`
	Preview    QueryPreview `json:"preview"`
	Result     QueryResult  `json:"result"`
	Provider   string       `json:"provider,omitempty"`
	Source     string       `json:"source,omitempty"`
	TemplateID *int64       `json:"template_id,omitempty"`
}

type SavedReport struct {
	ID           int64        `json:"id"`
	Name         string       `json:"name"`
	QueryText    string       `json:"query_text"`
	SQLText      string       `json:"sql_text"`
	Intent       Intent       `json:"intent"`
	Preview      QueryPreview `json:"preview"`
	Result       QueryResult  `json:"result"`
	Provider     string       `json:"provider,omitempty"`
	Source       string       `json:"source,omitempty"`
	TemplateID   *int64       `json:"template_id,omitempty"`
	TemplateName string       `json:"template_name,omitempty"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

type ReportTemplateSchedule struct {
	Enabled   bool   `json:"enabled"`
	DayOfWeek int    `json:"day_of_week,omitempty"`
	Hour      int    `json:"hour,omitempty"`
	Minute    int    `json:"minute,omitempty"`
	Timezone  string `json:"timezone,omitempty"`
	Label     string `json:"label,omitempty"`
	NextRun   string `json:"next_run,omitempty"`
}

type ReportTemplate struct {
	ID              int64                  `json:"id"`
	Name            string                 `json:"name"`
	Description     string                 `json:"description"`
	QueryText       string                 `json:"query_text"`
	Schedule        ReportTemplateSchedule `json:"schedule"`
	LastRunAt       *time.Time             `json:"last_run_at,omitempty"`
	LastStatus      string                 `json:"last_status,omitempty"`
	LastErrorText   string                 `json:"last_error_text,omitempty"`
	LastResultCount int                    `json:"last_result_count,omitempty"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

type UpsertReportTemplateRequest struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	QueryText   string                 `json:"query_text"`
	Schedule    ReportTemplateSchedule `json:"schedule"`
}

type ExportReportRequest struct {
	Name      string      `json:"name"`
	QueryText string      `json:"query_text"`
	Run       RunResponse `json:"run"`
}

func NormalizeText(value string) string {
	replacer := strings.NewReplacer(",", " ", ".", " ", "?", " ", "!", " ", "ё", "е", "  ", " ")
	return strings.TrimSpace(strings.ToLower(replacer.Replace(value)))
}

func ConfidenceLabel(score float64) string {
	switch {
	case score >= 0.8:
		return "Высокая"
	case score >= 0.6:
		return "Средняя"
	default:
		return "Низкая"
	}
}

func (intent Intent) MetricLabel(layer SemanticLayer) string {
	for _, metric := range layer.Metrics {
		if metric.ID == intent.Metric {
			return metric.Title
		}
	}
	return intent.Metric
}

func (intent Intent) GroupByLabel(layer SemanticLayer) string {
	if intent.GroupBy == "" {
		return ""
	}
	for _, dimension := range layer.Dimensions {
		if dimension.ID == intent.GroupBy {
			return dimension.Title
		}
	}
	return intent.GroupBy
}

func BuildPreview(intent Intent, layer SemanticLayer) QueryPreview {
	filters := make([]string, 0, len(intent.Filters))
	for _, filter := range intent.Filters {
		filters = append(filters, fmt.Sprintf("%s %s %s", filter.Field, filter.Operator, filter.Value))
	}

	summary := fmt.Sprintf("%s за %s", intent.MetricLabel(layer), intent.Period.Label)
	if label := intent.GroupByLabel(layer); label != "" {
		summary += fmt.Sprintf(", сгруппировано по %s", strings.ToLower(label))
	}
	if len(filters) > 0 {
		summary += ". Фильтры: " + strings.Join(filters, ", ")
	}

	return QueryPreview{
		Summary:         summary,
		MetricLabel:     intent.MetricLabel(layer),
		GroupByLabel:    intent.GroupByLabel(layer),
		AppliedFilters:  filters,
		Assumptions:     intent.Assumptions,
		Clarification:   intent.Clarification,
		ConfidenceLabel: ConfidenceLabel(intent.Confidence),
	}
}

func DefaultSemanticLayer() SemanticLayer {
	return SemanticLayer{
		Metrics: []MetricDefinition{
			{ID: "completed_rides", Title: "Завершенные поездки", Description: "Количество завершенных поездок", Format: "integer"},
			{ID: "total_rides", Title: "Все поездки", Description: "Сумма завершенных и отмененных поездок", Format: "integer"},
			{ID: "cancellations", Title: "Отмены", Description: "Количество отмененных поездок", Format: "integer"},
			{ID: "revenue", Title: "Выручка", Description: "Суммарная выручка в рублях", Format: "currency"},
			{ID: "avg_fare", Title: "Средний чек", Description: "Средний чек завершенной поездки", Format: "currency"},
			{ID: "active_drivers", Title: "Активные водители", Description: "Количество активных водителей", Format: "integer"},
		},
		Dimensions: []DimensionDefinition{
			{ID: "day", Title: "День", Column: "stat_date", Description: "Дневная гранулярность"},
			{ID: "week", Title: "Неделя", Column: "stat_date", Description: "Недельная гранулярность"},
			{ID: "month", Title: "Месяц", Column: "stat_date", Description: "Месячная гранулярность"},
			{ID: "city", Title: "Город", Column: "city", Description: "Город поездки", Values: []string{"Москва", "Санкт-Петербург", "Казань", "Екатеринбург", "Новосибирск"}},
			{ID: "service_class", Title: "Тариф", Column: "service_class", Description: "Класс поездки", Values: []string{"Эконом", "Комфорт", "Бизнес"}},
			{ID: "source_channel", Title: "Канал", Column: "source_channel", Description: "Канал заказа", Values: []string{"Приложение", "Сайт", "Партнеры"}},
			{ID: "driver_segment", Title: "Сегмент водителя", Column: "driver_segment", Description: "Сегмент активности водителя", Values: []string{"Новые", "Стабильные", "Премиум"}},
		},
		Terms: []BusinessTerm{
			{Term: "поездки", Kind: "metric", Canonical: "completed_rides", Description: "Завершенные поездки"},
			{Term: "заказы", Kind: "metric", Canonical: "total_rides", Description: "Все поездки"},
			{Term: "отмены", Kind: "metric", Canonical: "cancellations", Description: "Количество отмен"},
			{Term: "выручка", Kind: "metric", Canonical: "revenue", Description: "Суммарная выручка"},
			{Term: "средний чек", Kind: "metric", Canonical: "avg_fare", Description: "Средняя стоимость поездки"},
			{Term: "водители", Kind: "metric", Canonical: "active_drivers", Description: "Активные водители"},
			{Term: "по городам", Kind: "dimension", Canonical: "city", Description: "Группировка по городам"},
			{Term: "по тарифам", Kind: "dimension", Canonical: "service_class", Description: "Группировка по тарифам"},
			{Term: "по каналам", Kind: "dimension", Canonical: "source_channel", Description: "Группировка по каналам"},
			{Term: "по дням", Kind: "dimension", Canonical: "day", Description: "Группировка по дням"},
			{Term: "по неделям", Kind: "dimension", Canonical: "week", Description: "Группировка по неделям"},
			{Term: "по месяцам", Kind: "dimension", Canonical: "month", Description: "Группировка по месяцам"},
			{Term: "москва", Kind: "filter", Canonical: "Москва", Description: "Фильтр по городу"},
			{Term: "питер", Kind: "filter", Canonical: "Санкт-Петербург", Description: "Фильтр по городу"},
			{Term: "казань", Kind: "filter", Canonical: "Казань", Description: "Фильтр по городу"},
			{Term: "эконом", Kind: "filter", Canonical: "Эконом", Description: "Фильтр по тарифу"},
			{Term: "комфорт", Kind: "filter", Canonical: "Комфорт", Description: "Фильтр по тарифу"},
			{Term: "бизнес", Kind: "filter", Canonical: "Бизнес", Description: "Фильтр по тарифу"},
			{Term: "приложение", Kind: "filter", Canonical: "Приложение", Description: "Фильтр по каналу"},
			{Term: "сайт", Kind: "filter", Canonical: "Сайт", Description: "Фильтр по каналу"},
			{Term: "партнеры", Kind: "filter", Canonical: "Партнеры", Description: "Фильтр по каналу"},
		},
		SampleQuestions: []string{
			"Покажи выручку по городам за последние 30 дней",
			"Сколько было отмен по тарифам на прошлой неделе",
			"Какие каналы дают больше всего поездок в Москве за месяц",
			"Покажи средний чек по дням за последние 14 дней",
		},
	}
}

func MustJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

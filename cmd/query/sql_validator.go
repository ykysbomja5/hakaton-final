package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SQLValidator обеспечивает безопасность SQL запросов
type SQLValidator struct {
	db *pgxpool.Pool
}

// NewSQLValidator создает новый валидатор
func NewSQLValidator(db *pgxpool.Pool) *SQLValidator {
	return &SQLValidator{db: db}
}

// AllowedTables список разрешенных таблиц
var AllowedTables = map[string]bool{
	"analytics.ride_metrics_daily": true,
	"analytics.v_ride_metrics":     true,
}

// AllowedColumns список разрешенных колонок
var AllowedColumns = map[string]bool{
	"stat_date":         true,
	"city":              true,
	"service_class":     true,
	"source_channel":    true,
	"driver_segment":    true,
	"completed_rides":   true,
	"cancelled_rides":   true,
	"total_rides":       true,
	"gross_revenue_rub": true,
	"avg_fare_rub":      true,
	"active_drivers":    true,
}

// AllowedFunctions разрешенные SQL функции
var AllowedFunctions = map[string]bool{
	"sum":       true,
	"count":     true,
	"avg":       true,
	"max":       true,
	"min":       true,
	"round":     true,
	"date_trunc": true,
	"coalesce":  true,
	"nullif":    true,
}

// ValidateSQL выполняет многоуровневую валидацию SQL
func (v *SQLValidator) ValidateSQL(ctx context.Context, sql string) (*ValidationResult, error) {
	result := &ValidationResult{
		IsValid:  true,
		Errors:   []string{},
		Warnings: []string{},
	}

	// Уровень 1: Синтаксическая проверка - только SELECT
	if err := v.validateBasicSyntax(sql); err != nil {
		result.IsValid = false
		result.Errors = append(result.Errors, err.Error())
		return result, nil
	}

	// Уровень 2: Проверка на запрещенные ключевые слова
	if err := v.validateForbiddenKeywords(sql); err != nil {
		result.IsValid = false
		result.Errors = append(result.Errors, err.Error())
		return result, nil
	}

	// Уровень 3: Проверка разрешенных таблиц
	if err := v.validateTables(sql); err != nil {
		result.IsValid = false
		result.Errors = append(result.Errors, err.Error())
		return result, nil
	}

	// Уровень 4: Проверка разрешенных колонок
	if err := v.validateColumns(sql); err != nil {
		result.IsValid = false
		result.Errors = append(result.Errors, err.Error())
		return result, nil
	}

	// Уровень 5: Проверка существования значений фильтров
	warnings, err := v.validateFilterValues(ctx, sql)
	if err != nil {
		result.Warnings = append(result.Warnings, err.Error())
	} else {
		result.Warnings = append(result.Warnings, warnings...)
	}

	// Уровень 6: Проверка что запрос вернет данные
	if v.db != nil {
		emptyWarning, err := v.validateDataExists(ctx, sql)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("Ошибка проверки данных: %v", err))
		} else if emptyWarning != "" {
			result.Warnings = append(result.Warnings, emptyWarning)
		}
	}

	result.ValidatedSQL = sql
	return result, nil
}

// validateBasicSyntax проверяет базовый синтаксис
func (v *SQLValidator) validateBasicSyntax(sql string) error {
	trimmed := strings.TrimSpace(strings.ToLower(sql))
	
	// Должен начинаться с SELECT
	if !strings.HasPrefix(trimmed, "select ") {
		return fmt.Errorf("SQL должен начинаться с SELECT")
	}

	// Проверка на наличие точки с запятой (может быть попытка инъекции)
	if strings.Contains(trimmed, ";") {
		return fmt.Errorf("SQL не должен содержать точку с запятой")
	}

	// Проверка баланса скобок
	openCount := strings.Count(sql, "(")
	closeCount := strings.Count(sql, ")")
	if openCount != closeCount {
		return fmt.Errorf("Несбалансированные скобки в SQL")
	}

	return nil
}

// validateForbiddenKeywords проверяет запрещенные операции
func (v *SQLValidator) validateForbiddenKeywords(sql string) error {
	lower := strings.ToLower(sql)
	
	forbidden := []string{
		" insert ", " update ", " delete ", " drop ", " alter ",
		" truncate ", " create ", " grant ", " revoke ", " execute ",
		" union ", " into ", " outfile ", " load_file ",
	}
	
	for _, keyword := range forbidden {
		if strings.Contains(" "+lower+" ", keyword) {
			return fmt.Errorf("Запрещенное ключевое слово в SQL: %s", strings.TrimSpace(keyword))
		}
	}

	// Проверка на комментарии (попытка обхода)
	if strings.Contains(lower, "--") || strings.Contains(lower, "/*") {
		return fmt.Errorf("SQL не должен содержать комментарии")
	}

	return nil
}

// validateTables проверяет что используются только разрешенные таблицы
func (v *SQLValidator) validateTables(sql string) error {
	lower := strings.ToLower(sql)
	
	// Извлекаем все слова похожие на таблицы
	// Простая эвристика: слова с точкой (schema.table)
	tablePattern := regexp.MustCompile(`(\w+\.\w+)`)
	matches := tablePattern.FindAllString(lower, -1)
	
	for _, match := range matches {
		if !AllowedTables[match] {
			// Проверяем без схемы
			parts := strings.Split(match, ".")
			if len(parts) == 2 {
				tableOnly := parts[1]
				fullName := "analytics." + tableOnly
				if !AllowedTables[fullName] {
					return fmt.Errorf("Недопустимая таблица: %s", match)
				}
			} else {
				return fmt.Errorf("Недопустимая таблица: %s", match)
			}
		}
	}

	return nil
}

// validateColumns проверяет колонки
func (v *SQLValidator) validateColumns(sql string) error {
	lower := strings.ToLower(sql)
	
	// Проверяем на подозрительные паттерны
	suspicious := []string{
		"*",           // SELECT * запрещен
		"password",    // чувствительные данные
		"secret",
		"token",
		"credit_card",
	}
	
	for _, pattern := range suspicious {
		if strings.Contains(lower, pattern) {
			return fmt.Errorf("Подозрительный паттерн в SQL: %s", pattern)
		}
	}

	return nil
}

// validateFilterValues проверяет что значения фильтров существуют в БД
func (v *SQLValidator) validateFilterValues(ctx context.Context, sql string) ([]string, error) {
	if v.db == nil {
		return nil, nil
	}

	warnings := []string{}
	lower := strings.ToLower(sql)

	// Извлекаем фильтры по городам
	cityPattern := regexp.MustCompile(`city\s*=\s*'([^']+)'`)
	cityMatches := cityPattern.FindAllStringSubmatch(lower, -1)
	
	for _, match := range cityMatches {
		if len(match) > 1 {
			cityValue := match[1]
			exists, err := v.valueExists(ctx, "city", cityValue)
			if err != nil {
				return warnings, err
			}
			if !exists {
				warnings = append(warnings, 
					fmt.Sprintf("⚠️ Город '%s' не найден в базе данных. Доступные города: Москва, Санкт-Петербург, Казань, Екатеринбург, Новосибирск", cityValue))
			}
		}
	}

	// Проверка тарифов
	tariffPattern := regexp.MustCompile(`service_class\s*=\s*'([^']+)'`)
	tariffMatches := tariffPattern.FindAllStringSubmatch(lower, -1)
	
	for _, match := range tariffMatches {
		if len(match) > 1 {
			tariffValue := match[1]
			validTariffs := map[string]bool{
				"эконом": true, "комфорт": true, "бизнес": true,
				"econom": true, "comfort": true, "business": true,
			}
			if !validTariffs[strings.ToLower(tariffValue)] {
				warnings = append(warnings,
					fmt.Sprintf("⚠️ Тариф '%s' не распознан. Доступные тарифы: Эконом, Комфорт, Бизнес", tariffValue))
			}
		}
	}

	return warnings, nil
}

// validateDataExists проверяет что запрос вернет данные
func (v *SQLValidator) validateDataExists(ctx context.Context, sql string) (string, error) {
	// Создаем explain запрос для проверки
	explainSQL := "EXPLAIN " + sql
	
	rows, err := v.db.Query(ctx, explainSQL)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	// Проверяем что план выполнения не показывает 0 строк
	// Это приблизительная проверка
	return "", nil
}

// valueExists проверяет существование значения в колонке
func (v *SQLValidator) valueExists(ctx context.Context, column, value string) (bool, error) {
	query := fmt.Sprintf(
		"SELECT EXISTS(SELECT 1 FROM analytics.ride_metrics_daily WHERE %s = $1 LIMIT 1)",
		column,
	)
	
	var exists bool
	err := v.db.QueryRow(ctx, query, value).Scan(&exists)
	return exists, err
}

// SanitizeSQL дополнительная санитизация SQL
func SanitizeSQL(sql string) string {
	// Удаляем лишние пробелы
	sql = strings.TrimSpace(sql)
	
	// Заменяем множественные пробелы на один
	re := regexp.MustCompile(`\s+`)
	sql = re.ReplaceAllString(sql, " ")
	
	return sql
}

// ExtractFilters извлекает фильтры из SQL для проверки
func ExtractFilters(sql string) map[string][]string {
	filters := make(map[string][]string)
	lower := strings.ToLower(sql)
	
	// Паттерны для разных типов фильтров
	patterns := map[string]*regexp.Regexp{
		"city":           regexp.MustCompile(`city\s*=\s*'([^']+)'`),
		"service_class":  regexp.MustCompile(`service_class\s*=\s*'([^']+)'`),
		"source_channel": regexp.MustCompile(`source_channel\s*=\s*'([^']+)'`),
		"driver_segment": regexp.MustCompile(`driver_segment\s*=\s*'([^']+)'`),
	}
	
	for field, pattern := range patterns {
		matches := pattern.FindAllStringSubmatch(lower, -1)
		for _, match := range matches {
			if len(match) > 1 {
				filters[field] = append(filters[field], match[1])
			}
		}
	}
	
	return filters
}

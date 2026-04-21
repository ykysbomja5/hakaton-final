package shared

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type SQLGenerationRequest struct {
	Text          string        `json:"text"`
	SemanticLayer SemanticLayer `json:"semantic_layer"`
}

type SQLGenerationResponse struct {
	SQL      string `json:"sql"`
	Intent   Intent `json:"intent"`
	Provider string `json:"provider"`
}

func ValidateGeneratedSQL(sqlText string) error {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return fmt.Errorf("empty sql received from llm")
	}

	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "select ") {
		return fmt.Errorf("only plain select queries are allowed")
	}
	if strings.ContainsAny(trimmed, ";") {
		return fmt.Errorf("multiple SQL statements are not allowed")
	}
	for _, marker := range []string{"--", "/*", "*/"} {
		if strings.Contains(trimmed, marker) {
			return fmt.Errorf("sql comments are not allowed")
		}
	}
	for _, forbidden := range []string{
		" insert ", " update ", " delete ", " drop ", " alter ", " truncate ",
		" create ", " grant ", " revoke ", " call ", " execute ", " do ",
		" copy ", " refresh ", " merge ", " vacuum ", " analyze ",
	} {
		if strings.Contains(" "+lower+" ", forbidden) {
			return fmt.Errorf("query did not pass security validation")
		}
	}
	for _, forbiddenRef := range []string{
		" information_schema.",
		" pg_catalog.",
		" pg_toast.",
		" pg_temp.",
		" app.",
		" public.",
	} {
		if strings.Contains(lower, forbiddenRef) {
			return fmt.Errorf("query references a forbidden schema")
		}
	}
	if !strings.Contains(lower, " from analytics.v_ride_metrics") {
		return fmt.Errorf("query must read only from analytics.v_ride_metrics")
	}

	limitRe := regexp.MustCompile(`(?i)\blimit\s+(\d+)\b`)
	if matches := limitRe.FindStringSubmatch(trimmed); len(matches) == 2 {
		limit, err := strconv.Atoi(matches[1])
		if err != nil || limit <= 0 || limit > 100 {
			return fmt.Errorf("limit must be between 1 and 100")
		}
	}

	return nil
}

func WrapQueryForExecution(sqlText string) string {
	return "select * from (" + sqlText + ") as llm_query limit 100"
}

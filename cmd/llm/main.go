package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"drivee-self-service/internal/shared"
)

func gigachatHTTPClient() *http.Client {
	if strings.ToLower(getenv("GIGACHAT_INSECURE_SKIP_VERIFY", "false")) == "true" {
		return &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
	}
	return http.DefaultClient
}

func main() {
	port := getenv("PORT", "8082")
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/intent", handleIntent)
	mux.HandleFunc("/v1/sql", handleSQLGeneration)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "llm"})
	})

	log.Printf("llm listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func handleIntent(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req shared.IntentRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if strings.TrimSpace(req.Text) == "" {
		shared.WriteError(w, http.StatusBadRequest, "text is required")
		return
	}
	if len(req.SemanticLayer.Metrics) == 0 {
		req.SemanticLayer = shared.DefaultSemanticLayer()
	}

	intent, provider, err := inferIntent(r.Context(), req.Text, req.SemanticLayer)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, shared.IntentResponse{
		Intent:   intent,
		Provider: provider,
	})
}

func inferIntent(ctx context.Context, text string, layer shared.SemanticLayer) (shared.Intent, string, error) {
	provider := strings.ToLower(getenv("LLM_PROVIDER", "gigachat"))
	fallback := strings.ToLower(getenv("LLM_FALLBACK", "rule-based"))

	switch provider {
	case "gigachat":
		if intent, err := callGigaChat(ctx, text, layer); err == nil && intent.Metric != "" {
			intent = normalizeExternalIntent(text, intent, layer)
			return intent, "gigachat", nil
		}
	case "yandexgpt", "alice":
		if intent, err := callYandexGPT(ctx, text, layer); err == nil && intent.Metric != "" {
			intent = normalizeExternalIntent(text, intent, layer)
			return intent, "yandexgpt", nil
		}
	}

	intent := inferRuleBasedIntent(text, layer)
	if intent.Metric == "" && fallback == "none" {
		return shared.Intent{}, "", fmt.Errorf("external llm did not return a valid intent")
	}
	return intent, "rule-based", nil
}

func normalizeExternalIntent(text string, external shared.Intent, layer shared.SemanticLayer) shared.Intent {
	reference := inferRuleBasedIntent(text, layer)

	if !isKnownMetric(external.Metric) {
		external.Metric = reference.Metric
	}
	if external.GroupBy != "" && !isKnownGroupBy(external.GroupBy) {
		external.GroupBy = reference.GroupBy
	}
	if external.GroupBy == "" {
		external.GroupBy = reference.GroupBy
	}
	if !isISODate(external.Period.From) || !isISODate(external.Period.To) {
		external.Period = reference.Period
	} else {
		if strings.TrimSpace(external.Period.Label) == "" {
			external.Period.Label = reference.Period.Label
		}
		if strings.TrimSpace(external.Period.Grain) == "" {
			external.Period.Grain = reference.Period.Grain
		}
	}
	if len(external.Filters) == 0 && len(reference.Filters) > 0 {
		external.Filters = reference.Filters
	}
	if strings.TrimSpace(external.Sort) == "" {
		external.Sort = reference.Sort
	}
	if external.Limit <= 0 || external.Limit > 100 {
		external.Limit = reference.Limit
	}
	if external.Confidence <= 0 {
		external.Confidence = reference.Confidence
	}
	if len(external.Assumptions) == 0 && len(reference.Assumptions) > 0 {
		external.Assumptions = reference.Assumptions
	}
	return external
}

func isKnownMetric(metric string) bool {
	switch metric {
	case "completed_rides", "total_rides", "cancellations", "revenue", "avg_fare", "active_drivers":
		return true
	default:
		return false
	}
}

func isKnownGroupBy(groupBy string) bool {
	switch groupBy {
	case "day", "week", "month", "city", "service_class", "source_channel", "driver_segment", "":
		return true
	default:
		return false
	}
}

func isISODate(value string) bool {
	if len(value) != len("2006-01-02") {
		return false
	}
	_, err := time.Parse("2006-01-02", value)
	return err == nil
}

func inferRuleBasedIntent(text string, layer shared.SemanticLayer) shared.Intent {
	now := time.Now()
	normalized := shared.NormalizeText(text)
	intent := shared.Intent{
		Filters:     []shared.Filter{},
		Period:      defaultPeriod(now),
		Sort:        "desc",
		Limit:       12,
		Assumptions: []string{},
		Confidence:  0.35,
	}

	if metric, delta := detectMetric(normalized); metric != "" {
		intent.Metric = metric
		intent.Confidence += delta
	}
	if groupBy, delta := detectGroupBy(normalized); groupBy != "" {
		intent.GroupBy = groupBy
		intent.Confidence += delta
	}
	if period, explicit := detectPeriod(normalized, now); explicit {
		intent.Period = period
		intent.Confidence += 0.2
	} else {
		intent.Assumptions = append(intent.Assumptions, "Период не был указан явно, использованы последние 30 дней.")
		intent.Confidence -= 0.05
	}
	if filters := detectFilters(normalized, layer); len(filters) > 0 {
		intent.Filters = filters
		intent.Confidence += math.Min(0.18, float64(len(filters))*0.06)
	}
	if limit, ok := detectLimit(normalized); ok {
		intent.Limit = limit
		intent.Confidence += 0.04
	}
	if strings.Contains(normalized, "сравн") {
		intent.Assumptions = append(intent.Assumptions, "Сравнение с предыдущим периодом отмечено как следующая итерация MVP.")
		intent.Confidence -= 0.08
	}

	intent.Confidence = math.Max(0.1, math.Min(0.96, intent.Confidence))
	if intent.Metric == "" {
		intent.Clarification = "Уточните метрику: поездки, выручка, отмены, средний чек или активные водители."
		intent.Confidence = 0.28
	}
	return intent
}

func detectMetric(text string) (string, float64) {
	lookup := []struct {
		ID       string
		Words    []string
		Strength float64
	}{
		{ID: "revenue", Words: []string{"выручк", "оборот", "доход"}, Strength: 0.3},
		{ID: "cancellations", Words: []string{"отмен", "cancel"}, Strength: 0.28},
		{ID: "avg_fare", Words: []string{"средний чек", "средн чек", "средняя стоимость", "средний тариф"}, Strength: 0.28},
		{ID: "active_drivers", Words: []string{"активные водители", "водител", "актив вод"}, Strength: 0.22},
		{ID: "total_rides", Words: []string{"все поездки", "все заказы", "количество заказов"}, Strength: 0.26},
		{ID: "completed_rides", Words: []string{"поездк", "заверш", "выполнен"}, Strength: 0.24},
	}
	for _, item := range lookup {
		for _, word := range item.Words {
			if strings.Contains(text, word) {
				return item.ID, item.Strength
			}
		}
	}
	return "", 0
}

func detectGroupBy(text string) (string, float64) {
	lookup := []struct {
		ID    string
		Words []string
	}{
		{ID: "city", Words: []string{"по город", "по регионам", "по городам"}},
		{ID: "service_class", Words: []string{"по тариф", "по класс", "по сервисам"}},
		{ID: "source_channel", Words: []string{"по канал", "по источникам", "по источнику"}},
		{ID: "driver_segment", Words: []string{"по сегмент", "по водителям"}},
		{ID: "day", Words: []string{"по дням", "ежедневно", "по датам"}},
		{ID: "week", Words: []string{"по неделям", "еженедельно"}},
		{ID: "month", Words: []string{"по месяцам", "ежемесячно"}},
	}
	for _, item := range lookup {
		for _, word := range item.Words {
			if strings.Contains(text, word) {
				return item.ID, 0.14
			}
		}
	}
	return "", 0
}

func detectFilters(text string, layer shared.SemanticLayer) []shared.Filter {
	filters := make([]shared.Filter, 0)
	added := map[string]bool{}

	for _, dimension := range layer.Dimensions {
		switch dimension.ID {
		case "city", "service_class", "source_channel", "driver_segment":
			for _, value := range dimension.Values {
				lowered := shared.NormalizeText(value)
				if strings.Contains(text, lowered) || (value == "Санкт-Петербург" && strings.Contains(text, "питер")) {
					key := dimension.ID + ":" + value
					if !added[key] {
						filters = append(filters, shared.Filter{Field: dimension.ID, Operator: "=", Value: value})
						added[key] = true
					}
				}
			}
		}
	}
	return filters
}

func detectPeriod(text string, now time.Time) (shared.TimeRange, bool) {
	if strings.Contains(text, "сегодня") {
		return shared.TimeRange{Label: "сегодня", From: now.Format("2006-01-02"), To: now.Format("2006-01-02"), Grain: "day"}, true
	}
	if strings.Contains(text, "вчера") {
		day := now.AddDate(0, 0, -1)
		return shared.TimeRange{Label: "вчера", From: day.Format("2006-01-02"), To: day.Format("2006-01-02"), Grain: "day"}, true
	}

	lastDaysRe := regexp.MustCompile(`последн(?:ие|их)?\s+(\d+)\s+д`)
	if matches := lastDaysRe.FindStringSubmatch(text); len(matches) == 2 {
		days, _ := strconv.Atoi(matches[1])
		from := now.AddDate(0, 0, -(days - 1))
		return shared.TimeRange{
			Label: fmt.Sprintf("последние %d дней", days),
			From:  from.Format("2006-01-02"),
			To:    now.Format("2006-01-02"),
			Grain: "day",
		}, true
	}

	if strings.Contains(text, "прошлую неделю") || strings.Contains(text, "прошлая неделя") {
		startOfWeek := startOfWeek(now.AddDate(0, 0, -7))
		endOfWeek := startOfWeek.AddDate(0, 0, 6)
		return shared.TimeRange{Label: "прошлая неделя", From: startOfWeek.Format("2006-01-02"), To: endOfWeek.Format("2006-01-02"), Grain: "day"}, true
	}

	if strings.Contains(text, "прошлый месяц") {
		start := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, now.Location())
		end := start.AddDate(0, 1, -1)
		return shared.TimeRange{Label: "прошлый месяц", From: start.Format("2006-01-02"), To: end.Format("2006-01-02"), Grain: "day"}, true
	}

	if strings.Contains(text, "за месяц") {
		from := now.AddDate(0, -1, 1)
		return shared.TimeRange{Label: "последний месяц", From: from.Format("2006-01-02"), To: now.Format("2006-01-02"), Grain: "day"}, true
	}
	return shared.TimeRange{}, false
}

func detectLimit(text string) (int, bool) {
	re := regexp.MustCompile(`топ\s+(\d+)`)
	matches := re.FindStringSubmatch(text)
	if len(matches) != 2 {
		return 0, false
	}
	value, err := strconv.Atoi(matches[1])
	if err != nil || value <= 0 {
		return 0, false
	}
	return value, true
}

func defaultPeriod(now time.Time) shared.TimeRange {
	from := now.AddDate(0, 0, -29)
	return shared.TimeRange{
		Label: "последние 30 дней",
		From:  from.Format("2006-01-02"),
		To:    now.Format("2006-01-02"),
		Grain: "day",
	}
}

func startOfWeek(value time.Time) time.Time {
	offset := (int(value.Weekday()) + 6) % 7
	day := value.AddDate(0, 0, -offset)
	return time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, value.Location())
}

func callGigaChat(ctx context.Context, text string, layer shared.SemanticLayer) (shared.Intent, error) {
	token, err := getGigaChatToken(ctx)
	if err != nil {
		return shared.Intent{}, err
	}

	body := map[string]any{
		"model": getenv("GIGACHAT_MODEL", "GigaChat-2-Max"),
		"messages": []map[string]string{
			{"role": "system", "content": buildSystemPrompt(layer)},
			{"role": "user", "content": text},
		},
		"temperature": 0.1,
		"top_p":       0.3,
		"stream":      false,
	}

	rawBody, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, getenv("GIGACHAT_CHAT_URL", "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"), bytes.NewReader(rawBody))
	if err != nil {
		return shared.Intent{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := gigachatHTTPClient().Do(req)
	if err != nil {
		return shared.Intent{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return shared.Intent{}, fmt.Errorf("gigachat request failed: %s", string(body))
	}

	var payload struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return shared.Intent{}, err
	}
	if len(payload.Choices) == 0 {
		return shared.Intent{}, fmt.Errorf("gigachat returned no choices")
	}
	return decodeIntentFromLLM(payload.Choices[0].Message.Content)
}

func callYandexGPT(ctx context.Context, text string, layer shared.SemanticLayer) (shared.Intent, error) {
	authHeader := ""
	switch {
	case os.Getenv("YANDEX_IAM_TOKEN") != "":
		authHeader = "Bearer " + os.Getenv("YANDEX_IAM_TOKEN")
	case os.Getenv("YANDEX_API_KEY") != "":
		authHeader = "Api-Key " + os.Getenv("YANDEX_API_KEY")
	default:
		return shared.Intent{}, fmt.Errorf("missing yandex credentials")
	}

	modelURI := getenv("YANDEX_MODEL_URI", "")
	if modelURI == "" {
		folderID := getenv("YANDEX_FOLDER_ID", "")
		if folderID == "" {
			return shared.Intent{}, fmt.Errorf("missing yandex model uri")
		}
		modelURI = "gpt://" + folderID + "/yandexgpt/latest"
	}

	body := map[string]any{
		"modelUri": modelURI,
		"completionOptions": map[string]any{
			"stream":      false,
			"temperature": 0.1,
			"maxTokens":   "1200",
		},
		"messages": []map[string]string{
			{"role": "system", "text": buildSystemPrompt(layer)},
			{"role": "user", "text": text},
		},
	}

	rawBody, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, getenv("YANDEX_COMPLETION_URL", "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"), bytes.NewReader(rawBody))
	if err != nil {
		return shared.Intent{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return shared.Intent{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return shared.Intent{}, fmt.Errorf("yandexgpt request failed: %s", string(body))
	}

	var payload struct {
		Result struct {
			Alternatives []struct {
				Message struct {
					Text string `json:"text"`
				} `json:"message"`
			} `json:"alternatives"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return shared.Intent{}, err
	}
	if len(payload.Result.Alternatives) == 0 {
		return shared.Intent{}, fmt.Errorf("yandexgpt returned no alternatives")
	}
	return decodeIntentFromLLM(payload.Result.Alternatives[0].Message.Text)
}

func buildSystemPrompt(layer shared.SemanticLayer) string {
	var metrics []string
	for _, metric := range layer.Metrics {
		metrics = append(metrics, metric.ID+": "+metric.Title)
	}
	var dimensions []string
	for _, dimension := range layer.Dimensions {
		dimensions = append(dimensions, dimension.ID+": "+dimension.Title)
	}
	return fmt.Sprintf(`Ты NLU-модуль self-service аналитики.
Нужно интерпретировать русский запрос пользователя и вернуть только JSON без markdown.
Доступные метрики: %s.
Доступные группировки: %s.
Схема JSON:
{
  "metric": "metric_id",
  "group_by": "dimension_id или пустая строка",
  "filters": [{"field":"city|service_class|source_channel|driver_segment","operator":"=","value":"..."}],
  "period": {"label":"...","from":"YYYY-MM-DD","to":"YYYY-MM-DD","grain":"day"},
  "sort":"asc|desc",
  "limit": 10,
  "clarification":"если нужна дополнительная информация, иначе пустая строка",
  "assumptions":["..."],
  "confidence":0.0
}
Если запрос неоднозначный и метрику нельзя определить надежно, оставь "metric" пустым и задай clarification.`, strings.Join(metrics, "; "), strings.Join(dimensions, "; "))
}

func decodeIntentFromLLM(content string) (shared.Intent, error) {
	payload := extractJSONBlock(content)
	if payload == "" {
		return shared.Intent{}, fmt.Errorf("llm returned non-json response")
	}
	var intent shared.Intent
	if err := json.Unmarshal([]byte(payload), &intent); err != nil {
		return shared.Intent{}, err
	}
	return intent, nil
}

func extractJSONBlock(content string) string {
	start := strings.Index(content, "{")
	end := strings.LastIndex(content, "}")
	if start == -1 || end == -1 || end <= start {
		return ""
	}
	return content[start : end+1]
}

var gigaChatTokenCache struct {
	sync.Mutex
	Token     string
	ExpiresAt time.Time
}

func getGigaChatToken(ctx context.Context) (string, error) {
	gigaChatTokenCache.Lock()
	if gigaChatTokenCache.Token != "" && time.Until(gigaChatTokenCache.ExpiresAt) > 2*time.Minute {
		token := gigaChatTokenCache.Token
		gigaChatTokenCache.Unlock()
		return token, nil
	}
	gigaChatTokenCache.Unlock()

	authKey := strings.TrimSpace(os.Getenv("GIGACHAT_AUTH_KEY"))
	if authKey == "" {
		clientID := strings.TrimSpace(os.Getenv("GIGACHAT_CLIENT_ID"))
		clientSecret := strings.TrimSpace(os.Getenv("GIGACHAT_CLIENT_SECRET"))
		if clientID == "" || clientSecret == "" {
			return "", fmt.Errorf("missing gigachat credentials")
		}
		authKey = base64.StdEncoding.EncodeToString([]byte(clientID + ":" + clientSecret))
	}

	form := url.Values{}
	form.Set("scope", getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS"))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, getenv("GIGACHAT_AUTH_URL", "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"), strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("RqUID", randomUUID())
	req.Header.Set("Authorization", "Basic "+authKey)

	resp, err := gigachatHTTPClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("gigachat auth failed: %s", string(body))
	}

	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresAt   int64  `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	if payload.AccessToken == "" {
		return "", fmt.Errorf("empty gigachat token")
	}

	expiry := time.Now().Add(25 * time.Minute)
	if payload.ExpiresAt > 0 {
		expiry = time.Unix(payload.ExpiresAt, 0)
	}

	gigaChatTokenCache.Lock()
	gigaChatTokenCache.Token = payload.AccessToken
	gigaChatTokenCache.ExpiresAt = expiry
	gigaChatTokenCache.Unlock()
	return payload.AccessToken, nil
}

func randomUUID() string {
	raw := make([]byte, 16)
	_, _ = rand.Read(raw)
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(raw[0:4]),
		hex.EncodeToString(raw[4:6]),
		hex.EncodeToString(raw[6:8]),
		hex.EncodeToString(raw[8:10]),
		hex.EncodeToString(raw[10:16]),
	)
}

func handleSQLGeneration(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req shared.SQLGenerationRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if strings.TrimSpace(req.Text) == "" {
		shared.WriteError(w, http.StatusBadRequest, "text is required")
		return
	}
	if len(req.SemanticLayer.Metrics) == 0 {
		req.SemanticLayer = shared.DefaultSemanticLayer()
	}

	sqlResp, provider, err := generateSQL(r.Context(), req)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, sqlResp)
	_ = provider
}

func generateSQL(ctx context.Context, req shared.SQLGenerationRequest) (shared.SQLGenerationResponse, string, error) {
	provider := strings.ToLower(getenv("LLM_PROVIDER", "gigachat"))
	fallback := strings.ToLower(getenv("LLM_FALLBACK", "rule-based"))

	switch provider {
	case "gigachat":
		if resp, err := callGigaChatSQL(ctx, req); err == nil && resp.SQL != "" {
			return resp, "gigachat", nil
		}
	case "yandexgpt", "alice":
		if resp, err := callYandexGPTSQL(ctx, req); err == nil && resp.SQL != "" {
			return resp, "yandexgpt", nil
		}
	}

	// Fallback на rule-based генерацию
	resp := generateRuleBasedSQL(req)
	if resp.SQL == "" && fallback == "none" {
		return shared.SQLGenerationResponse{}, "", fmt.Errorf("failed to generate SQL")
	}
	return resp, "rule-based", nil
}

func callGigaChatSQL(ctx context.Context, req shared.SQLGenerationRequest) (shared.SQLGenerationResponse, error) {
	token, err := getGigaChatToken(ctx)
	if err != nil {
		return shared.SQLGenerationResponse{}, err
	}

	body := map[string]any{
		"model": getenv("GIGACHAT_MODEL", "GigaChat-2-Max"),
		"messages": []map[string]string{
			{"role": "system", "content": buildSQLSystemPrompt(req)},
			{"role": "user", "content": req.Text},
		},
		"temperature": 0.1,
		"top_p":       0.3,
		"stream":      false,
	}

	rawBody, _ := json.Marshal(body)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, getenv("GIGACHAT_CHAT_URL", "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"), bytes.NewReader(rawBody))
	if err != nil {
		return shared.SQLGenerationResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := gigachatHTTPClient().Do(httpReq)
	if err != nil {
		return shared.SQLGenerationResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return shared.SQLGenerationResponse{}, fmt.Errorf("gigachat request failed: %s", string(body))
	}

	var payload struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return shared.SQLGenerationResponse{}, err
	}
	if len(payload.Choices) == 0 {
		return shared.SQLGenerationResponse{}, fmt.Errorf("gigachat returned no choices")
	}
	return decodeSQLFromLLM(payload.Choices[0].Message.Content)
}

func callYandexGPTSQL(ctx context.Context, req shared.SQLGenerationRequest) (shared.SQLGenerationResponse, error) {
	authHeader := ""
	switch {
	case os.Getenv("YANDEX_IAM_TOKEN") != "":
		authHeader = "Bearer " + os.Getenv("YANDEX_IAM_TOKEN")
	case os.Getenv("YANDEX_API_KEY") != "":
		authHeader = "Api-Key " + os.Getenv("YANDEX_API_KEY")
	default:
		return shared.SQLGenerationResponse{}, fmt.Errorf("missing yandex credentials")
	}

	modelURI := getenv("YANDEX_MODEL_URI", "")
	if modelURI == "" {
		folderID := getenv("YANDEX_FOLDER_ID", "")
		if folderID == "" {
			return shared.SQLGenerationResponse{}, fmt.Errorf("missing yandex model uri")
		}
		modelURI = "gpt://" + folderID + "/yandexgpt/latest"
	}

	body := map[string]any{
		"modelUri": modelURI,
		"completionOptions": map[string]any{
			"stream":      false,
			"temperature": 0.1,
			"maxTokens":   "1200",
		},
		"messages": []map[string]string{
			{"role": "system", "text": buildSQLSystemPrompt(req)},
			{"role": "user", "text": req.Text},
		},
	}

	rawBody, _ := json.Marshal(body)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, getenv("YANDEX_COMPLETION_URL", "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"), bytes.NewReader(rawBody))
	if err != nil {
		return shared.SQLGenerationResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", authHeader)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return shared.SQLGenerationResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return shared.SQLGenerationResponse{}, fmt.Errorf("yandexgpt request failed: %s", string(body))
	}

	var payload struct {
		Result struct {
			Alternatives []struct {
				Message struct {
					Text string `json:"text"`
				} `json:"message"`
			} `json:"alternatives"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return shared.SQLGenerationResponse{}, err
	}
	if len(payload.Result.Alternatives) == 0 {
		return shared.SQLGenerationResponse{}, fmt.Errorf("yandexgpt returned no alternatives")
	}
	return decodeSQLFromLLM(payload.Result.Alternatives[0].Message.Text)
}

func buildSQLSystemPrompt(req shared.SQLGenerationRequest) string {
	var metrics []string
	for _, metric := range req.SemanticLayer.Metrics {
		metrics = append(metrics, metric.ID+": "+metric.Title)
	}
	var dimensions []string
	for _, dimension := range req.SemanticLayer.Dimensions {
		dimensions = append(dimensions, dimension.ID+": "+dimension.Title)
	}

	// Формируем список доступных значений
	var enumInfo string
	if len(req.Schema.EnumValues.Cities) > 0 {
		enumInfo += fmt.Sprintf("\nДоступные города: %s", strings.Join(req.Schema.EnumValues.Cities, ", "))
	}
	if len(req.Schema.EnumValues.ServiceClasses) > 0 {
		enumInfo += fmt.Sprintf("\nДоступные тарифы: %s", strings.Join(req.Schema.EnumValues.ServiceClasses, ", "))
	}

	return fmt.Sprintf(`Ты SQL-генератор для аналитической системы.
Задача: Сгенерировать безопасный PostgreSQL SELECT запрос на основе запроса пользователя.

ДОСТУПНЫЕ ТАБЛИЦЫ:
- analytics.v_ride_metrics (основная view)
  Колонки: stat_date, city, service_class, source_channel, driver_segment, 
           completed_rides, cancelled_rides, total_rides, gross_revenue_rub, avg_fare_rub, active_drivers

ДОСТУПНЫЕ МЕТРИКИ:
%s

ДОСТУПНЫЕ ИЗМЕРЕНИЯ (для GROUP BY):
%s
%s

ПРАВИЛА БЕЗОПАСНОСТИ (строго соблюдать):
1. ТОЛЬКО SELECT запросы
2. НЕТ точки с запятой
3. НЕТ комментариев (-- или /* */)
4. НЕТ подзапросов в WHERE
5. Использовать только разрешенные таблицы и колонки
6. Для фильтров использовать только существующие значения из списка выше
7. Если значение фильтра не из списка - верни предупреждение

ФОРМАТ ОТВЕТА (строго JSON):
{
  "sql": "SELECT ...",
  "explanation": "что делает запрос",
  "used_filters": [{"field": "city", "value": "Москва"}],
  "confidence": 0.95,
  "warnings": ["если есть проблемы"]
}

Примеры:
Запрос: "выручка по городам"
Ответ: {"sql": "SELECT city, ROUND(SUM(gross_revenue_rub)::numeric, 2) as revenue FROM analytics.v_ride_metrics WHERE stat_date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY city ORDER BY revenue DESC", "explanation": "Выручка по городам за последние 30 дней", "used_filters": [], "confidence": 0.95, "warnings": []}

Запрос: "поездки в Москве"
Ответ: {"sql": "SELECT SUM(completed_rides) as total_rides FROM analytics.v_ride_metrics WHERE city = 'Москва' AND stat_date >= CURRENT_DATE - INTERVAL '30 days'", "explanation": "Количество поездок в Москве за последние 30 дней", "used_filters": [{"field": "city", "value": "Москва"}], "confidence": 0.92, "warnings": []}`,
		strings.Join(metrics, "; "),
		strings.Join(dimensions, "; "),
		enumInfo)
}

func decodeSQLFromLLM(content string) (shared.SQLGenerationResponse, error) {
	payload := extractJSONBlock(content)
	if payload == "" {
		return shared.SQLGenerationResponse{}, fmt.Errorf("llm returned non-json response")
	}
	var resp shared.SQLGenerationResponse
	if err := json.Unmarshal([]byte(payload), &resp); err != nil {
		return shared.SQLGenerationResponse{}, err
	}
	return resp, nil
}

func generateRuleBasedSQL(req shared.SQLGenerationRequest) shared.SQLGenerationResponse {
	// Простая rule-based генерация SQL как fallback
	text := strings.ToLower(req.Text)
	resp := shared.SQLGenerationResponse{
		Confidence: 0.6,
		Warnings:   []string{},
	}

	// Определяем метрику
	metric := ""
	metricExpr := ""
	if strings.Contains(text, "выручк") {
		metric = "revenue"
		metricExpr = "ROUND(SUM(gross_revenue_rub)::numeric, 2)"
	} else if strings.Contains(text, "поездк") || strings.Contains(text, "заказ") {
		metric = "completed_rides"
		metricExpr = "SUM(completed_rides)"
	} else if strings.Contains(text, "отмен") {
		metric = "cancellations"
		metricExpr = "SUM(cancelled_rides)"
	} else if strings.Contains(text, "средний чек") {
		metric = "avg_fare"
		metricExpr = "ROUND(AVG(avg_fare_rub)::numeric, 2)"
	} else {
		resp.Warnings = append(resp.Warnings, "Не удалось определить метрику, используется выручка по умолчанию")
		metric = "revenue"
		metricExpr = "ROUND(SUM(gross_revenue_rub)::numeric, 2)"
	}

	// Определяем группировку
	groupCol := ""
	if strings.Contains(text, "по город") {
		groupCol = "city"
	} else if strings.Contains(text, "по тариф") {
		groupCol = "service_class"
	} else if strings.Contains(text, "по дням") {
		groupCol = "stat_date"
	}

	// Проверяем фильтры по городам
	usedFilters := []shared.Filter{}
	for _, city := range req.Schema.EnumValues.Cities {
		if strings.Contains(text, strings.ToLower(city)) {
			usedFilters = append(usedFilters, shared.Filter{Field: "city", Operator: "=", Value: city})
			// Проверяем что город существует
			found := false
			for _, validCity := range req.Schema.EnumValues.Cities {
				if validCity == city {
					found = true
					break
				}
			}
			if !found {
				resp.Warnings = append(resp.Warnings, fmt.Sprintf("Город '%s' не найден в списке доступных городов", city))
			}
		}
	}

	// Строим SQL
	var sql strings.Builder
	sql.WriteString("SELECT ")
	if groupCol != "" {
		sql.WriteString(groupCol + ", ")
	}
	sql.WriteString(metricExpr + " as " + metric)
	sql.WriteString(" FROM analytics.v_ride_metrics")
	sql.WriteString(" WHERE stat_date >= CURRENT_DATE - INTERVAL '30 days'")

	// Добавляем фильтры
	for i, filter := range usedFilters {
		if i == 0 {
			sql.WriteString(" AND ")
		} else {
			sql.WriteString(" AND ")
		}
		sql.WriteString(fmt.Sprintf("%s = '%s'", filter.Field, filter.Value))
	}

	if groupCol != "" {
		sql.WriteString(" GROUP BY " + groupCol)
		sql.WriteString(" ORDER BY " + metric + " DESC")
	}
	sql.WriteString(" LIMIT 12")

	resp.SQL = sql.String()
	resp.Explanation = fmt.Sprintf("Запрос на получение %s", metric)
	resp.UsedFilters = usedFilters

	return resp
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

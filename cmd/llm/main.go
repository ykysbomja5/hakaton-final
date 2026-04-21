package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
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

type providerSQLResponse struct {
	SQL           string  `json:"sql"`
	Clarification string  `json:"clarification"`
	Confidence    float64 `json:"confidence"`
}

var (
	gigachatClientOnce sync.Once
	gigachatClient     *http.Client
	gigachatClientErr  error
)

func gigachatHTTPClient() (*http.Client, error) {
	gigachatClientOnce.Do(func() {
		gigachatClient, gigachatClientErr = newGigachatHTTPClient()
	})
	return gigachatClient, gigachatClientErr
}

func newGigachatHTTPClient() (*http.Client, error) {
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		baseTransport = &http.Transport{}
	}

	transport := baseTransport.Clone()
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if strings.ToLower(getenv("GIGACHAT_INSECURE_SKIP_VERIFY", "false")) == "true" {
		tlsConfig.InsecureSkipVerify = true
	}

	caFile := strings.TrimSpace(os.Getenv("GIGACHAT_CA_CERT_FILE"))
	if caFile != "" {
		rootCAs, err := x509.SystemCertPool()
		if err != nil || rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}

		pemData, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read GIGACHAT_CA_CERT_FILE %q: %w", caFile, err)
		}
		if !rootCAs.AppendCertsFromPEM(pemData) {
			return nil, fmt.Errorf("failed to append certificates from GIGACHAT_CA_CERT_FILE %q", caFile)
		}
		tlsConfig.RootCAs = rootCAs
	}

	transport.TLSClientConfig = tlsConfig
	return &http.Client{
		Timeout:   60 * time.Second,
		Transport: transport,
	}, nil
}

func wrapGigaChatHTTPError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "certificate signed by unknown authority") {
		return fmt.Errorf("%w. Configure GIGACHAT_CA_CERT_FILE with your trusted root CA PEM or, for local dev only, set GIGACHAT_INSECURE_SKIP_VERIFY=true", err)
	}
	return err
}

func main() {
	if err := shared.LoadDotEnv(".env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}
	if err := validateProviderStartup(context.Background()); err != nil {
		log.Fatalf("llm startup validation failed: %v", err)
	}

	port := getenv("PORT", getenv("LLM_PORT", "8082"))
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/query", handleQuery)
	mux.HandleFunc("/v1/intent", handleQuery)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		provider, providerErr := activeProvider()
		if err := validateProviderConfig(); err != nil {
			if providerErr != nil {
				provider = requestedProvider()
			}
			shared.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status":   "error",
				"service":  "llm",
				"provider": provider,
				"error":    err.Error(),
			})
			return
		}
		shared.WriteJSON(w, http.StatusOK, map[string]string{
			"status":   "ok",
			"service":  "llm",
			"provider": provider,
		})
	})

	log.Printf("llm listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
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

	resp, err := generateSQLPlan(r.Context(), req.Text, req.SemanticLayer)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, resp)
}

func generateSQLPlan(ctx context.Context, text string, layer shared.SemanticLayer) (shared.SQLGenerationResponse, error) {
	intent := inferRuleBasedIntent(text, layer)

	sqlResp, provider, err := generateProviderSQL(ctx, text, layer, intent)
	if err != nil {
		return shared.SQLGenerationResponse{}, err
	}

	intent = mergeIntentWithSQLResponse(intent, sqlResp)
	if strings.TrimSpace(sqlResp.SQL) == "" && strings.TrimSpace(intent.Clarification) == "" {
		intent.Clarification = "Не удалось безопасно построить SQL, уточните метрику или фильтры."
	}

	return shared.SQLGenerationResponse{
		SQL:      compactSQL(sqlResp.SQL),
		Intent:   intent,
		Provider: provider,
	}, nil
}

func mergeIntentWithSQLResponse(intent shared.Intent, resp providerSQLResponse) shared.Intent {
	if resp.Confidence > 0 && resp.Confidence <= 1 {
		intent.Confidence = resp.Confidence
	}
	if strings.TrimSpace(resp.Clarification) != "" {
		intent.Clarification = strings.TrimSpace(resp.Clarification)
	}
	return intent
}

func compactSQL(sqlText string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(sqlText)), " ")
}

func requestedProvider() string {
	value := strings.ToLower(strings.TrimSpace(getenv("LLM_PROVIDER", "gigachat")))
	switch value {
	case "", "rule-based", "rule_based", "rulebased", "local":
		return "rule-based"
	case "alice", "yandex", "yandexgpt":
		return "yandexgpt"
	default:
		return value
	}
}

func allowRuleBasedFallback() bool {
	return strings.ToLower(getenv("LLM_ALLOW_RULE_BASED_FALLBACK", "false")) == "true"
}

func activeProvider() (string, error) {
	switch requestedProvider() {
	case "rule-based":
		return "rule-based", nil
	case "gigachat":
		if hasGigaChatCredentials() {
			return "gigachat", nil
		}
		if allowRuleBasedFallback() {
			return "rule-based", nil
		}
		return "", fmt.Errorf("gigachat credentials are required: set GIGACHAT_AUTH_KEY or GIGACHAT_CLIENT_ID with GIGACHAT_CLIENT_SECRET")
	case "yandexgpt":
		if hasYandexCredentials() {
			return "yandexgpt", nil
		}
		if allowRuleBasedFallback() {
			return "rule-based", nil
		}
		return "", fmt.Errorf("yandex credentials are required: set YANDEX_API_KEY or YANDEX_IAM_TOKEN and YANDEX_MODEL_URI or YANDEX_FOLDER_ID")
	default:
		if allowRuleBasedFallback() {
			return "rule-based", nil
		}
		return "", fmt.Errorf("unsupported LLM_PROVIDER %q", os.Getenv("LLM_PROVIDER"))
	}
}

func hasGigaChatCredentials() bool {
	authKey := strings.TrimSpace(os.Getenv("GIGACHAT_AUTH_KEY"))
	clientID := strings.TrimSpace(os.Getenv("GIGACHAT_CLIENT_ID"))
	clientSecret := strings.TrimSpace(os.Getenv("GIGACHAT_CLIENT_SECRET"))
	return authKey != "" || (clientID != "" && clientSecret != "")
}

func hasYandexCredentials() bool {
	hasAuth := strings.TrimSpace(os.Getenv("YANDEX_API_KEY")) != "" || strings.TrimSpace(os.Getenv("YANDEX_IAM_TOKEN")) != ""
	hasModel := strings.TrimSpace(os.Getenv("YANDEX_MODEL_URI")) != "" || strings.TrimSpace(os.Getenv("YANDEX_FOLDER_ID")) != ""
	return hasAuth && hasModel
}

func generateProviderSQL(ctx context.Context, text string, layer shared.SemanticLayer, intent shared.Intent) (providerSQLResponse, string, error) {
	provider, err := activeProvider()
	if err != nil {
		return providerSQLResponse{}, "", err
	}

	switch provider {
	case "rule-based":
		return buildRuleBasedSQLResponse(intent), "rule-based", nil
	case "gigachat":
		resp, err := callGigaChatSQL(ctx, text, layer)
		if err != nil {
			return fallbackToRuleBasedResponse("gigachat", err, intent)
		}
		return resp, "gigachat", nil
	case "yandexgpt":
		resp, err := callYandexGPTSQL(ctx, text, layer)
		if err != nil {
			return fallbackToRuleBasedResponse("yandexgpt", err, intent)
		}
		return resp, "yandexgpt", nil
	default:
		return providerSQLResponse{}, "", fmt.Errorf("unsupported LLM_PROVIDER %q", os.Getenv("LLM_PROVIDER"))
	}
}

func validateProviderStartup(ctx context.Context) error {
	provider, err := activeProvider()
	if err != nil {
		return err
	}
	if provider == "rule-based" && requestedProvider() != "rule-based" {
		log.Printf("llm provider %q is unavailable or not fully configured; using rule-based fallback", requestedProvider())
	}

	if err := validateProviderConfig(); err != nil {
		return err
	}

	if strings.ToLower(getenv("LLM_VALIDATE_ON_STARTUP", "false")) != "true" {
		return nil
	}
	if allowRuleBasedFallback() {
		return nil
	}

	switch provider {
	case "gigachat":
		_, err := getGigaChatToken(ctx)
		return err
	default:
		return nil
	}
}

func validateProviderConfig() error {
	provider, err := activeProvider()
	if err != nil {
		return err
	}

	switch provider {
	case "rule-based":
		return nil
	case "gigachat":
		if !hasGigaChatCredentials() {
			return fmt.Errorf("gigachat credentials are required: set GIGACHAT_AUTH_KEY or GIGACHAT_CLIENT_ID with GIGACHAT_CLIENT_SECRET")
		}
		return nil
	case "yandexgpt":
		if !hasYandexCredentials() {
			return fmt.Errorf("yandex credentials are required: set YANDEX_API_KEY or YANDEX_IAM_TOKEN and YANDEX_MODEL_URI or YANDEX_FOLDER_ID")
		}
		return nil
	default:
		return fmt.Errorf("unsupported LLM_PROVIDER %q", os.Getenv("LLM_PROVIDER"))
	}
}

func fallbackToRuleBasedResponse(provider string, err error, intent shared.Intent) (providerSQLResponse, string, error) {
	if !allowRuleBasedFallback() {
		return providerSQLResponse{}, "", err
	}
	log.Printf("%s request failed: %v; using rule-based fallback", provider, err)
	return buildRuleBasedSQLResponse(intent), "rule-based", nil
}

func buildRuleBasedSQLResponse(intent shared.Intent) providerSQLResponse {
	resp := providerSQLResponse{
		Clarification: strings.TrimSpace(intent.Clarification),
		Confidence:    math.Max(intent.Confidence, 0.35),
	}

	sqlText, err := buildRuleBasedSQL(intent)
	if err != nil {
		if resp.Clarification == "" {
			resp.Clarification = err.Error()
		}
		return resp
	}

	resp.SQL = sqlText
	return resp
}

func buildRuleBasedSQL(intent shared.Intent) (string, error) {
	if strings.TrimSpace(intent.Metric) == "" {
		return "", fmt.Errorf("unable to build sql without a metric")
	}

	metricExpr, ok := metricExpression(intent.Metric)
	if !ok {
		return "", fmt.Errorf("unsupported metric %q", intent.Metric)
	}

	whereClauses := buildWhereClauses(intent)
	groupExpr, groupAlias, isTimeGrouping, ok := groupExpression(intent.GroupBy)
	if !ok {
		return "", fmt.Errorf("unsupported group_by %q", intent.GroupBy)
	}

	var query strings.Builder
	if groupAlias == "" {
		fmt.Fprintf(&query, "select %s as metric_value from analytics.v_ride_metrics", metricExpr)
	} else {
		fmt.Fprintf(&query, "select %s as %s, %s as metric_value from analytics.v_ride_metrics", groupExpr, groupAlias, metricExpr)
	}

	if len(whereClauses) > 0 {
		query.WriteString(" where ")
		query.WriteString(strings.Join(whereClauses, " and "))
	}

	if groupAlias != "" {
		query.WriteString(" group by 1")
		if isTimeGrouping {
			query.WriteString(" order by 1 asc")
		} else {
			query.WriteString(" order by metric_value desc, 1 asc")
		}
		if intent.Limit > 0 {
			fmt.Fprintf(&query, " limit %d", minInt(intent.Limit, 100))
		}
	}

	return compactSQL(query.String()), nil
}

func metricExpression(metric string) (string, bool) {
	switch metric {
	case "completed_rides":
		return "sum(completed_rides)", true
	case "total_rides":
		return "sum(total_rides)", true
	case "cancellations":
		return "sum(cancelled_rides)", true
	case "revenue":
		return "round(sum(gross_revenue_rub)::numeric, 2)", true
	case "avg_fare":
		return "round(sum(gross_revenue_rub) / nullif(sum(completed_rides), 0), 2)", true
	case "active_drivers":
		return "max(active_drivers)", true
	default:
		return "", false
	}
}

func groupExpression(groupBy string) (string, string, bool, bool) {
	switch groupBy {
	case "":
		return "", "", false, true
	case "city":
		return "city", "group_value", false, true
	case "service_class":
		return "service_class", "group_value", false, true
	case "source_channel":
		return "source_channel", "group_value", false, true
	case "driver_segment":
		return "driver_segment", "group_value", false, true
	case "day":
		return "date_trunc('day', stat_date)::date", "period_value", true, true
	case "week":
		return "date_trunc('week', stat_date)::date", "period_value", true, true
	case "month":
		return "date_trunc('month', stat_date)::date", "period_value", true, true
	default:
		return "", "", false, false
	}
}

func buildWhereClauses(intent shared.Intent) []string {
	clauses := make([]string, 0, len(intent.Filters)+2)
	if intent.Period.From != "" && intent.Period.To != "" {
		clauses = append(clauses, fmt.Sprintf("stat_date between date %s and date %s", quoteSQLLiteral(intent.Period.From), quoteSQLLiteral(intent.Period.To)))
	}

	allowedFields := map[string]string{
		"city":           "city",
		"service_class":  "service_class",
		"source_channel": "source_channel",
		"driver_segment": "driver_segment",
	}
	for _, filter := range intent.Filters {
		column, ok := allowedFields[filter.Field]
		if !ok {
			continue
		}
		if filter.Operator != "=" {
			continue
		}
		clauses = append(clauses, fmt.Sprintf("%s = %s", column, quoteSQLLiteral(filter.Value)))
	}

	return clauses
}

func quoteSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func callGigaChatSQL(ctx context.Context, text string, layer shared.SemanticLayer) (providerSQLResponse, error) {
	token, err := getGigaChatToken(ctx)
	if err != nil {
		return providerSQLResponse{}, err
	}

	body := map[string]any{
		"model": getenv("GIGACHAT_MODEL", "GigaChat-2-Max"),
		"messages": []map[string]string{
			{"role": "system", "content": buildSQLSystemPrompt(layer)},
			{"role": "user", "content": text},
		},
		"temperature": 0.1,
		"top_p":       0.3,
		"stream":      false,
	}

	rawBody, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, getenv("GIGACHAT_CHAT_URL", "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"), bytes.NewReader(rawBody))
	if err != nil {
		return providerSQLResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client, err := gigachatHTTPClient()
	if err != nil {
		return providerSQLResponse{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return providerSQLResponse{}, wrapGigaChatHTTPError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return providerSQLResponse{}, fmt.Errorf("gigachat request failed: %s", string(body))
	}

	var payload struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return providerSQLResponse{}, err
	}
	if len(payload.Choices) == 0 {
		return providerSQLResponse{}, fmt.Errorf("gigachat returned no choices")
	}
	return decodeSQLFromLLM(payload.Choices[0].Message.Content)
}

func callYandexGPTSQL(ctx context.Context, text string, layer shared.SemanticLayer) (providerSQLResponse, error) {
	authHeader := ""
	switch {
	case os.Getenv("YANDEX_IAM_TOKEN") != "":
		authHeader = "Bearer " + os.Getenv("YANDEX_IAM_TOKEN")
	case os.Getenv("YANDEX_API_KEY") != "":
		authHeader = "Api-Key " + os.Getenv("YANDEX_API_KEY")
	default:
		return providerSQLResponse{}, fmt.Errorf("missing yandex credentials")
	}

	modelURI := getenv("YANDEX_MODEL_URI", "")
	if modelURI == "" {
		folderID := getenv("YANDEX_FOLDER_ID", "")
		if folderID == "" {
			return providerSQLResponse{}, fmt.Errorf("missing yandex model uri")
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
			{"role": "system", "text": buildSQLSystemPrompt(layer)},
			{"role": "user", "text": text},
		},
	}

	rawBody, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, getenv("YANDEX_COMPLETION_URL", "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"), bytes.NewReader(rawBody))
	if err != nil {
		return providerSQLResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return providerSQLResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return providerSQLResponse{}, fmt.Errorf("yandexgpt request failed: %s", string(body))
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
		return providerSQLResponse{}, err
	}
	if len(payload.Result.Alternatives) == 0 {
		return providerSQLResponse{}, fmt.Errorf("yandexgpt returned no alternatives")
	}
	return decodeSQLFromLLM(payload.Result.Alternatives[0].Message.Text)
}

func buildSQLSystemPrompt(layer shared.SemanticLayer) string {
	metricHints := []string{
		"completed_rides -> sum(completed_rides) as metric_value",
		"total_rides -> sum(total_rides) as metric_value",
		"cancellations -> sum(cancelled_rides) as metric_value",
		"revenue -> round(sum(gross_revenue_rub)::numeric, 2) as metric_value",
		"avg_fare -> round(sum(gross_revenue_rub) / nullif(sum(completed_rides), 0), 2) as metric_value",
		"active_drivers -> max(active_drivers) as metric_value",
	}

	dimensionHints := []string{
		"city -> city as group_value",
		"service_class -> service_class as group_value",
		"source_channel -> source_channel as group_value",
		"driver_segment -> driver_segment as group_value",
		"day -> date_trunc('day', stat_date)::date as period_value",
		"week -> date_trunc('week', stat_date)::date as period_value",
		"month -> date_trunc('month', stat_date)::date as period_value",
	}

	allowedValues := make([]string, 0, len(layer.Dimensions))
	for _, dimension := range layer.Dimensions {
		if len(dimension.Values) == 0 {
			continue
		}
		allowedValues = append(allowedValues, dimension.ID+": "+strings.Join(dimension.Values, ", "))
	}

	return fmt.Sprintf(`Ты преобразуешь запрос аналитика на русском языке в безопасный PostgreSQL SQL.
Верни только JSON без markdown в формате:
{
  "sql": "SELECT ...",
  "clarification": "",
  "confidence": 0.0
}

Обязательные правила:
1. Разрешён только один SELECT без комментариев и без точки с запятой.
2. Используй только analytics.v_ride_metrics.
3. Нельзя обращаться к другим схемам, таблицам, information_schema, pg_catalog и app.
4. Разрешённые колонки: stat_date, city, service_class, source_channel, driver_segment, completed_rides, cancelled_rides, total_rides, gross_revenue_rub, avg_fare_rub, active_drivers.
5. Если нужна группировка, используй алиас group_value или period_value для первой колонки и metric_value для числовой метрики.
6. Если вопрос неясен или безопасный SQL построить нельзя, верни пустой sql и напиши clarification.
7. Ограничивай многорядные ответы LIMIT от 1 до 100.
8. Для фильтров по датам используй stat_date.

Подсказки по метрикам:
%s

Подсказки по измерениям:
%s

Допустимые значения справочников:
%s`, strings.Join(metricHints, "\n"), strings.Join(dimensionHints, "\n"), strings.Join(allowedValues, "\n"))
}

func decodeSQLFromLLM(content string) (providerSQLResponse, error) {
	payload := extractJSONBlock(content)
	if payload == "" {
		return providerSQLResponse{}, fmt.Errorf("llm returned non-json response")
	}
	var resp providerSQLResponse
	if err := json.Unmarshal([]byte(payload), &resp); err != nil {
		return providerSQLResponse{}, err
	}
	return resp, nil
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

	client, err := gigachatHTTPClient()
	if err != nil {
		return "", err
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", wrapGigaChatHTTPError(err)
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

func inferRuleBasedIntent(text string, layer shared.SemanticLayer) shared.Intent {
	now := time.Now()
	normalized := shared.NormalizeText(text)
	intent := shared.Intent{
		Filters:     []shared.Filter{},
		Period:      defaultPeriod(now),
		Sort:        "desc",
		Limit:       0,
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

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

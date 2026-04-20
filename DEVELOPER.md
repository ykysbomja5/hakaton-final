# Drivee Analytics — Developer Guide

> Полное техническое описание проекта для разработчиков

---

## 📑 Содержание

- [Обзор архитектуры](#обзор-архитектуры)
- [Структура проекта](#структура-проекта)
- [Микросервисы](#микросервисы)
- [Data Flow](#data-flow)
- [API Reference](#api-reference)
- [База данных](#база-данных)
- [Конфигурация](#конфигурация)
- [Разработка](#разработка)
- [Отладка](#отладка)
- [Тестирование](#тестирование)
- [Deployment](#deployment)

---

## Обзор архитектуры

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              КЛИЕНТСКИЙ СЛОЙ                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                         │
│  │   Browser   │  │   Mobile    │  │    API      │                         │
│  │   (Web UI)  │  │   (Future)  │  │   Clients   │                         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                         │
└─────────┼────────────────┼────────────────┼─────────────────────────────────┘
          │                │                │
          └────────────────┴────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────────────────┐
│                              GATEWAY LAYER                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         API Gateway (8080)                          │   │
│  │  • Static file serving (web/)                                       │   │
│  │  • Request routing                                                  │   │
│  │  • CORS handling                                                    │   │
│  │  • Load balancing (future)                                          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────┬──────────────────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
┌─────────▼─────────┐ ┌────▼─────┐ ┌────────▼────────┐
│   META SERVICE    │ │  SERVICE │ │  LLM SERVICE    │
│     (8084)        │ │          │ │    (8082)       │
│                   │ │          │ │                 │
│ • Semantic layer  │ │          │ │ • Intent parser │
│ • Metrics catalog │ │          │ │ • GigaChat API  │
│ • Business terms  │ │          │ │ • YandexGPT API │
│ • Sample queries  │ │          │ │ • Rule-based    │
└─────────┬─────────┘ └──────────┘ └────────┬────────┘
          │                                 │
          │         ┌───────────────┐       │
          └────────►│ QUERY SERVICE │◄──────┘
                    │    (8081)     │
                    │               │
                    │ • SQL Builder │
                    │ • Validator   │
                    │ • Executor    │
                    │ • Explain     │
                    └───────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
    ┌─────────▼─────┐ ┌─────▼─────┐ ┌─────▼──────┐
    │    REPORTS    │ │    DB     │ │   CACHE    │
    │   (8083)      │ │           │ │  (future)  │
    │               │ │ PostgreSQL│ │            │
    │ • Save reports│ │           │ │            │
    │ • History     │ │ • Analytics│ │            │
    │ • Re-run      │ │ • App data│ │            │
    └───────────────┘ └───────────┘ └────────────┘
```

---

## Структура проекта

```
drivee-analytics/
│
├── 📁 cmd/                          # Entry points for each service
│   ├── 📁 gateway/                  # API Gateway service
│   │   └── main.go                  # HTTP server, static files, proxy
│   │
│   ├── 📁 llm/                      # LLM Service
│   │   └── main.go                  # Intent parsing, GigaChat, YandexGPT
│   │
│   ├── 📁 meta/                     # Meta Service
│   │   └── main.go                  # Semantic layer API
│   │
│   ├── 📁 query/                    # Query Service
│   │   └── main.go                  # SQL builder, validator, executor
│   │
│   └── 📁 reports/                  # Reports Service
│       └── main.go                  # Report CRUD, re-run logic
│
├── 📁 internal/                     # Private application code
│   └── 📁 shared/                   # Shared packages
│       ├── contracts.go             # Data structures (Intent, QueryRequest, etc.)
│       ├── http.go                  # HTTP utilities
│       └── pg.go                    # PostgreSQL connection pool
│
├── 📁 web/                          # Frontend static files
│   ├── index.html                   # Main page
│   ├── reports.html                 # Reports page
│   ├── app.js                       # Main application logic
│   ├── reports.js                   # Reports page logic
│   └── styles.css                   # Styles
│
├── 📁 db/                           # Database
│   ├── schema.sql                   # DDL: tables, views, indexes, roles
│   └── seed.sql                     # Demo data
│
├── 📁 docs/                         # Documentation
│   ├── architecture.md              # Architecture decisions
│   └── implementation-plan.md       # Implementation roadmap
│
├── 📁 scripts/                      # Utility scripts
│   └── run-local.ps1                # PowerShell script to run all services
│
├── docker-compose.yml               # Docker Compose for PostgreSQL
├── go.mod                           # Go module definition
├── go.sum                           # Go dependencies checksum
├── .env.example                     # Environment variables template
└── README.md                        # User-facing documentation
```

---

## Микросервисы

### 1. Gateway Service (8080)

**Ответственность:** Единая точка входа для всех клиентских запросов

**Функции:**
- Раздача статических файлов (HTML, CSS, JS)
- Проксирование API-запросов к внутренним сервисам
- CORS обработка
- (Future) Load balancing, rate limiting

**Endpoints:**
```
GET  /              → index.html
GET  /reports       → reports.html
GET  /api/*         → Proxy to respective service
POST /api/*         → Proxy to respective service
```

**Конфигурация:**
```env
GATEWAY_PORT=8080
QUERY_SERVICE_URL=http://localhost:8081
LLM_SERVICE_URL=http://localhost:8082
REPORTS_SERVICE_URL=http://localhost:8083
META_SERVICE_URL=http://localhost:8084
```

---

### 2. Meta Service (8084)

**Ответственность:** Предоставление semantic layer — каталога доступных метрик и измерений

**Функции:**
- Возврат списка метрик
- Возврат списка измерений
- Бизнес-термины и синонимы
- Примеры вопросов

**Endpoints:**
```
GET /semantic-layer
```

**Response:**
```json
{
  "metrics": [
    {
      "id": "revenue",
      "title": "Выручка",
      "description": "Суммарная выручка в рублях",
      "format": "currency"
    }
  ],
  "dimensions": [
    {
      "id": "city",
      "title": "Город",
      "column": "city",
      "description": "Город поездки",
      "values": ["Москва", "Санкт-Петербург", "Казань"]
    }
  ],
  "terms": [
    {
      "term": "выручка",
      "kind": "metric",
      "canonical": "revenue",
      "description": "Суммарная выручка"
    }
  ],
  "sample_questions": [
    "Покажи выручку по городам за последние 30 дней"
  ]
}
```

---

### 3. LLM Service (8082)

**Ответственность:** Парсинг естественного языка в структурированный intent

**Функции:**
- Интерпретация русского текста
- Поддержка нескольких провайдеров:
  - **GigaChat** (Sber) — основной
  - **YandexGPT** — альтернатива
  - **Rule-based** — fallback без API ключей
- Возврат структурированного Intent JSON

**Endpoints:**
```
POST /parse
```

**Request:**
```json
{
  "text": "Покажи выручку по городам за последние 30 дней",
  "semantic_layer": { ... }
}
```

**Response:**
```json
{
  "intent": {
    "metric": "revenue",
    "group_by": "city",
    "period": {
      "label": "последние 30 дней",
      "from": "2024-01-01",
      "to": "2024-01-31",
      "grain": "day"
    },
    "confidence": 0.95
  },
  "provider": "gigachat"
}
```

**Конфигурация:**
```env
LLM_PROVIDER=gigachat          # gigachat | yandexgpt | rule-based
LLM_FALLBACK=rule-based

# GigaChat
GIGACHAT_AUTH_KEY=your_key
GIGACHAT_SCOPE=GIGACHAT_API_PERS
GIGACHAT_MODEL=GigaChat-2-Max

# YandexGPT
YANDEX_API_KEY=your_key
YANDEX_FOLDER_ID=your_folder
```

---

### 4. Query Service (8081)

**Ответственность:** Ядро системы — построение и выполнение SQL-запросов

**Функции:**
- Получение semantic layer от Meta
- Отправка текста в LLM Service
- Валидация Intent (allowlist проверка)
- Построение SQL по шаблонам
- Security validation (только SELECT)
- Выполнение в PostgreSQL (read-only)
- Генерация метаданных для графиков
- Explainability (объяснение результата)

**Endpoints:**
```
POST /parse     # Только парсинг без выполнения
POST /run       # Парсинг + выполнение
```

**Request (/run):**
```json
{
  "text": "Покажи выручку по городам"
}
```

**Response:**
```json
{
  "intent": { ... },
  "preview": {
    "summary": "Выручка за последние 30 дней, сгруппировано по городу",
    "metric_label": "Выручка",
    "group_by_label": "Город",
    "confidence_label": "Высокая"
  },
  "sql": "SELECT city, SUM(gross_revenue_rub) as revenue ...",
  "result": {
    "columns": ["city", "revenue"],
    "rows": [["Москва", "2400000"], ...],
    "count": 5
  },
  "chart": {
    "type": "bar",
    "x_key": "city",
    "y_key": "revenue"
  }
}
```

**SQL Builder Logic:**
```go
// Псевдокод построения SQL
func BuildSQL(intent Intent) string {
    // 1. Проверка allowlist
    if !isAllowedMetric(intent.Metric) {
        return error
    }
    
    // 2. Построение SELECT
    selectClause := fmt.Sprintf("SELECT %s, %s(...)", 
        intent.GroupBy, intent.Metric)
    
    // 3. FROM с проверкой
    fromClause := "FROM analytics.v_ride_metrics"
    
    // 4. WHERE с параметризацией
    whereClause := buildWhere(intent.Filters, intent.Period)
    
    // 5. GROUP BY
    groupClause := fmt.Sprintf("GROUP BY %s", intent.GroupBy)
    
    // 6. ORDER и LIMIT
    return strings.Join([]string{selectClause, fromClause, 
        whereClause, groupClause, orderClause}, " ")
}
```

---

### 5. Reports Service (8083)

**Ответственность:** Управление сохранёнными отчётами

**Функции:**
- Сохранение отчёта (query + SQL + intent)
- Список сохранённых отчётов
- Повторный запуск отчёта
- История запусков

**Endpoints:**
```
GET    /reports              # Список отчётов
POST   /reports              # Создать отчёт
POST   /reports/{id}/run     # Перезапустить отчёт
```

**Database Schema:**
```sql
CREATE TABLE app.saved_reports (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    query_text TEXT NOT NULL,
    sql_text TEXT NOT NULL,
    intent JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE app.report_runs (
    id BIGSERIAL PRIMARY KEY,
    report_id BIGINT REFERENCES app.saved_reports(id),
    executed_at TIMESTAMPTZ DEFAULT NOW(),
    status TEXT NOT NULL,
    row_count INTEGER DEFAULT 0,
    error_text TEXT
);
```

---

## Data Flow

### Полный цикл запроса

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Client  │────►│  Gateway │────►│  Query   │────►│   Meta   │
│  (User)  │     │  (8080)  │     │ (8081)   │     │ (8084)   │
└──────────┘     └──────────┘     └────┬─────┘     └────┬─────┘
     ▲                                 │                │
     │                                 │◄───────────────┘
     │                                 │  Semantic Layer
     │                                 │
     │                            ┌────▼─────┐
     │                            │   LLM    │
     │                            │ (8082)   │
     │                            └────┬─────┘
     │                                 │ Intent JSON
     │                                 │
     │                            ┌────▼─────┐     ┌──────────┐
     │                            │  SQL     │────►│ PostgreSQL│
     │                            │ Builder  │     │(read-only)│
     │                            └────┬─────┘     └────┬─────┘
     │                                 │                │
     │                                 │◄───────────────┘
     │                                 │  Result
     │                            ┌────▼─────┐
     └────────────────────────────│ Response │
                                  │ (JSON)   │
                                  └──────────┘
```

### Sequence Diagram

```mermaid
sequenceDiagram
    participant U as User
    participant G as Gateway
    participant Q as Query
    participant M as Meta
    participant L as LLM
    participant DB as PostgreSQL

    U->>G: POST /api/query/run
    G->>Q: Forward request
    
    Q->>M: GET /semantic-layer
    M-->>Q: Return metrics & dimensions
    
    Q->>L: POST /parse
    L-->>Q: Return intent JSON
    
    Q->>Q: Validate intent
    Q->>Q: Build SQL
    Q->>Q: Security check
    
    Q->>DB: Execute SELECT
    DB-->>Q: Return rows
    
    Q->>Q: Generate chart metadata
    Q-->>G: JSON response
    G-->>U: Display result
```

---

## API Reference

### Общие форматы

**Base URL:** `http://localhost:8080`

**Headers:**
```
Content-Type: application/json
```

### Endpoints

#### Parse Query (without execution)
```http
POST /api/query/parse
```

**Request:**
```json
{
  "text": "выручка по городам"
}
```

**Response:**
```json
{
  "intent": {
    "metric": "revenue",
    "group_by": "city",
    "period": { ... },
    "confidence": 0.92
  },
  "preview": {
    "summary": "Выручка, сгруппировано по городу",
    "metric_label": "Выручка",
    "group_by_label": "Город",
    "confidence_label": "Высокая"
  },
  "semantic_layer": { ... }
}
```

---

#### Execute Query
```http
POST /api/query/run
```

**Request:**
```json
{
  "text": "покажи выручку по городам за последние 7 дней"
}
```

**Response:**
```json
{
  "intent": { ... },
  "preview": { ... },
  "sql": "SELECT ...",
  "result": {
    "columns": ["city", "revenue"],
    "rows": [["Москва", "2400000"], ...],
    "count": 5
  },
  "chart": {
    "type": "bar",
    "x_key": "city",
    "y_key": "revenue"
  },
  "semantic_layer": { ... }
}
```

---

#### List Reports
```http
GET /api/reports
```

**Response:**
```json
[
  {
    "id": 1,
    "name": "Выручка по городам",
    "query_text": "выручка по городам",
    "created_at": "2024-01-15T10:30:00Z"
  }
]
```

---

#### Save Report
```http
POST /api/reports
```

**Request:**
```json
{
  "name": "Мой отчёт",
  "query_text": "выручка по городам",
  "sql_text": "SELECT ...",
  "intent": { ... }
}
```

---

#### Re-run Report
```http
POST /api/reports/{id}/run
```

**Response:**
```json
{
  "report_id": 1,
  "executed_at": "2024-01-15T11:00:00Z",
  "status": "success",
  "result": { ... }
}
```

---

## База данных

### Схемы

```sql
-- Аналитические данные
CREATE SCHEMA analytics;

-- Служебные таблицы приложения
CREATE SCHEMA app;
```

### Таблицы

#### analytics.ride_metrics_daily
Основная аналитическая таблица с агрегированными данными.

```sql
CREATE TABLE analytics.ride_metrics_daily (
    stat_date DATE NOT NULL,
    city TEXT NOT NULL,
    service_class TEXT NOT NULL,
    source_channel TEXT NOT NULL,
    driver_segment TEXT NOT NULL,
    completed_rides INTEGER NOT NULL,
    cancelled_rides INTEGER NOT NULL,
    gross_revenue_rub NUMERIC(14,2) NOT NULL,
    avg_fare_rub NUMERIC(10,2) NOT NULL,
    active_drivers INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stat_date, city, service_class, source_channel, driver_segment)
);
```

#### analytics.v_ride_metrics
View для Query Service.

```sql
CREATE VIEW analytics.v_ride_metrics AS
SELECT 
    stat_date,
    city,
    service_class,
    source_channel,
    driver_segment,
    completed_rides,
    cancelled_rides,
    completed_rides + cancelled_rides as total_rides,
    gross_revenue_rub,
    avg_fare_rub,
    active_drivers
FROM analytics.ride_metrics_daily;
```

#### app.saved_reports
Сохранённые отчёты пользователей.

```sql
CREATE TABLE app.saved_reports (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    query_text TEXT NOT NULL,
    sql_text TEXT NOT NULL,
    intent JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### app.query_logs
Журнал всех запросов для аудита.

```sql
CREATE TABLE app.query_logs (
    id BIGSERIAL PRIMARY KEY,
    query_text TEXT NOT NULL,
    intent JSONB NOT NULL,
    sql_text TEXT,
    confidence NUMERIC(4,2) NOT NULL,
    status TEXT NOT NULL,
    latency_ms BIGINT DEFAULT 0,
    error_text TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Безопасность на уровне БД

```sql
-- Read-only роль для приложения
CREATE ROLE analytics_readonly LOGIN PASSWORD 'analytics_demo';

GRANT USAGE ON SCHEMA analytics TO analytics_readonly;
GRANT SELECT ON analytics.ride_metrics_daily TO analytics_readonly;
GRANT SELECT ON analytics.v_ride_metrics TO analytics_readonly;

-- Нет прав на INSERT/UPDATE/DELETE!
```

---

## Конфигурация

### Полный список переменных окружения

| Переменная | Описание | Значение по умолчанию |
|------------|----------|----------------------|
| `PG_DSN` | PostgreSQL connection string | `postgres://postgres:postgres@localhost:5432/drivee_analytics?sslmode=disable` |
| `GATEWAY_PORT` | Gateway service port | `8080` |
| `QUERY_PORT` | Query service port | `8081` |
| `LLM_PORT` | LLM service port | `8082` |
| `REPORTS_PORT` | Reports service port | `8083` |
| `META_PORT` | Meta service port | `8084` |
| `QUERY_SERVICE_URL` | Query service URL | `http://localhost:8081` |
| `LLM_SERVICE_URL` | LLM service URL | `http://localhost:8082` |
| `REPORTS_SERVICE_URL` | Reports service URL | `http://localhost:8083` |
| `META_SERVICE_URL` | Meta service URL | `http://localhost:8084` |
| `LLM_PROVIDER` | LLM provider | `gigachat` |
| `LLM_FALLBACK` | Fallback provider | `rule-based` |
| `GIGACHAT_AUTH_KEY` | GigaChat API key | - |
| `GIGACHAT_SCOPE` | GigaChat scope | `GIGACHAT_API_PERS` |
| `GIGACHAT_MODEL` | GigaChat model | `GigaChat-2-Max` |
| `YANDEX_API_KEY` | Yandex API key | - |
| `YANDEX_FOLDER_ID` | Yandex folder ID | - |

### Профили конфигурации

#### Development (без API ключей)
```env
LLM_PROVIDER=rule-based
PG_DSN=postgres://postgres:postgres@localhost:5432/drivee_analytics?sslmode=disable
```

#### Production (с GigaChat)
```env
LLM_PROVIDER=gigachat
GIGACHAT_AUTH_KEY=your_key_here
PG_DSN=postgres://analytics_readonly:password@prod-db:5432/drivee_analytics?sslmode=require
```

---

## Разработка

### Добавление новой метрики

1. **Update `internal/shared/contracts.go`:**
```go
// Add to DefaultSemanticLayer()
Metrics: []MetricDefinition{
    // ... existing metrics
    {
        ID:          "new_metric",
        Title:       "Новая метрика",
        Description: "Описание метрики",
        Format:      "integer", // or "currency", "percent"
    },
}
```

2. **Add business term:**
```go
Terms: []BusinessTerm{
    // ... existing terms
    {
        Term:        "новый термин",
        Kind:        "metric",
        Canonical:   "new_metric",
        Description: "Описание",
    },
}
```

3. **Update database view (`db/schema.sql`):**
```sql
CREATE OR REPLACE VIEW analytics.v_ride_metrics AS
SELECT 
    -- ... existing columns
    new_column as new_metric
FROM analytics.ride_metrics_daily;
```

4. **Restart Meta Service**

---

### Добавление нового сервиса

1. Create directory: `cmd/newservice/`
2. Create `main.go` with HTTP server
3. Add port to `.env.example`
4. Update `scripts/run-local.ps1`
5. Update Gateway proxy rules

---

## Отладка

### Логи сервисов

Логи сохраняются в `.runlogs/`:
```
.runlogs/
├── gateway.out.log
├── gateway.err.log
├── query.out.log
├── query.err.log
├── llm.out.log
├── llm.err.log
├── reports.out.log
├── reports.err.log
├── meta.out.log
└── meta.err.log
```

### Проверка работоспособности

```bash
# Check all services
curl http://localhost:8080
curl http://localhost:8081/health
curl http://localhost:8082/health
curl http://localhost:8083/health
curl http://localhost:8084/semantic-layer
```

### Debug mode

```bash
# Run single service with debug output
go run ./cmd/query

# Or with environment
debug=true go run ./cmd/query
```

---

## Тестирование

### Unit тесты

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package
go test ./internal/shared/...
```

### Integration тесты

```bash
# Test database connection
go test -tags=integration ./...

# Test full pipeline
curl -X POST http://localhost:8081/run \
  -H "Content-Type: application/json" \
  -d '{"text": "тестовый запрос"}'
```

### Load testing

```bash
# Using wrk
wrk -t12 -c400 -d30s http://localhost:8080

# Using ab (Apache Bench)
ab -n 1000 -c 10 http://localhost:8080/
```

---

## Deployment

### Docker Build

```dockerfile
# Dockerfile for each service
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /gateway ./cmd/gateway

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /gateway .
CMD ["./gateway"]
```

### Kubernetes (future)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: query-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: query
  template:
    metadata:
      labels:
        app: query
    spec:
      containers:
      - name: query
        image: drivee/query:latest
        ports:
        - containerPort: 8081
```

---

## Troubleshooting

### Common Issues

| Проблема | Решение |
|----------|---------|
| Service won't start | Check port availability: `netstat -an \| findstr 8080` |
| Database connection error | Verify PG_DSN and PostgreSQL running |
| LLM timeout | Check internet connection or switch to rule-based |
| CORS error | Verify Gateway CORS headers |
| Slow queries | Check database indexes |

### Debug Commands

```bash
# Check running processes
Get-Process \| Where-Object {$_.ProcessName -like "*go*"}

# Check ports
netstat -ano \| findstr :8080

# View logs
tail -f .runlogs/query.err.log

# Database check
psql -d drivee_analytics -c "SELECT COUNT(*) FROM analytics.ride_metrics_daily;"
```

---

## Contributing

### Git Workflow

1. Create feature branch: `git checkout -b feature/new-feature`
2. Make changes and commit: `git commit -m "feat: description"`
3. Push to remote: `git push origin feature/new-feature`
4. Create Pull Request

### Commit Convention

- `feat:` — новая функциональность
- `fix:` — исправление бага
- `docs:` — документация
- `refactor:` — рефакторинг
- `test:` — тесты
- `chore:` — обслуживание

---

## Resources

- [Go Documentation](https://golang.org/doc/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [GigaChat API](https://developers.sber.ru/)
- [YandexGPT API](https://yandex.cloud/ru/docs/foundation-models/)

---

**Last Updated:** 2024

**Maintainers:** Drivee Analytics Team

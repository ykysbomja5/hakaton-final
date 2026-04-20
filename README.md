# Drivee Analytics MVP

Микросервисный прототип self-service аналитической платформы на русском языке. Позволяет бизнес-пользователям задавать вопросы естественным языком и получать структурированные аналитические отчёты с визуализацией — без знания SQL.

## Что делает этот проект

Проект решает задачу **Natural Language to SQL (NL→SQL)** для аналитики такси-сервиса:

1. Пользователь вводит вопрос на русском языке: *"Покажи выручку по городам за последние 30 дней"*
2. Система интерпретирует запрос через LLM (GigaChat/YandexGPT) или локальный rule-based парсер
3. Формирует безопасный SQL-запрос с учётом semantic layer (разрешённые метрики и измерения)
4. Выполняет запрос в PostgreSQL через read-only соединение
5. Возвращает результат в виде таблицы и графика
6. Позволяет сохранить отчёт для повторного использования

### Ключевые особенности

- **Безопасность**: LLM никогда не генерирует SQL напрямую — только структурированный intent
- **Explainability**: Пользователь видит интерпретацию запроса, confidence score и SQL preview
- **Guardrails**: Только SELECT-запросы, allowlist метрик/измерений, read-only роль БД
- **Fallback**: Работает без внешних API ключей в rule-based режиме

---

## Архитектура

### Микросервисы

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              GATEWAY (8080)                                 │
│  - Отдаёт статический frontend (HTML/CSS/JS)                               │
│  - Проксирует API-запросы к внутренним сервисам                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
        ┌─────────────┬───────────────┼───────────────┬─────────────┐
        ▼             ▼               ▼               ▼             ▼
   ┌─────────┐   ┌─────────┐    ┌─────────┐    ┌──────────┐   ┌─────────┐
   │  META   │   │   LLM   │    │  QUERY  │    │ REPORTS  │   │   DB    │
   │ (8084)  │   │ (8082)  │    │ (8081)  │    │ (8083)   │   │(Postgre │
   └─────────┘   └─────────┘    └─────────┘    └──────────┘   │  SQL)   │
        │             │               │               │        └─────────┘
        │             │               │               │
        └─────────────┴───────────────┴───────────────┘
```

| Сервис | Порт | Описание |
|--------|------|----------|
| **gateway** | 8080 | Единая точка входа. Раздаёт веб-интерфейс и проксирует API |
| **meta** | 8084 | Semantic layer: метрики, измерения, бизнес-термины, шаблоны вопросов |
| **llm** | 8082 | Интерпретация русского текста в структурированный intent JSON |
| **query** | 8081 | Валидация intent, построение SQL, выполнение запросов, explainability |
| **reports** | 8083 | Сохранение отчётов, история запусков, повторное выполнение |

### Поток данных

```
Пользователь → Gateway → Query Service
                              ↓
                    ┌────────┴────────┐
                    ▼                 ▼
              Meta Service      LLM Service
                    │                 │
                    └────────┬────────┘
                             ▼
                    Intent JSON (структура)
                             ↓
                    SQL Builder + Validator
                             ↓
                    PostgreSQL (read-only)
                             ↓
                    Результат + Chart metadata
                             ↓
                    Gateway → Frontend
```

---

## Структура проекта

```
.
├── cmd/                          # Точки входа для каждого микросервиса
│   ├── gateway/                  # API Gateway + статический frontend
│   │   └── main.go
│   ├── llm/                      # LLM Service (GigaChat/YandexGPT/rule-based)
│   │   └── main.go
│   ├── meta/                     # Meta Service (semantic layer)
│   │   └── main.go
│   ├── query/                    # Query Service (SQL builder + executor)
│   │   └── main.go
│   └── reports/                  # Reports Service (сохранение отчётов)
│       └── main.go
│
├── internal/shared/              # Общие компоненты
│   ├── contracts.go              # Структуры данных: Intent, QueryRequest, etc.
│   ├── http.go                   # HTTP-хелперы
│   └── pg.go                     # PostgreSQL подключение
│
├── db/                           # База данных
│   ├── schema.sql                # Схема БД: таблицы, view, индексы, роли
│   └── seed.sql                  # Демо-данные
│
├── web/                          # Frontend (статические файлы)
│   ├── index.html                # Главная страница
│   ├── reports.html              # Страница отчётов
│   ├── app.js                    # Логика главной страницы
│   ├── reports.js                # Логика отчётов
│   └── styles.css                # Стили
│
├── docs/                         # Документация
│   ├── architecture.md           # Архитектурное описание
│   └── implementation-plan.md    # План реализации
│
├── scripts/                      # Утилиты
│   └── run-local.ps1             # PowerShell скрипт для запуска всех сервисов
│
├── docker-compose.yml            # Docker Compose для PostgreSQL
├── .env.example                  # Пример переменных окружения
├── go.mod                        # Go модули
└── README.md                     # Этот файл
```

---

## Технологический стек

| Компонент | Технология |
|-----------|------------|
| Backend | Go 1.25 |
| База данных | PostgreSQL 16 |
| Драйвер БД | pgx/v5 |
| LLM провайдеры | GigaChat (Sber), YandexGPT |
| Frontend | Vanilla JS, HTML5, CSS3 |
| Контейнеризация | Docker, Docker Compose |

---

## Быстрый старт

### Предварительные требования

- [Go 1.25+](https://go.dev/dl/)
- [PostgreSQL 16](https://www.postgresql.org/download/) (или Docker)
- PowerShell (для Windows) или терминал (для Linux/Mac)
- (Опционально) API ключи GigaChat или YandexGPT

### Способ 1: С Docker (рекомендуется)

```powershell
# 1. Клонируйте репозиторий
git clone https://github.com/ykysbomja5/hakaton-final.git
cd hakaton-final

# 2. Запустите PostgreSQL в Docker
docker-compose up -d

# 3. Дождитесь инициализации БД (схема и данные применятся автоматически)
Start-Sleep -Seconds 10

# 4. Скопируйте и настройте переменные окружения
copy .env.example .env
# Отредактируйте .env при необходимости

# 5. Запустите все сервисы
powershell -ExecutionPolicy Bypass -File .\scripts\run-local.ps1

# 6. Откройте http://localhost:8080
```

### Способ 2: Без Docker (локальная PostgreSQL)

```powershell
# 1. Создайте базу данных
createdb drivee_analytics

# 2. Примените схему
psql -d drivee_analytics -f db/schema.sql

# 3. Загрузите демо-данные
psql -d drivee_analytics -f db/seed.sql

# 4. Настройте переменные окружения
$env:PG_DSN = "postgres://postgres:postgres@localhost:5432/drivee_analytics?sslmode=disable"
$env:GATEWAY_PORT = "8080"
$env:QUERY_PORT = "8081"
$env:LLM_PORT = "8082"
$env:REPORTS_PORT = "8083"
$env:META_PORT = "8084"

# 5. Запустите все сервисы
powershell -ExecutionPolicy Bypass -File .\scripts\run-local.ps1

# 6. Откройте http://localhost:8080
```

### Ручной запуск (для разработки)

```powershell
# В отдельных терминалах:

go run ./cmd/meta      # Порт 8084
go run ./cmd/llm       # Порт 8082
go run ./cmd/query     # Порт 8081
go run ./cmd/reports   # Порт 8083
go run ./cmd/gateway   # Порт 8080
```

---

## Конфигурация

### Переменные окружения

Создайте файл `.env` на основе `.env.example`:

```env
# Подключение к БД
PG_DSN=postgres://postgres:postgres@localhost:5432/drivee_analytics?sslmode=disable

# Порты сервисов
GATEWAY_PORT=8080
QUERY_PORT=8081
LLM_PORT=8082
REPORTS_PORT=8083
META_PORT=8084

# URL внутренних сервисов
QUERY_SERVICE_URL=http://localhost:8081
LLM_SERVICE_URL=http://localhost:8082
REPORTS_SERVICE_URL=http://localhost:8083
META_SERVICE_URL=http://localhost:8084

# Настройки LLM
LLM_PROVIDER=gigachat        # gigachat | yandexgpt | rule-based
LLM_FALLBACK=rule-based      # fallback при ошибке основного провайдера

# GigaChat (получить: https://developers.sber.ru/)
GIGACHAT_AUTH_KEY=
GIGACHAT_SCOPE=GIGACHAT_API_PERS
GIGACHAT_MODEL=GigaChat-2-Max
GIGACHAT_AUTH_URL=https://ngw.devices.sberbank.ru:9443/api/v2/oauth
GIGACHAT_CHAT_URL=https://gigachat.devices.sberbank.ru/api/v1/chat/completions

# YandexGPT (получить: https://yandex.cloud/ru/docs/foundation-models/)
YANDEX_API_KEY=
YANDEX_IAM_TOKEN=
YANDEX_FOLDER_ID=
YANDEX_MODEL_URI=
YANDEX_COMPLETION_URL=https://llm.api.cloud.yandex.net/foundationModels/v1/completion
```

### Режимы работы LLM

| Режим | Описание | Требования |
|-------|----------|------------|
| `rule-based` | Локальный парсер без внешних API | Не требует ключей |
| `gigachat` | Sber GigaChat API | Требует `GIGACHAT_AUTH_KEY` |
| `yandexgpt` | YandexGPT API | Требует `YANDEX_API_KEY` или `YANDEX_IAM_TOKEN` |

Если ключи не заданы, система автоматически переключается на `rule-based` режим.

---

## Использование

### Демо-сценарии

Откройте http://localhost:8080 и попробуйте эти запросы:

1. **Выручка по городам**
   ```
   Покажи выручку по городам за последние 30 дней
   ```

2. **Отмены по тарифам**
   ```
   Сколько было отмен по тарифам на прошлой неделе
   ```

3. **Средний чек по дням**
   ```
   Покажи средний чек по дням за последние 14 дней
   ```

4. **Каналы в Москве**
   ```
   Какие каналы дают больше всего поездок в Москве за месяц
   ```

### Функционал интерфейса

- **Ввод запроса**: Текстовое поле с подсказками
- **Explainability**: Интерпретация запроса, confidence score
- **SQL Preview**: Показывается сгенерированный SQL
- **Результат**: Таблица с данными
- **Визуализация**: Автоматический график (линейный/столбчатый)
- **Сохранение**: Кнопка "Сохранить отчёт"
- **История**: Переход на страницу отчётов для повторного запуска

---

## Доступные метрики и измерения

### Метрики

| ID | Название | Описание | Формат |
|----|----------|----------|--------|
| `completed_rides` | Завершенные поездки | Количество завершенных поездок | Целое число |
| `total_rides` | Все поездки | Сумма завершенных и отмененных | Целое число |
| `cancellations` | Отмены | Количество отмененных поездок | Целое число |
| `revenue` | Выручка | Суммарная выручка в рублях | Валюта |
| `avg_fare` | Средний чек | Средняя стоимость поездки | Валюта |
| `active_drivers` | Активные водители | Количество активных водителей | Целое число |

### Измерения (группировка)

| ID | Название | Описание |
|----|----------|----------|
| `day` | День | Дневная гранулярность |
| `week` | Неделя | Недельная гранулярность |
| `month` | Месяц | Месячная гранулярность |
| `city` | Город | Москва, Санкт-Петербург, Казань, Екатеринбург, Новосибирск |
| `service_class` | Тариф | Эконом, Комфорт, Бизнес |
| `source_channel` | Канал | Приложение, Сайт, Партнеры |
| `driver_segment` | Сегмент водителя | Новые, Стабильные, Премиум |

---

## Безопасность (Guardrails)

1. **LLM не имеет доступа к БД** — только формирует структуру intent
2. **SQL строится только backend-ом** — детерминированная логика на Go
3. **Только SELECT** — любые другие операции блокируются
4. **Allowlist** — только разрешённые метрики и измерения из semantic layer
5. **Read-only роль** — отдельный пользователь `analytics_readonly` в БД
6. **Журналирование** — все запросы логируются в `app.query_logs`
7. **Confidence score** — низкая уверенность требует уточнения

---

## API Endpoints

### Gateway (8080)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/` | Главная страница |
| GET | `/reports` | Страница отчётов |
| POST | `/api/query/parse` | Распознать намерение |
| POST | `/api/query/run` | Выполнить запрос |
| POST | `/api/reports` | Сохранить отчёт |
| GET | `/api/reports` | Список отчётов |
| POST | `/api/reports/{id}/run` | Перезапустить отчёт |
| GET | `/api/meta/semantic-layer` | Получить semantic layer |

### Внутренние сервисы

**Meta Service (8084)**
- `GET /semantic-layer` — метрики, измерения, термины

**LLM Service (8082)**
- `POST /parse` — текст → intent JSON

**Query Service (8081)**
- `POST /parse` — текст → intent + preview
- `POST /run` — выполнить запрос → результат + график

**Reports Service (8083)**
- `GET /reports` — список отчётов
- `POST /reports` — сохранить отчёт
- `POST /reports/{id}/run` — перезапустить

---

## Разработка

### Структура данных

**Intent** — структурированное намерение пользователя:
```go
type Intent struct {
    Metric        string    // Выбранная метрика (revenue, completed_rides, ...)
    GroupBy       string    // Группировка (city, day, service_class, ...)
    Filters       []Filter  // Фильтры
    Period        TimeRange // Временной диапазон
    Sort          string    // Сортировка
    Limit         int       // Лимит строк
    Clarification string    // Уточнение при неоднозначности
    Assumptions   []string  // Сделанные предположения
    Confidence    float64   // Уверенность (0.0 - 1.0)
}
```

### Добавление новой метрики

1. Добавьте в `internal/shared/contracts.go`:
```go
{ID: "new_metric", Title: "Новая метрика", Description: "...", Format: "integer"}
```

2. Добавьте бизнес-термин:
```go
{Term: "новый термин", Kind: "metric", Canonical: "new_metric", Description: "..."}
```

3. Обновите view `analytics.v_ride_metrics` в `db/schema.sql`

---

## Тестирование

```powershell
# Проверка работоспособности сервисов
curl http://localhost:8080
curl http://localhost:8084/semantic-layer

# Тест parse endpoint
curl -X POST http://localhost:8081/parse `
  -H "Content-Type: application/json" `
  -d '{"text": "выручка по городам"}'

# Тест run endpoint
curl -X POST http://localhost:8081/run `
  -H "Content-Type: application/json" `
  -d '{"text": "покажи выручку по городам за последние 7 дней"}'
```

---

## Частые вопросы

**Q: Почему LLM не генерирует SQL напрямую?**  
A: Для безопасности и контроля. LLM только интерпретирует намерение, а SQL строится детерминированным кодом с валидацией.

**Q: Как защититься от инъекций?**  
A: Несколько уровней: структурированный intent, allowlist колонок, prepared statements, read-only роль БД.

**Q: Можно ли подключить свою схему данных?**  
A: Да, замените view `analytics.v_ride_metrics` на своё представление или materialized view.

**Q: Как масштабировать?**  
A: Каждый сервис независим — можно масштабировать `query` и `llm` отдельно, добавить кэширование, очереди.

---

## Лицензия

MIT License — свободное использование для образовательных и коммерческих целей.

---

## Контакты

Проект разработан для хакатона Drivee Analytics Challenge.

Репозиторий: https://github.com/ykysbomja5/hakaton-final

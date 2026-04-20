# Drivee Analytics MVP

Микросервисный прототип для кейса Drivee: self-service аналитика на русском языке с explainability, безопасным NL→SQL и сохранением отчётов.

## Что внутри

- `gateway` раздаёт статический frontend и проксирует API.
- `query` строит безопасный SQL из намерения пользователя и выполняет запросы в PostgreSQL.
- `llm` интерпретирует запрос на русском языке. Поддерживает `GigaChat`, `YandexGPT` и локальный fallback `rule-based`.
- `reports` сохраняет отчёты и умеет запускать их повторно.
- `meta` отдаёт semantic layer: метрики, измерения, словарь и шаблоны вопросов.

## Почему решение выглядит сильно на защите

- Проходит полный сценарий из тизера: текстовый запрос → SQL → данные → график → сохранённый отчёт.
- Делает ставку на ваши сильные стороны: backend и микросервисы на Go.
- Использует русскоязычную LLM только как интерпретатор намерения, а не как прямой генератор SQL.
- Явно показывает guardrails: только `SELECT`, allowlist метрик/измерений, read-only роль БД, журналирование и explainability.

## Структура

```text
cmd/
  gateway/
  llm/
  meta/
  query/
  reports/
db/
  schema.sql
  seed.sql
docs/
  architecture.md
  implementation-plan.md
internal/shared/
scripts/
web/
```

## Быстрый старт

1. Создайте базу PostgreSQL `drivee_analytics`.
2. Выполните `db/schema.sql`, затем `db/seed.sql`.
3. Скопируйте `.env.example` в `.env` или экспортируйте переменные окружения вручную.
4. Запустите `powershell -ExecutionPolicy Bypass -File .\scripts\run-local.ps1`.
5. Откройте `http://localhost:8080`.

## Переменные для LLM

### GigaChat

- `LLM_PROVIDER=gigachat`
- `GIGACHAT_AUTH_KEY` или пара `GIGACHAT_CLIENT_ID` + `GIGACHAT_CLIENT_SECRET`

### YandexGPT / Алиса-стек

- `LLM_PROVIDER=yandexgpt`
- `YANDEX_API_KEY` или `YANDEX_IAM_TOKEN`
- `YANDEX_FOLDER_ID` или полный `YANDEX_MODEL_URI`

Если внешние ключи не заданы, `llm` автоматически переходит в локальный `rule-based` режим. Это полезно для офлайн-демо и разработки UI/guardrails.

## Демо-сценарии

- `Покажи выручку по городам за последние 30 дней`
- `Сколько было отмен по тарифам на прошлой неделе`
- `Покажи средний чек по дням за последние 14 дней`
- `Какие каналы дают больше всего поездок в Москве за месяц`

## Что можно показать экспертам

- Архитектурное разделение ответственности по микросервисам.
- Безопасный pipeline: LLM → intent JSON → SQL builder → validator → read-only executor.
- Semantic layer и словарь бизнес-терминов как бонусный критерий.
- Confidence score и сценарий с уточнением запроса.
- Готовый контур для расширения: расписание рассылки, materialized views, RBAC, кэширование.

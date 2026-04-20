<div align="center">

<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,11,20&height=200&section=header&text=Drivee%20Analytics&fontSize=60&fontColor=fff&animation=twinkling&fontAlignY=35&desc=Self-Service%20NL%E2%86%92SQL%20Platform&descAlignY=55&descSize=20"/>

<br>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.25-00ADD8?style=for-the-badge&logo=go&logoColor=white&labelColor=000" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white&labelColor=000" />
  <img src="https://img.shields.io/badge/Microservices-Architecture-FF6B6B?style=for-the-badge&logo=docker&logoColor=white&labelColor=000" />
  <img src="https://img.shields.io/badge/GigaChat-AI-00A651?style=for-the-badge&logo=openai&logoColor=white&labelColor=000" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/YandexGPT-Alternative-FFCC00?style=for-the-badge&logo=yandex&logoColor=black&labelColor=000" />
  <img src="https://img.shields.io/badge/license-MIT-00C853?style=for-the-badge&logo=opensourceinitiative&logoColor=white&labelColor=000" />
  <img src="https://img.shields.io/badge/Made%20with-❤️-ff69b4?style=for-the-badge&labelColor=000" />
</p>

<br>

**🚀 Превращайте русский текст в аналитику за секунды**

*Безопасный NL→SQL pipeline с explainability, guardrails и визуализацией*

[🌟 Демо](#-демо) • [🚀 Быстрый старт](#-быстрый-старт) • [📖 Документация](#-архитектура) • [🛡️ Безопасность](#️-безопасность)

</div>

---

<br>

## ✨ Что это?

<div align="center">

```
┌─────────────────────────────────────────────────────────────────┐
│  "Покажи выручку по городам за последние 30 дней"              │
│                        ⬇️                                        │
│              🧠 LLM (GigaChat/YandexGPT)                        │
│                        ⬇️                                        │
│              📊 Intent JSON (структура)                         │
│                        ⬇️                                        │
│              🔒 SQL Builder (Go)                                │
│                        ⬇️                                        │
│              📈 Результат + График                              │
└─────────────────────────────────────────────────────────────────┘
```

</div>

<br>

**Drivee Analytics** — это микросервисная платформа self-service аналитики, которая позволяет бизнес-пользователям:

| 💬 | Задавать вопросы на русском языке |
|----|-----------------------------------|
| 🔒 | Получать безопасные SQL-запросы |
| 📊 | Видеть результаты в таблицах и графиках |
| 💾 | Сохранять отчёты для повторного использования |
| 🧠 | Понимать логику через explainability |

<br>

---

<br>

## 🎥 Демо

<div align="center">

### 🖼️ Интерфейс платформы

<p align="center">
  <kbd>
    <img src="https://via.placeholder.com/800x400/1a1a2e/ffffff?text=Drivee+Analytics+Dashboard" width="800" />
  </kbd>
</p>

*💡 Замените на реальный скриншот: `web/screenshot.png`*

</div>

<br>

### 🎯 Примеры запросов

<table>
<tr>
<td width="50%">

**💬 Ввод пользователя:**
```text
Покажи выручку по городам 
за последние 30 дней
```

</td>
<td width="50%">

**🧠 Интерпретация:**
```json
{
  "metric": "revenue",
  "group_by": "city",
  "period": "last_30_days",
  "confidence": 0.95
}
```

</td>
</tr>
<tr>
<td colspan="2" align="center">

**📊 Результат:**

| Город | Выручка | График |
|-------|---------|--------|
| Москва | ₽2.4M | 📈 |
| СПб | ₽1.8M | 📈 |
| Казань | ₽890K | 📈 |

</td>
</tr>
</table>

<br>

---

<br>

## 🏗️ Архитектура

<div align="center">

```mermaid
flowchart TB
    subgraph "🌐 Frontend"
        U[👤 Пользователь]
        F[📱 Web UI]
    end

    subgraph "🚪 Gateway"
        G[🔀 API Gateway<br/>Port: 8080]
    end

    subgraph "⚙️ Microservices"
        direction TB
        M[📚 Meta Service<br/>Port: 8084]
        L[🧠 LLM Service<br/>Port: 8082]
        Q[⚡ Query Service<br/>Port: 8081]
        R[💾 Reports Service<br/>Port: 8083]
    end

    subgraph "🗄️ Data Layer"
        DB[(🐘 PostgreSQL 16)]
    end

    U --> F
    F --> G
    G --> M
    G --> L
    G --> Q
    G --> R
    Q --> M
    Q --> L
    Q --> DB
    R --> DB
```

</div>

<br>

### 📋 Микросервисы

<table>
<tr>
<td width="20%" align="center">

### 🚪 Gateway
`8080`

</td>
<td>

Единая точка входа. Раздаёт статический frontend и проксирует API-запросы к внутренним сервисам.

</td>
</tr>
<tr>
<td align="center">

### 📚 Meta
`8084`

</td>
<td>

Semantic layer: метрики, измерения, бизнес-термины, шаблоны вопросов.

</td>
</tr>
<tr>
<td align="center">

### 🧠 LLM
`8082`

</td>
<td>

Интерпретация русского текста в структурированный intent JSON. Поддержка GigaChat, YandexGPT и rule-based fallback.

</td>
</tr>
<tr>
<td align="center">

### ⚡ Query
`8081`

</td>
<td>

Валидация intent, построение SQL, выполнение запросов, explainability, генерация графиков.

</td>
</tr>
<tr>
<td align="center">

### 💾 Reports
`8083`

</td>
<td>

Сохранение отчётов, история запусков, повторное выполнение.

</td>
</tr>
</table>

<br>

---

<br>

## 🚀 Быстрый старт

<div align="center">

### ⚡ Запуск за 5 минут

</div>

<br>

<details open>
<summary><b>🐳 Способ 1: С Docker (рекомендуется)</b></summary>
<br>

```bash
# 1. Клонируйте репозиторий
git clone https://github.com/ykysbomja5/hakaton-final.git
cd hakaton-final

# 2. Запустите PostgreSQL
docker-compose up -d

# 3. Дождитесь инициализации (10 сек)
sleep 10

# 4. Запустите все сервисы
powershell -ExecutionPolicy Bypass -File .\scripts\run-local.ps1

# 5. Откройте http://localhost:8080 🎉
```

</details>

<br>

<details>
<summary><b>💻 Способ 2: Без Docker</b></summary>
<br>

```bash
# 1. Создайте БД
createdb drivee_analytics

# 2. Примените схему
psql -d drivee_analytics -f db/schema.sql

# 3. Загрузите данные
psql -d drivee_analytics -f db/seed.sql

# 4. Настройте окружение
copy .env.example .env

# 5. Запустите
powershell -ExecutionPolicy Bypass -File .\scripts\run-local.ps1
```

</details>

<br>

<div align="center">

### 🎯 После запуска

<p align="center">
  <a href="http://localhost:8080">
    <img src="https://img.shields.io/badge/🌐_Открыть_приложение-http://localhost:8080-00ADD8?style=for-the-badge&labelColor=000" />
  </a>
</p>

</div>

<br>

---

<br>

## 🛡️ Безопасность

<div align="center">

```
┌─────────────────────────────────────────────────────────────────┐
│                    🔒 MULTI-LAYER SECURITY                      │
├─────────────────────────────────────────────────────────────────┤
│  1️⃣  LLM → Intent JSON (никакого SQL)                          │
│  2️⃣  SQL Builder → Только allowlist колонки                    │
│  3️⃣  Validator → Только SELECT                                 │
│  4️⃣  PostgreSQL → Read-only роль                               │
│  5️⃣  Logging → Полный аудит в query_logs                       │
└─────────────────────────────────────────────────────────────────┘
```

</div>

<br>

<table>
<tr>
<td width="50%">

### ✅ Что разрешено

- ✅ Только `SELECT` запросы
- ✅ Разрешённые метрики из white-list
- ✅ Параметризованные запросы
- ✅ Read-only соединение с БД

</td>
<td width="50%">

### ❌ Что заблокировано

- ❌ `DROP`, `DELETE`, `UPDATE`, `INSERT`
- ❌ Прямой доступ LLM к БД
- ❌ Неразрешённые таблицы/колонки
- ❌ Подозрительные паттерны

</td>
</tr>
</table>

<br>

---

<br>

## 📊 Доступные метрики

<div align="center">

<table>
<tr>
<th>📈 Метрика</th>
<th>Описание</th>
<th>Пример запроса</th>
</tr>
<tr>
<td><code>revenue</code></td>
<td>💰 Выручка</td>
<td><i>"Выручка по городам"</i></td>
</tr>
<tr>
<td><code>completed_rides</code></td>
<td>🚗 Завершённые поездки</td>
<td><i>"Поездки по тарифам"</i></td>
</tr>
<tr>
<td><code>cancellations</code></td>
<td>❌ Отмены</td>
<td><i>"Отмены за неделю"</i></td>
</tr>
<tr>
<td><code>avg_fare</code></td>
<td>💳 Средний чек</td>
<td><i>"Средний чек по дням"</i></td>
</tr>
<tr>
<td><code>active_drivers</code></td>
<td>👨‍✈️ Активные водители</td>
<td><i>"Водители по сегментам"</i></td>
</tr>
</table>

</div>

<br>

---

<br>

## 🔧 Технологический стек

<div align="center">

<p align="center">
  <img src="https://img.shields.io/badge/Go-00ADD8?style=flat-square&logo=go&logoColor=white" height="30" />
  &nbsp;
  <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white" height="30" />
  &nbsp;
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white" height="30" />
  &nbsp;
  <img src="https://img.shields.io/badge/JavaScript-F7DF1E?style=flat-square&logo=javascript&logoColor=black" height="30" />
</p>

<br>

| Компонент | Технология | Версия |
|-----------|------------|--------|
| Backend | Go | 1.25 |
| Database | PostgreSQL | 16 |
| Driver | pgx | v5 |
| LLM | GigaChat / YandexGPT | - |
| Frontend | Vanilla JS | ES6+ |
| Container | Docker | Latest |

</div>

<br>

---

<br>

## 📁 Структура проекта

```
📦 drivee-analytics
├── 📂 cmd/                    # Микросервисы
│   ├── 🚪 gateway/            # API Gateway (8080)
│   ├── 🧠 llm/                # LLM Service (8082)
│   ├── ⚡ query/               # Query Service (8081)
│   ├── 💾 reports/             # Reports Service (8083)
│   └── 📚 meta/                # Meta Service (8084)
│
├── 📂 internal/shared/        # Общий код
│   ├── 📄 contracts.go        # Структуры данных
│   ├── 📄 http.go             # HTTP helpers
│   └── 📄 pg.go               # PostgreSQL
│
├── 📂 web/                    # Frontend
│   ├── 📄 index.html
│   ├── 📄 app.js
│   └── 📄 styles.css
│
├── 📂 db/                     # База данных
│   ├── 📄 schema.sql          # Схема
│   └── 📄 seed.sql            # Данные
│
└── 📄 docker-compose.yml      # Docker конфиг
```

<br>

---

<br>

## 🌟 Roadmap

<div align="center">

| Статус | Фича |
|--------|------|
| ✅ | Базовый NL→SQL |
| ✅ | GigaChat интеграция |
| ✅ | Визуализация |
| ✅ | Сохранение отчётов |
| 🚧 | RBAC авторизация |
| 🚧 | Кэширование |
| 📋 | Slack/Email рассылки |
| 📋 | Materialized views |

</div>

<br>

---

<br>

## 🤝 Contributing

<div align="center">

**Приветствуем PR и Issues!**

<p align="center">
  <a href="https://github.com/ykysbomja5/hakaton-final/issues">
    <img src="https://img.shields.io/badge/🐛_Сообщить_об_ошибке-FF6B6B?style=for-the-badge&labelColor=000" />
  </a>
  &nbsp;
  <a href="https://github.com/ykysbomja5/hakaton-final/pulls">
    <img src="https://img.shields.io/badge/🚀_Сделать_PR-00C853?style=for-the-badge&labelColor=000" />
  </a>
</p>

</div>

<br>

---

<br>

<div align="center">

### 📜 Лицензия

MIT License © 2024 Drivee Analytics Team

<br>

<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,11,20&height=100&section=footer"/>
</p>

**⭐ Если проект полезен — поставьте звезду!**

</div>

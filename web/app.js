const state = {
  parse: null,
  run: null,
  reports: [],
  semantic: null,
  provider: "rule-based",
};

const elements = {
  queryInput: document.getElementById("query-input"),
  parseButton: document.getElementById("parse-button"),
  runButton: document.getElementById("run-button"),
  saveButton: document.getElementById("save-button"),
  responseBanner: document.getElementById("response-banner"),
  providerPill: document.getElementById("provider-pill"),
  intentSummary: document.getElementById("intent-summary"),
  intentMetric: document.getElementById("intent-metric"),
  intentGroup: document.getElementById("intent-group"),
  intentPeriod: document.getElementById("intent-period"),
  intentConfidence: document.getElementById("intent-confidence"),
  intentFilters: document.getElementById("intent-filters"),
  clarificationBox: document.getElementById("clarification-box"),
  sqlPreview: document.getElementById("sql-preview"),
  chartSurface: document.getElementById("chart-surface"),
  chartInsights: document.getElementById("chart-insights"),
  businessSummary: document.getElementById("business-summary"),
  summaryStats: document.getElementById("summary-stats"),
  resultTableHead: document.querySelector("#result-table thead"),
  resultTableBody: document.querySelector("#result-table tbody"),
  resultCount: document.getElementById("result-count"),
  reportList: document.getElementById("report-list"),
  samplePrompts: document.getElementById("sample-prompts"),
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || "Ошибка запроса");
  }
  return data;
}

function setBanner(message) {
  elements.responseBanner.textContent = message;
}

function formatConfidence(intent) {
  if (!intent) return "—";
  const percent = `${Math.round((intent.confidence || 0) * 100)}%`;
  return `${percent} • ${state.parse?.preview?.confidence_label || "—"}`;
}

function formatNumber(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return String(value ?? "");
  }
  return new Intl.NumberFormat("ru-RU", {
    maximumFractionDigits: 2,
  }).format(numeric);
}

function parseNumeric(value) {
  if (typeof value === "number") {
    return value;
  }

  const normalized = String(value ?? "")
    .replace(/\s/g, "")
    .replace(",", ".");
  const numeric = Number(normalized);
  return Number.isFinite(numeric) ? numeric : 0;
}

function inferProvider(run) {
  if (run?.provider) {
    return run.provider;
  }
  return state.provider;
}

function metricTitle() {
  return state.parse?.preview?.metric_label || "Показатель";
}

function groupTitle() {
  return state.parse?.preview?.group_by_label || "Без разбивки";
}

function periodTitle() {
  return state.parse?.intent?.period?.label || "выбранный период";
}

function buildReportName() {
  const metric = metricTitle();
  const period = periodTitle();
  const group = state.parse?.preview?.group_by_label;
  return group ? `${metric} по ${group.toLowerCase()} • ${period}` : `${metric} • ${period}`;
}

function createChip(label, value) {
  return `<span class="summary-chip"><strong>${label}:</strong> ${value}</span>`;
}

function createInsight(label, value) {
  return `<span class="insight-chip"><strong>${label}:</strong> ${value}</span>`;
}

function displayColumnName(column) {
  switch (column) {
    case "group_value":
      return groupTitle();
    case "period_value":
      return "Период";
    case "metric_value":
      return metricTitle();
    default:
      return column;
  }
}

function renderParse() {
  const parse = state.parse;
  if (!parse) {
    return;
  }

  elements.intentSummary.textContent = parse.preview.summary || "Не удалось интерпретировать запрос.";
  elements.intentMetric.textContent = parse.preview.metric_label || "—";
  elements.intentGroup.textContent = parse.preview.group_by_label || "Без разбивки";
  elements.intentPeriod.textContent = parse.intent.period?.label || "—";
  elements.intentConfidence.textContent = formatConfidence(parse.intent);
  elements.providerPill.textContent = `LLM: ${state.provider} • ${Math.round((parse.intent.confidence || 0) * 100)}%`;

  elements.intentFilters.innerHTML = "";
  const chips = parse.preview.applied_filters || [];
  if (chips.length === 0) {
    const span = document.createElement("span");
    span.textContent = "Дополнительные фильтры не заданы";
    elements.intentFilters.appendChild(span);
  } else {
    chips.forEach((item) => {
      const span = document.createElement("span");
      span.textContent = item;
      elements.intentFilters.appendChild(span);
    });
  }

  const messages = [];
  if (parse.preview.clarification) messages.push(parse.preview.clarification);
  (parse.preview.assumptions || []).forEach((item) => messages.push(item));
  elements.clarificationBox.textContent = messages.length
    ? messages.join(" ")
    : "Система готова выполнить только безопасный и разрешённый вариант запроса.";
}

function renderSummary(run) {
  if (!run?.result?.rows?.length) {
    elements.businessSummary.innerHTML = `
      <div class="summary-hero">
        <span class="summary-label">Главный вывод</span>
        <strong>По запросу нет данных</strong>
        <p>Попробуйте изменить формулировку, период или убрать часть фильтров.</p>
      </div>
    `;
    return;
  }

  const metric = metricTitle();
  const period = periodTitle();

  if (run.result.columns.length < 2) {
    const value = run.result.rows[0][0];
    elements.businessSummary.innerHTML = `
      <div class="summary-hero">
        <span class="summary-label">Главный вывод</span>
        <strong>${metric}: ${formatNumber(value)}</strong>
        <p>Система посчитала итоговое значение за период «${period}» без дополнительной разбивки.</p>
      </div>
      <div class="summary-stats">
        ${createChip("Период", period)}
        ${createChip("Тип ответа", "Итоговый KPI")}
        ${createChip("Строк в результате", run.result.count)}
      </div>
    `;
    return;
  }

  const rows = run.result.rows.map((row) => ({
    label: row[0],
    value: parseNumeric(row[1]),
  }));
  const top = rows.reduce((best, current) => (current.value > best.value ? current : best), rows[0]);
  const total = rows.reduce((sum, row) => sum + row.value, 0);

  elements.businessSummary.innerHTML = `
    <div class="summary-hero">
      <span class="summary-label">Главный вывод</span>
      <strong>Лидер: ${top.label}</strong>
      <p>По запросу «${metric}» за период «${period}» лучший результат показывает ${top.label}: ${formatNumber(top.value)}.</p>
    </div>
    <div class="summary-stats">
      ${createChip("Лидер", `${top.label} • ${formatNumber(top.value)}`)}
      ${createChip("Всего значений", formatNumber(total))}
      ${createChip("Разбивка", groupTitle())}
      ${createChip("Строк в результате", run.result.count)}
    </div>
  `;
}

function renderTable(result) {
  elements.resultTableHead.innerHTML = "";
  elements.resultTableBody.innerHTML = "";

  if (!result || !result.columns || result.columns.length === 0) {
    elements.resultCount.textContent = "0 строк";
    elements.resultTableBody.innerHTML = `<tr><td class="empty-state">Нет данных для отображения.</td></tr>`;
    return;
  }

  elements.resultCount.textContent = `${result.count} строк`;
  const headerRow = document.createElement("tr");
  result.columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = displayColumnName(column);
    headerRow.appendChild(th);
  });
  elements.resultTableHead.appendChild(headerRow);

  result.rows.forEach((row) => {
    const tr = document.createElement("tr");
    row.forEach((value, index) => {
      const td = document.createElement("td");
      td.textContent = index > 0 || row.length === 1 ? formatNumber(value) : value;
      tr.appendChild(td);
    });
    elements.resultTableBody.appendChild(tr);
  });
}

function renderChartInsights(run) {
  if (!run?.result?.rows?.length) {
    elements.chartInsights.innerHTML = "";
    return;
  }

  if (run.result.columns.length < 2) {
    elements.chartInsights.innerHTML = [
      createInsight("Формат", "KPI"),
      createInsight("Период", periodTitle()),
      createInsight("Значение", formatNumber(run.result.rows[0][0])),
    ].join("");
    return;
  }

  const rows = run.result.rows.map((row) => ({
    label: row[0],
    value: parseNumeric(row[1]),
  }));
  const top = rows.reduce((best, current) => (current.value > best.value ? current : best), rows[0]);
  const bottom = rows.reduce((best, current) => (current.value < best.value ? current : best), rows[0]);
  const average = rows.reduce((sum, row) => sum + row.value, 0) / rows.length;

  elements.chartInsights.innerHTML = [
    createInsight("Лидер", `${top.label} • ${formatNumber(top.value)}`),
    createInsight("Минимум", `${bottom.label} • ${formatNumber(bottom.value)}`),
    createInsight("Среднее", formatNumber(average)),
  ].join("");
}

function renderKpiCard(run) {
  const value = run?.result?.rows?.[0]?.[0] ?? "—";
  const title = run?.preview?.metric_label || "Итоговый показатель";
  elements.chartSurface.innerHTML = `
    <div class="kpi-card">
      <span>KPI</span>
      <strong>${formatNumber(value)}</strong>
      <p>${title} за период «${periodTitle()}». Для такого вопроса дополнительный график не нужен.</p>
    </div>
  `;
}

function renderLineChart(labels, values, width, height, padding) {
  const max = Math.max(...values, 1);
  const step = values.length > 1 ? (width - padding * 2) / (values.length - 1) : 0;
  const points = values.map((value, index) => {
    const x = padding + index * step;
    const y = height - padding - (value / max) * (height - padding * 2);
    return { x, y, label: labels[index], value };
  });
  const path = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
  const dots = points
    .map(
      (point) => `
        <circle class="line-dot" cx="${point.x}" cy="${point.y}" r="5"></circle>
        <text class="axis-label" x="${point.x}" y="${height - 8}" text-anchor="middle">${point.label}</text>
      `
    )
    .join("");

  return `
    <path class="line-path" d="${path}"></path>
    ${dots}
  `;
}

function renderBarChart(labels, values, width, height, padding) {
  const max = Math.max(...values, 1);
  const gap = 18;
  const barWidth = Math.max((width - padding * 2 - gap * (values.length - 1)) / values.length, 18);

  return values
    .map((value, index) => {
      const x = padding + index * (barWidth + gap);
      const barHeight = (value / max) * (height - padding * 2);
      const y = height - padding - barHeight;
      return `
        <rect class="bar" x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="8"></rect>
        <text class="value-label" x="${x + barWidth / 2}" y="${Math.max(y - 8, 14)}" text-anchor="middle">${formatNumber(value)}</text>
        <text class="axis-label" x="${x + barWidth / 2}" y="${height - 8}" text-anchor="middle">${labels[index]}</text>
      `;
    })
    .join("");
}

function renderChart(run) {
  if (!run || !run.result || !run.result.rows || run.result.rows.length === 0) {
    elements.chartSurface.innerHTML = `<div class="chart-placeholder">После выполнения запроса здесь появится график или KPI-карточка.</div>`;
    elements.chartInsights.innerHTML = "";
    return;
  }

  renderChartInsights(run);

  if (run.result.columns.length < 2) {
    renderKpiCard(run);
    return;
  }

  const labels = run.result.rows.map((row) => row[0]);
  const values = run.result.rows.map((row) => parseNumeric(row[1]));
  const width = 640;
  const height = 320;
  const padding = 38;
  const grid = [0.25, 0.5, 0.75]
    .map((ratio) => {
      const y = height - padding - ratio * (height - padding * 2);
      return `<line class="chart-grid-line" x1="${padding}" x2="${width - padding}" y1="${y}" y2="${y}"></line>`;
    })
    .join("");

  const chartBody =
    run.chart?.type === "line"
      ? renderLineChart(labels, values, width, height, padding)
      : renderBarChart(labels, values, width, height, padding);

  elements.chartSurface.innerHTML = `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
      ${grid}
      ${chartBody}
    </svg>
  `;
}

function formatDate(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function renderReports() {
  if (!state.reports.length) {
    elements.reportList.innerHTML = `
      <div class="reports-overview-card">
        <strong>Пока нет сохранённых отчётов</strong>
        <p>После сохранения здесь появится короткая сводка и ссылка на отдельную страницу отчётов.</p>
        <a class="reports-link-button muted" href="/reports.html">Перейти к сохранённым отчётам</a>
      </div>
    `;
    return;
  }

  const latest = state.reports[0];
  elements.reportList.innerHTML = `
    <div class="reports-overview-card">
      <span class="reports-count">${state.reports.length}</span>
      <div class="reports-overview-copy">
        <strong>Сохранённых отчётов: ${state.reports.length}</strong>
        <p>Последний отчёт: ${latest.name}</p>
        <small>Обновлён: ${formatDate(latest.updated_at)}</small>
      </div>
      <a class="reports-link-button" href="/reports.html">Перейти к сохранённым отчётам</a>
    </div>
  `;
}

async function refreshReports() {
  state.reports = await api("/api/v1/reports").catch(() => []);
  renderReports();
}

async function openReportById(reportId) {
  try {
    setBanner("Открываю сохранённый отчёт...");
    const run = await api(`/api/v1/reports/${reportId}/run`, { method: "POST" });
    state.run = run;
    state.parse = {
      intent: run.intent,
      preview: run.preview,
    };
    renderParse();
    renderRun();
    const report = state.reports.find((item) => Number(item.id) === Number(reportId));
    setBanner(report ? `Отчёт «${report.name}» успешно выполнен.` : "Отчёт успешно выполнен.");
  } catch (error) {
    setBanner(error.message);
  }
}

function renderRun() {
  if (!state.run) {
    return;
  }
  state.provider = inferProvider(state.run);
  elements.sqlPreview.textContent = state.run.sql || "Система попросила уточнить запрос, SQL не выполнялся.";
  renderSummary(state.run);
  renderTable(state.run.result);
  renderChart(state.run);
}

async function loadInitialData() {
  try {
    state.semantic = await api("/api/v1/meta/schema");

    elements.samplePrompts.innerHTML = "";
    (state.semantic.sample_questions || []).forEach((prompt) => {
      const button = document.createElement("button");
      button.className = "prompt-chip";
      button.textContent = prompt;
      button.addEventListener("click", () => {
        elements.queryInput.value = prompt;
      });
      elements.samplePrompts.appendChild(button);
    });

    await refreshReports();

    const pendingReportId = window.localStorage.getItem("drivee:openReportId");
    if (pendingReportId) {
      window.localStorage.removeItem("drivee:openReportId");
      await openReportById(pendingReportId);
    }
  } catch (error) {
    setBanner(error.message);
  }
}

async function parseQuery() {
  const text = elements.queryInput.value.trim();
  if (!text) {
    setBanner("Введите текстовый запрос.");
    return;
  }
  setBanner("Понимаю запрос и проверяю, что именно нужно показать...");
  try {
    state.parse = await api("/api/v1/query/parse", {
      method: "POST",
      body: JSON.stringify({ text }),
    });
    renderParse();
    setBanner("Запрос понятен. Можно сразу показать результат.");
  } catch (error) {
    setBanner(error.message);
  }
}

async function runQuery() {
  const text = elements.queryInput.value.trim();
  if (!text) {
    setBanner("Введите текстовый запрос.");
    return;
  }
  setBanner("Собираю результат и визуализацию...");
  try {
    state.run = await api("/api/v1/query/run", {
      method: "POST",
      body: JSON.stringify({ text }),
    });
    state.parse = {
      intent: state.run.intent,
      preview: state.run.preview,
    };
    renderParse();
    renderRun();
    const hasRows = state.run.result?.count > 0;
    setBanner(
      state.run.preview?.clarification
        ? state.run.preview.clarification
        : hasRows
          ? `Результат готов. Получено строк: ${state.run.result.count}.`
          : "Запрос выполнен, но результат пустой."
    );
  } catch (error) {
    setBanner(error.message);
  }
}

async function saveReport() {
  const text = elements.queryInput.value.trim();
  if (!state.run || !state.run.sql) {
    setBanner("Сначала покажите результат, затем можно сохранить отчёт.");
    return;
  }

  const name = buildReportName();
  setBanner("Сохраняю отчёт...");

  try {
    await api("/api/v1/reports", {
      method: "POST",
      body: JSON.stringify({
        name,
        query_text: text,
        sql_text: state.run.sql,
        intent: state.run.intent,
      }),
    });
    await refreshReports();
    setBanner(`Отчёт «${name}» сохранён.`);
  } catch (error) {
    setBanner(error.message);
  }
}

elements.parseButton.addEventListener("click", parseQuery);
elements.runButton.addEventListener("click", runQuery);
elements.saveButton.addEventListener("click", saveReport);

loadInitialData();

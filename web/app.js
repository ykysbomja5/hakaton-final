const state = {
  parse: null,
  run: null,
  reports: [],
  templates: [],
  semantic: null,
  provider: "rule-based",
  glossaryKind: "all",
};

const elements = {
  queryInput: document.getElementById("query-input"),
  parseButton: document.getElementById("parse-button"),
  runButton: document.getElementById("run-button"),
  saveButton: document.getElementById("save-button"),
  addTemplateButton: document.getElementById("add-template-button"),
  exportPdfButton: document.getElementById("export-pdf-button"),
  responseBanner: document.getElementById("response-banner"),
  providerPill: document.getElementById("provider-pill"),
  sidebarProvider: document.getElementById("sidebar-provider"),
  intentSummary: document.getElementById("intent-summary"),
  intentMetric: document.getElementById("intent-metric"),
  intentGroup: document.getElementById("intent-group"),
  intentPeriod: document.getElementById("intent-period"),
  intentConfidence: document.getElementById("intent-confidence"),
  intentFilters: document.getElementById("intent-filters"),
  clarificationBox: document.getElementById("clarification-box"),
  intentHighlightStrip: document.getElementById("intent-highlight-strip"),
  sqlPreview: document.getElementById("sql-preview"),
  chartSurface: document.getElementById("chart-surface"),
  chartInsights: document.getElementById("chart-insights"),
  rankingPanel: document.getElementById("ranking-panel"),
  businessSummary: document.getElementById("business-summary"),
  summaryStats: document.getElementById("summary-stats"),
  storyGrid: document.getElementById("story-grid"),
  resultTableHead: document.querySelector("#result-table thead"),
  resultTableBody: document.querySelector("#result-table tbody"),
  resultCount: document.getElementById("result-count"),
  reportList: document.getElementById("report-list"),
  samplePrompts: document.getElementById("sample-prompts"),
  glossarySearch: document.getElementById("glossary-search"),
  glossaryFilters: document.getElementById("glossary-filters"),
  glossaryList: document.getElementById("glossary-list"),
  templateForm: document.getElementById("template-form"),
  templateId: document.getElementById("template-id"),
  templateName: document.getElementById("template-name"),
  templateDescription: document.getElementById("template-description"),
  templateQuery: document.getElementById("template-query"),
  templateScheduleEnabled: document.getElementById("template-schedule-enabled"),
  templateScheduleDay: document.getElementById("template-schedule-day"),
  templateScheduleTime: document.getElementById("template-schedule-time"),
  templateCancelButton: document.getElementById("template-cancel-button"),
  templateList: document.getElementById("template-list"),
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

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatConfidence(intent) {
  if (!intent) return "—";
  const percent = `${Math.round((intent.confidence || 0) * 100)}%`;
  return `${percent} - ${state.parse?.preview?.confidence_label || "—"}`;
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
  if (typeof value === "number") return value;
  const numeric = Number(String(value ?? "").replace(/\s/g, "").replace(",", "."));
  return Number.isFinite(numeric) ? numeric : 0;
}

function formatDate(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function inferProvider(run) {
  return run?.provider || state.provider || "rule-based";
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
  if (state.parse?.preview?.metric_label) {
    const metric = metricTitle();
    const group = state.parse?.preview?.group_by_label;
    return group ? `${metric} по ${group.toLowerCase()} - ${periodTitle()}` : `${metric} - ${periodTitle()}`;
  }
  const fallback = elements.queryInput.value.trim();
  return fallback ? fallback.slice(0, 60) : "Аналитический отчёт";
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

function getSeries(result) {
  if (!result?.rows?.length || result.columns.length < 2) {
    return [];
  }
  return result.rows.map((row) => ({
    label: String(row[0] ?? ""),
    value: parseNumeric(row[1]),
    raw: row,
  }));
}

function summarizeSeries(series) {
  if (!series.length) {
    return null;
  }
  const total = series.reduce((sum, item) => sum + item.value, 0);
  const top = series.reduce((best, current) => (current.value > best.value ? current : best), series[0]);
  const bottom = series.reduce((best, current) => (current.value < best.value ? current : best), series[0]);
  const average = total / series.length;
  return { total, top, bottom, average };
}

function renderParse() {
  const parse = state.parse;
  if (!parse) {
    return;
  }

  state.provider = inferProvider(parse);
  elements.providerPill.textContent = `LLM: ${state.provider} - ${Math.round((parse.intent.confidence || 0) * 100)}%`;
  if (elements.sidebarProvider) {
    elements.sidebarProvider.textContent = `LLM: ${state.provider}`;
  }

  elements.intentSummary.textContent = parse.preview.summary || "Не удалось интерпретировать запрос.";
  elements.intentMetric.textContent = parse.preview.metric_label || "—";
  elements.intentGroup.textContent = parse.preview.group_by_label || "Без разбивки";
  elements.intentPeriod.textContent = parse.intent.period?.label || "—";
  elements.intentConfidence.textContent = formatConfidence(parse.intent);

  const stripItems = [
    ["Метрика", parse.preview.metric_label || "Требует уточнения"],
    ["Разрез", parse.preview.group_by_label || "Без разбивки"],
    ["Период", parse.intent.period?.label || "—"],
  ];
  elements.intentHighlightStrip.innerHTML = stripItems
    .map(
      ([label, value]) => `
        <div class="mini-stat-card">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </div>
      `
    )
    .join("");

  const filters = parse.preview.applied_filters || [];
  elements.intentFilters.innerHTML = filters.length
    ? filters.map((item) => `<span>${escapeHtml(item)}</span>`).join("")
    : `<span>Дополнительные фильтры не заданы</span>`;

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
        <strong>По запросу пока нет данных</strong>
        <p>Попробуйте изменить формулировку, период или уточнить метрику. Все guardrails и интерпретация при этом сохранятся.</p>
      </div>
    `;
    elements.summaryStats.innerHTML = "";
    elements.storyGrid.innerHTML = "";
    return;
  }

  const series = getSeries(run.result);
  const summary = summarizeSeries(series);

  if (!series.length) {
    const value = run.result.rows[0]?.[0] ?? "—";
    elements.businessSummary.innerHTML = `
      <div class="summary-hero">
        <span class="summary-label">Главный вывод</span>
        <strong>${escapeHtml(metricTitle())}: ${escapeHtml(formatNumber(value))}</strong>
        <p>Итоговое значение рассчитано без дополнительной разбивки за период «${escapeHtml(periodTitle())}».</p>
      </div>
    `;
    elements.summaryStats.innerHTML = `
      <span class="summary-chip"><strong>Период:</strong> ${escapeHtml(periodTitle())}</span>
      <span class="summary-chip"><strong>Тип ответа:</strong> KPI</span>
      <span class="summary-chip"><strong>Источник:</strong> ${escapeHtml(inferProvider(run))}</span>
    `;
    elements.storyGrid.innerHTML = `
      <div class="story-card">
        <span>Главный KPI</span>
        <strong>${escapeHtml(formatNumber(value))}</strong>
        <p>Это итоговая цифра по запросу без сравнительного ряда.</p>
      </div>
    `;
    return;
  }

  elements.businessSummary.innerHTML = `
    <div class="summary-hero">
      <span class="summary-label">Главный вывод</span>
      <strong>Лидер: ${escapeHtml(summary.top.label)}</strong>
      <p>${escapeHtml(metricTitle())} за период «${periodTitle()}» сильнее всего выглядит у ${summary.top.label}. Значение лидера составляет ${formatNumber(summary.top.value)}.</p>
    </div>
  `;

  elements.summaryStats.innerHTML = `
    <span class="summary-chip"><strong>Лидер:</strong> ${escapeHtml(summary.top.label)} - ${escapeHtml(formatNumber(summary.top.value))}</span>
    <span class="summary-chip"><strong>Минимум:</strong> ${escapeHtml(summary.bottom.label)} - ${escapeHtml(formatNumber(summary.bottom.value))}</span>
    <span class="summary-chip"><strong>Среднее:</strong> ${escapeHtml(formatNumber(summary.average))}</span>
    <span class="summary-chip"><strong>Всего по выборке:</strong> ${escapeHtml(formatNumber(summary.total))}</span>
  `;

  elements.storyGrid.innerHTML = `
    <div class="story-card highlight">
      <span>Лидер</span>
      <strong>${escapeHtml(summary.top.label)}</strong>
      <p>${escapeHtml(formatNumber(summary.top.value))}</p>
    </div>
    <div class="story-card">
      <span>Контраст</span>
      <strong>${escapeHtml(summary.bottom.label)}</strong>
      <p>${escapeHtml(formatNumber(summary.bottom.value))}</p>
    </div>
    <div class="story-card">
      <span>Разрез</span>
      <strong>${escapeHtml(groupTitle())}</strong>
      <p>${escapeHtml(periodTitle())}</p>
    </div>
  `;
}

function renderTable(result) {
  elements.resultTableHead.innerHTML = "";
  elements.resultTableBody.innerHTML = "";

  if (!result?.columns?.length) {
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

function createInsightChip(label, value) {
  return `<span class="insight-chip"><strong>${escapeHtml(label)}:</strong> ${escapeHtml(value)}</span>`;
}

function trimLabel(value) {
  const text = String(value ?? "");
  return text.length > 14 ? `${text.slice(0, 13)}…` : text;
}

function renderLineChart(series, width, height, padding) {
  const max = Math.max(...series.map((item) => item.value), 1);
  const min = Math.min(...series.map((item) => item.value), 0);
  const span = Math.max(max - min, 1);
  const step = series.length > 1 ? (width - padding * 2) / (series.length - 1) : 0;

  const points = series.map((item, index) => {
    const x = padding + index * step;
    const y = height - padding - ((item.value - min) / span) * (height - padding * 2);
    return { ...item, x, y };
  });

  const line = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
  const area = `${line} L ${points.at(-1).x} ${height - padding} L ${points[0].x} ${height - padding} Z`;
  const dots = points
    .map(
      (point) => `
        <circle class="line-dot" cx="${point.x}" cy="${point.y}" r="5"></circle>
        <text class="axis-label" x="${point.x}" y="${height - 10}" text-anchor="middle">${escapeHtml(trimLabel(point.label))}</text>
      `
    )
    .join("");

  return `
    <path class="area-path" d="${area}"></path>
    <path class="line-path" d="${line}"></path>
    ${dots}
  `;
}

function renderBarChart(series, width, height, padding) {
  const max = Math.max(...series.map((item) => item.value), 1);
  const slotWidth = (width - padding * 2) / series.length;
  const barWidth = Math.max(slotWidth - 18, 18);

  return series
    .map((item, index) => {
      const barHeight = (item.value / max) * (height - padding * 2);
      const x = padding + index * slotWidth + (slotWidth - barWidth) / 2;
      const y = height - padding - barHeight;
      return `
        <rect class="bar-shadow" x="${x}" y="${y + 6}" width="${barWidth}" height="${Math.max(barHeight - 6, 0)}" rx="18"></rect>
        <rect class="bar" x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="18"></rect>
        <text class="value-label" x="${x + barWidth / 2}" y="${Math.max(y - 8, 16)}" text-anchor="middle">${escapeHtml(formatNumber(item.value))}</text>
        <text class="axis-label" x="${x + barWidth / 2}" y="${height - 10}" text-anchor="middle">${escapeHtml(trimLabel(item.label))}</text>
      `;
    })
    .join("");
}

function renderRankingPanel(series) {
  if (!series.length) {
    elements.rankingPanel.innerHTML = `<div class="ranking-empty">Рейтинг появится после запуска запроса.</div>`;
    return;
  }
  const ranking = [...series].sort((a, b) => b.value - a.value).slice(0, 5);
  elements.rankingPanel.innerHTML = `
    <div class="ranking-head">
      <span>Топ по значению</span>
      <strong>${escapeHtml(metricTitle())}</strong>
    </div>
    <div class="ranking-list">
      ${ranking
        .map(
          (item, index) => `
            <div class="ranking-item">
              <span class="ranking-index">#${index + 1}</span>
              <div>
                <strong>${escapeHtml(item.label)}</strong>
                <small>${escapeHtml(formatNumber(item.value))}</small>
              </div>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

function renderChart(run) {
  if (!run?.result?.rows?.length) {
    elements.chartInsights.innerHTML = "";
    elements.chartSurface.innerHTML = `<div class="chart-placeholder">После выполнения запроса здесь появится выразительный график, а рядом — рейтинг и быстрые выводы.</div>`;
    renderRankingPanel([]);
    return;
  }

  const series = getSeries(run.result).slice(0, 8);
  const summary = summarizeSeries(series);
  elements.chartInsights.innerHTML = summary
    ? [
        createInsightChip("Лидер", `${summary.top.label} - ${formatNumber(summary.top.value)}`),
        createInsightChip("Среднее", formatNumber(summary.average)),
        createInsightChip("Период", periodTitle()),
      ].join("")
    : [
        createInsightChip("Формат", "KPI"),
        createInsightChip("Период", periodTitle()),
        createInsightChip("Источник", inferProvider(run)),
      ].join("");

  if (!series.length) {
    const value = run.result.rows[0]?.[0] ?? "—";
    elements.chartSurface.innerHTML = `
      <div class="kpi-card">
        <span>KPI</span>
        <strong>${escapeHtml(formatNumber(value))}</strong>
        <p>${escapeHtml(metricTitle())} за период «${periodTitle()}». Для такого вопроса визуальный акцент сделан на одном показателе.</p>
      </div>
    `;
    renderRankingPanel([]);
    return;
  }

  const width = 780;
  const height = 340;
  const padding = 44;
  const grid = [0, 0.25, 0.5, 0.75, 1]
    .map((ratio) => {
      const y = height - padding - ratio * (height - padding * 2);
      return `<line class="chart-grid-line" x1="${padding}" x2="${width - padding}" y1="${y}" y2="${y}"></line>`;
    })
    .join("");
  const chartBody = run.chart?.type === "line" ? renderLineChart(series, width, height, padding) : renderBarChart(series, width, height, padding);

  elements.chartSurface.innerHTML = `
    <div class="chart-frame">
      <div class="chart-caption">
        <strong>${escapeHtml(metricTitle())}</strong>
        <p>${escapeHtml(groupTitle())} - ${escapeHtml(periodTitle())}</p>
      </div>
      <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
        <defs>
          <linearGradient id="drivee-gradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stop-color="#8de80f"></stop>
            <stop offset="100%" stop-color="#b7f04d"></stop>
          </linearGradient>
          <linearGradient id="drivee-area" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stop-color="rgba(141, 232, 15, 0.26)"></stop>
            <stop offset="100%" stop-color="rgba(141, 232, 15, 0.02)"></stop>
          </linearGradient>
        </defs>
        ${grid}
        ${chartBody}
      </svg>
    </div>
  `;

  renderRankingPanel(series);
}

function renderRun() {
  if (!state.run) {
    return;
  }
  state.provider = inferProvider(state.run);
  elements.providerPill.textContent = `LLM: ${state.provider}`;
  if (elements.sidebarProvider) {
    elements.sidebarProvider.textContent = `LLM: ${state.provider}`;
  }
  elements.sqlPreview.textContent = state.run.sql || "SQL не выполнялся: системе потребовалось уточнение запроса.";
  renderSummary(state.run);
  renderTable(state.run.result);
  renderChart(state.run);
}

function getGlossaryEntries() {
  if (!state.semantic) {
    return [];
  }

  const metricEntries = (state.semantic.metrics || []).map((metric) => ({
    term: metric.title,
    kind: "metric",
    canonical: metric.id,
    description: metric.description,
  }));
  const dimensionEntries = (state.semantic.dimensions || []).map((dimension) => ({
    term: dimension.title,
    kind: "dimension",
    canonical: dimension.id,
    description: dimension.description,
  }));
  return [...metricEntries, ...dimensionEntries, ...(state.semantic.terms || [])];
}

function renderGlossary() {
  if (!elements.glossaryList || !elements.glossarySearch) {
    return;
  }
  const search = elements.glossarySearch.value.trim().toLowerCase();
  const entries = getGlossaryEntries().filter((item) => {
    if (state.glossaryKind !== "all" && item.kind !== state.glossaryKind) {
      return false;
    }
    if (!search) {
      return true;
    }
    const haystack = [item.term, item.description, item.canonical, item.kind].join(" ").toLowerCase();
    return haystack.includes(search);
  });

  if (!entries.length) {
    elements.glossaryList.innerHTML = `<div class="empty-state">По вашему фильтру ничего не найдено.</div>`;
    return;
  }

  elements.glossaryList.innerHTML = entries
    .map(
      (item) => `
        <article class="glossary-card">
          <span class="glossary-kind">${escapeHtml(item.kind)}</span>
          <strong>${escapeHtml(item.term)}</strong>
          <p>${escapeHtml(item.description)}</p>
          <div class="glossary-meta">
            <span>${escapeHtml(item.canonical)}</span>
            <button class="mini-button" type="button" data-glossary-term="${escapeHtml(item.term)}">Вставить в запрос</button>
          </div>
        </article>
      `
    )
    .join("");
}

function renderReports() {
  if (!elements.reportList) {
    return;
  }
  if (!state.reports.length) {
    elements.reportList.innerHTML = `
      <div class="empty-state">
        Пока нет сохранённых отчётов. После первого сохранения или регулярного запуска архив появится здесь.
      </div>
    `;
    return;
  }

  elements.reportList.innerHTML = state.reports
    .slice(0, 8)
    .map(
      (report) => `
        <article class="report-card">
          <div class="report-card-head">
            <div>
              <strong>${escapeHtml(report.name)}</strong>
              <p>${escapeHtml(report.query_text)}</p>
            </div>
            <span class="report-badge ${report.source === "scheduled" ? "scheduled" : "manual"}">${escapeHtml(report.source || "manual")}</span>
          </div>
          <div class="report-card-meta">
            <span>Обновлён: ${escapeHtml(formatDate(report.updated_at))}</span>
            <span>Строк: ${escapeHtml(report.result?.count ?? 0)}</span>
            ${report.template_name ? `<span>Шаблон: ${escapeHtml(report.template_name)}</span>` : ""}
          </div>
          <div class="button-row compact">
            <button class="mini-button" type="button" data-report-open="${report.id}">Открыть</button>
            <button class="mini-button" type="button" data-report-export="pdf" data-report-id="${report.id}">PDF</button>
            <button class="mini-button" type="button" data-report-export="docx" data-report-id="${report.id}">DOCX</button>
          </div>
        </article>
      `
    )
    .join("");
}

function resetTemplateForm() {
  elements.templateId.value = "";
  elements.templateName.value = "";
  elements.templateDescription.value = "";
  elements.templateQuery.value = "";
  elements.templateScheduleEnabled.checked = false;
  elements.templateScheduleDay.value = "1";
  elements.templateScheduleTime.value = "13:00";
  document.getElementById("template-save-button").textContent = "Сохранить шаблон";
}

function fillTemplateForm(data) {
  elements.templateId.value = data.id || "";
  elements.templateName.value = data.name || "";
  elements.templateDescription.value = data.description || "";
  elements.templateQuery.value = data.query_text || "";
  elements.templateScheduleEnabled.checked = Boolean(data.schedule?.enabled);
  elements.templateScheduleDay.value = String(data.schedule?.day_of_week ?? 1);
  elements.templateScheduleTime.value = `${String(data.schedule?.hour ?? 13).padStart(2, "0")}:${String(data.schedule?.minute ?? 0).padStart(2, "0")}`;
  document.getElementById("template-save-button").textContent = data.id ? "Обновить шаблон" : "Сохранить шаблон";
}

function renderTemplates() {
  if (!state.templates.length) {
    elements.templateList.innerHTML = `
      <div class="empty-state">
        Шаблонов пока нет. Нажмите «Добавить шаблон», сохраните запрос и при необходимости включите регулярный запуск.
      </div>
    `;
    return;
  }

  elements.templateList.innerHTML = state.templates
    .map(
      (template) => `
        <article class="template-card">
          <div class="template-card-head">
            <div>
              <strong>${escapeHtml(template.name)}</strong>
              <p>${escapeHtml(template.description || template.query_text)}</p>
            </div>
            <span class="template-status ${template.schedule?.enabled ? "live" : "draft"}">${escapeHtml(template.schedule?.enabled ? "регулярный" : "ручной")}</span>
          </div>
          <div class="template-query-preview">${escapeHtml(template.query_text)}</div>
          <div class="template-meta">
            <span>${escapeHtml(template.schedule?.label || "Без расписания")}</span>
            ${template.schedule?.next_run ? `<span>Следующий запуск: ${escapeHtml(formatDate(template.schedule.next_run))}</span>` : ""}
            ${template.last_run_at ? `<span>Последний запуск: ${escapeHtml(formatDate(template.last_run_at))}</span>` : ""}
            ${template.last_status ? `<span>Статус: ${escapeHtml(template.last_status)}</span>` : ""}
          </div>
          <div class="button-row compact">
            <button class="mini-button" type="button" data-template-apply="${template.id}">Использовать</button>
            <button class="mini-button" type="button" data-template-run="${template.id}">Запустить сейчас</button>
            <button class="mini-button" type="button" data-template-edit="${template.id}">Редактировать</button>
            <button class="mini-button danger" type="button" data-template-delete="${template.id}">Удалить</button>
          </div>
        </article>
      `
    )
    .join("");
}

async function refreshReports() {
  if (!elements.reportList) {
    return;
  }
  state.reports = await api("/api/v1/reports").catch(() => []);
  renderReports();
}

async function refreshTemplates() {
  state.templates = await api("/api/v1/reports/templates").catch(() => []);
  renderTemplates();
  renderTemplatePrompts();
}

async function openReportById(reportId) {
  try {
    setBanner("Открываю сохранённый отчёт...");
    const run = await api(`/api/v1/reports/${reportId}/run`, { method: "POST" });
    state.run = run;
    state.parse = { intent: run.intent, preview: run.preview, provider: run.provider };
    renderParse();
    renderRun();
    setBanner("Отчёт успешно выполнен.");
  } catch (error) {
    setBanner(error.message);
  }
}

async function exportCurrent(format) {
  if (!state.run?.sql && !state.run?.result?.rows?.length) {
    setBanner("Сначала получите результат, затем можно скачать отчёт.");
    return;
  }

  const response = await fetch(`/api/v1/reports/export?format=${format}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name: buildReportName(),
      query_text: elements.queryInput.value.trim(),
      run: state.run,
    }),
  });

  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(data.error || "Не удалось подготовить экспорт");
  }

  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `${buildReportName().replace(/[^\p{L}\p{N}\- ]/gu, "").trim() || "report"}.${format}`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

async function exportSavedReport(reportId, format) {
  const response = await fetch(`/api/v1/reports/${reportId}/export?format=${format}`);
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(data.error || "Не удалось скачать отчёт");
  }
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `report-${reportId}.${format}`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function renderTemplatePrompts() {
  elements.samplePrompts.innerHTML = "";
  if (!state.templates.length) {
    elements.samplePrompts.innerHTML = `<span class="empty-inline">Шаблонов пока нет. Создай первый шаблон через кнопку «Добавить шаблон».</span>`;
    return;
  }

  state.templates.forEach((template) => {
    const button = document.createElement("button");
    button.className = "prompt-chip";
    button.type = "button";
    button.textContent = template.name;
    button.addEventListener("click", () => {
      elements.queryInput.value = template.query_text;
      setBanner(`Шаблон «${template.name}» подставлен в редактор запроса.`);
    });
    elements.samplePrompts.appendChild(button);
  });
}

async function loadInitialData() {
  try {
    const [semantic] = await Promise.all([api("/api/v1/meta/schema"), refreshTemplates(), refreshReports()]);
    state.semantic = semantic;
    renderGlossary();

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

  setBanner("Понимаю запрос и сверяю его с бизнес-словариём...");
  try {
    state.parse = await api("/api/v1/query/parse", {
      method: "POST",
      body: JSON.stringify({ text }),
    });
    renderParse();
    setBanner("Запрос понятен. Можно сразу запускать результат или сохранить его как шаблон.");
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

  setBanner("Собираю результат, визуализацию и управленческое резюме...");
  try {
    state.run = await api("/api/v1/query/run", {
      method: "POST",
      body: JSON.stringify({ text }),
    });
    state.parse = {
      intent: state.run.intent,
      preview: state.run.preview,
      provider: state.run.provider,
    };
    renderParse();
    renderRun();
    setBanner(state.run.result?.count ? `Результат готов. Получено строк: ${state.run.result.count}.` : "Запрос выполнен, но результат пустой.");
  } catch (error) {
    setBanner(error.message);
  }
}

async function saveReport() {
  const text = elements.queryInput.value.trim();
  if (!state.run?.sql) {
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
        preview: state.run.preview,
        result: state.run.result,
        provider: state.run.provider,
        source: "manual",
      }),
    });
    await refreshReports();
    setBanner(`Отчёт «${name}» сохранён.`);
  } catch (error) {
    setBanner(error.message);
  }
}

function startTemplateFromCurrent() {
  fillTemplateForm({
    name: buildReportName(),
    description: state.parse?.preview?.summary || "Пользовательский шаблон для повторного запуска",
    query_text: elements.queryInput.value.trim(),
    schedule: { enabled: false, day_of_week: 1, hour: 13, minute: 0 },
  });
  document.getElementById("templates").scrollIntoView({ behavior: "smooth", block: "start" });
}

function collectTemplatePayload() {
  const [hour, minute] = elements.templateScheduleTime.value.split(":").map((item) => Number(item));
  return {
    name: elements.templateName.value.trim(),
    description: elements.templateDescription.value.trim(),
    query_text: elements.templateQuery.value.trim(),
    schedule: {
      enabled: elements.templateScheduleEnabled.checked,
      day_of_week: Number(elements.templateScheduleDay.value),
      hour: Number.isFinite(hour) ? hour : 13,
      minute: Number.isFinite(minute) ? minute : 0,
      timezone: "Europe/Moscow",
    },
  };
}

async function submitTemplate(event) {
  event.preventDefault();
  const payload = collectTemplatePayload();
  if (!payload.name || !payload.query_text) {
    setBanner("У шаблона должны быть название и текст запроса.");
    return;
  }

  const templateId = elements.templateId.value.trim();
  const isEditing = Boolean(templateId);
  setBanner(isEditing ? "Обновляю шаблон..." : "Сохраняю шаблон...");

  try {
    if (isEditing) {
      await api(`/api/v1/reports/templates/${templateId}`, {
        method: "PUT",
        body: JSON.stringify(payload),
      });
    } else {
      await api("/api/v1/reports/templates", {
        method: "POST",
        body: JSON.stringify(payload),
      });
    }
    await refreshTemplates();
    resetTemplateForm();
    setBanner(isEditing ? "Шаблон обновлён." : "Шаблон сохранён.");
  } catch (error) {
    setBanner(error.message);
  }
}

async function runTemplate(templateId) {
  setBanner("Запускаю шаблон и сохраняю результат...");
  try {
    const response = await api(`/api/v1/reports/templates/${templateId}/run`, { method: "POST" });
    state.run = response.run;
    state.parse = {
      intent: response.run.intent,
      preview: response.run.preview,
      provider: response.run.provider,
    };
    renderParse();
    renderRun();
    await Promise.all([refreshReports(), refreshTemplates()]);
    setBanner("Шаблон выполнен. Новый снимок отчёта сохранён.");
  } catch (error) {
    setBanner(error.message);
  }
}

async function deleteTemplate(templateId) {
  setBanner("Удаляю шаблон...");
  try {
    await api(`/api/v1/reports/templates/${templateId}`, { method: "DELETE" });
    await refreshTemplates();
    if (elements.templateId.value === String(templateId)) {
      resetTemplateForm();
    }
    setBanner("Шаблон удалён.");
  } catch (error) {
    setBanner(error.message);
  }
}

function handleTemplateAction(event) {
  const target = event.target.closest("button");
  if (!target) return;

  const applyId = target.dataset.templateApply;
  const runId = target.dataset.templateRun;
  const editId = target.dataset.templateEdit;
  const deleteId = target.dataset.templateDelete;

  if (applyId) {
    const template = state.templates.find((item) => String(item.id) === applyId);
    if (template) {
      elements.queryInput.value = template.query_text;
      setBanner(`Шаблон «${template.name}» подставлен в редактор запроса.`);
      document.getElementById("workspace").scrollIntoView({ behavior: "smooth", block: "start" });
    }
    return;
  }

  if (runId) {
    runTemplate(runId);
    return;
  }

  if (editId) {
    const template = state.templates.find((item) => String(item.id) === editId);
    if (template) {
      fillTemplateForm(template);
      document.getElementById("templates").scrollIntoView({ behavior: "smooth", block: "start" });
      setBanner(`Шаблон «${template.name}» открыт на редактирование.`);
    }
    return;
  }

  if (deleteId) {
    deleteTemplate(deleteId);
  }
}

function handleReportAction(event) {
  const target = event.target.closest("button");
  if (!target) return;

  if (target.dataset.reportOpen) {
    openReportById(target.dataset.reportOpen);
    return;
  }

  if (target.dataset.reportExport && target.dataset.reportId) {
    exportSavedReport(target.dataset.reportId, target.dataset.reportExport).catch((error) => setBanner(error.message));
  }
}

elements.parseButton.addEventListener("click", parseQuery);
elements.runButton.addEventListener("click", runQuery);
elements.saveButton.addEventListener("click", saveReport);
elements.addTemplateButton.addEventListener("click", startTemplateFromCurrent);
elements.exportPdfButton.addEventListener("click", () => exportCurrent("pdf").catch((error) => setBanner(error.message)));
elements.templateForm.addEventListener("submit", submitTemplate);
elements.templateCancelButton.addEventListener("click", resetTemplateForm);
elements.templateList.addEventListener("click", handleTemplateAction);
if (elements.reportList) {
  elements.reportList.addEventListener("click", handleReportAction);
}
if (elements.glossarySearch) {
  elements.glossarySearch.addEventListener("input", renderGlossary);
}
if (elements.glossaryFilters) {
  elements.glossaryFilters.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-kind]");
    if (!button) return;
    state.glossaryKind = button.dataset.kind;
    [...elements.glossaryFilters.querySelectorAll("button")].forEach((item) => item.classList.toggle("active", item === button));
    renderGlossary();
  });
}
if (elements.glossaryList) {
  elements.glossaryList.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-glossary-term]");
    if (!button) return;
    const term = button.dataset.glossaryTerm;
    const current = elements.queryInput.value.trim();
    elements.queryInput.value = current ? `${current} ${term}` : term;
    document.getElementById("workspace").scrollIntoView({ behavior: "smooth", block: "start" });
  });
}

loadInitialData();

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

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
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

async function deleteSavedReport(reportId) {
  const response = await fetch(`/api/v1/reports/${reportId}`, {
    method: "DELETE",
  });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(data.error || "Не удалось удалить отчёт");
  }
}

function renderReports(reports) {
  const container = document.getElementById("reports-page-list");
  if (!reports.length) {
    container.innerHTML = `<div class="empty-state">Пока нет сохранённых отчётов.</div>`;
    return;
  }

  container.innerHTML = reports
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
            <button class="mini-button" type="button" data-open="${report.id}">Открыть на главной</button>
            <button class="mini-button" type="button" data-export="pdf" data-id="${report.id}">PDF</button>
            <button class="mini-button danger" type="button" data-delete="${report.id}">Удалить</button>
          </div>
        </article>
      `
    )
    .join("");
}

async function loadReportsPage() {
  try {
    const reports = await api("/api/v1/reports");
    renderReports(reports);
  } catch (error) {
    document.getElementById("reports-page-list").innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  }
}

document.getElementById("reports-page-list").addEventListener("click", (event) => {
  const button = event.target.closest("button");
  if (!button) return;

  if (button.dataset.open) {
    window.localStorage.setItem("drivee:openReportId", String(button.dataset.open));
    window.location.href = "/";
    return;
  }

  if (button.dataset.export && button.dataset.id) {
    exportSavedReport(button.dataset.id, button.dataset.export).catch((error) => {
      document.getElementById("reports-page-list").insertAdjacentHTML("afterbegin", `<div class="empty-state">${escapeHtml(error.message)}</div>`);
    });
    return;
  }

  if (button.dataset.delete) {
    deleteSavedReport(button.dataset.delete)
      .then(loadReportsPage)
      .catch((error) => {
        document.getElementById("reports-page-list").insertAdjacentHTML("afterbegin", `<div class="empty-state">${escapeHtml(error.message)}</div>`);
      });
  }
});

loadReportsPage();

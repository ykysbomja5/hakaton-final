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

function renderReports(reports) {
  const container = document.getElementById("reports-page-list");
  if (!reports.length) {
    container.innerHTML = `<div class="empty-state">Пока нет сохранённых отчётов.</div>`;
    return;
  }

  container.innerHTML = "";
  reports.forEach((report) => {
    const item = document.createElement("article");
    item.className = "report-item";
    item.innerHTML = `
      <strong>${report.name}</strong>
      <p>${report.query_text}</p>
      <small>Обновлён: ${formatDate(report.updated_at)}</small>
      <button type="button">Открыть на главной</button>
    `;
    item.querySelector("button").addEventListener("click", () => {
      window.localStorage.setItem("drivee:openReportId", String(report.id));
      window.location.href = "/";
    });
    container.appendChild(item);
  });
}

async function loadReportsPage() {
  try {
    const reports = await api("/api/v1/reports");
    renderReports(reports);
  } catch (error) {
    document.getElementById("reports-page-list").innerHTML = `<div class="empty-state">${error.message}</div>`;
  }
}

loadReportsPage();

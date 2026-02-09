const state = {
  pollMs: 3000,
  selectedExecutionId: null,
  selectedRunId: null,
  selectedJobId: null,
  runLogOffset: 0,
};

const healthBadge = document.getElementById("healthBadge");
const pollBadge = document.getElementById("pollBadge");
const executionsBody = document.getElementById("executionsBody");
const jobsBody = document.getElementById("jobsBody");
const runsBody = document.getElementById("runsBody");
const jobsTitle = document.getElementById("jobsTitle");
const jobDetails = document.getElementById("jobDetails");
const runLogs = document.getElementById("runLogs");
const runError = document.getElementById("runError");
const runForm = document.getElementById("runForm");

async function api(path, options = {}) {
  const response = await fetch(path, options);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || `Request failed: ${response.status}`);
  }
  return response.json();
}

function splitArgs(value) {
  const regex = /[^\s"]+|"([^"]*)"/g;
  const args = [];
  let match = null;
  while ((match = regex.exec(value)) !== null) {
    args.push(match[1] !== undefined ? match[1] : match[0]);
  }
  return args;
}

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined) {
    return "--";
  }
  return Number(seconds).toFixed(2);
}

function formatTime(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  return date.toLocaleString();
}

function statusCell(status) {
  return `<span class="status ${status}">${status}</span>`;
}

function rowIdLabel(id) {
  return id ? id.slice(0, 8) : "--";
}

async function loadHealth() {
  try {
    const data = await api("/api/health");
    state.pollMs = data.poll_interval_seconds * 1000;
    healthBadge.textContent = "Connected";
    pollBadge.textContent = `Poll: ${data.poll_interval_seconds}s`;
  } catch (error) {
    healthBadge.textContent = "Offline";
    pollBadge.textContent = "Poll: --";
    console.error(error);
  }
}

async function loadExecutions() {
  const data = await api("/api/executions?page=1&page_size=40");
  executionsBody.innerHTML = "";
  for (const execution of data.executions) {
    const tr = document.createElement("tr");
    tr.dataset.active = String(execution.id === state.selectedExecutionId);
    tr.innerHTML = `
      <td>${rowIdLabel(execution.id)}</td>
      <td>${statusCell(execution.status)}</td>
      <td>${execution.task_fullname || "--"}</td>
      <td>${formatTime(execution.start_time)}</td>
      <td>${formatDuration(execution.duration_seconds)}</td>
    `;
    tr.addEventListener("click", () => {
      state.selectedExecutionId = execution.id;
      state.selectedJobId = null;
      jobDetails.textContent = "";
      loadExecutions().catch(console.error);
      loadJobs().catch(console.error);
    });
    executionsBody.appendChild(tr);
  }
}

async function loadJobs() {
  if (!state.selectedExecutionId) {
    jobsTitle.textContent = "Jobs";
    jobsBody.innerHTML = "";
    return;
  }

  jobsTitle.textContent = `Jobs (${rowIdLabel(state.selectedExecutionId)})`;
  const data = await api(
    `/api/executions/${state.selectedExecutionId}/jobs?page=1&page_size=120`
  );
  jobsBody.innerHTML = "";

  for (const job of data.jobs) {
    const tr = document.createElement("tr");
    tr.dataset.active = String(job.id === state.selectedJobId);
    tr.innerHTML = `
      <td>${"&nbsp;".repeat(job.depth * 2)}${rowIdLabel(job.id)}</td>
      <td>${statusCell(job.status)}</td>
      <td>${job.task_fullname || "--"}</td>
      <td>${job.depth}</td>
      <td>${formatDuration(job.duration_seconds)}</td>
    `;
    tr.addEventListener("click", () => {
      state.selectedJobId = job.id;
      loadJobs().catch(console.error);
      loadJobDetails().catch(console.error);
    });
    jobsBody.appendChild(tr);
  }
}

async function loadJobDetails() {
  if (!state.selectedJobId) {
    jobDetails.textContent = "";
    return;
  }
  const data = await api(`/api/jobs/${state.selectedJobId}`);
  jobDetails.textContent = JSON.stringify(data, null, 2);
}

async function loadRuns() {
  const data = await api("/api/runs?limit=30");
  runsBody.innerHTML = "";
  for (const run of data.runs) {
    const tr = document.createElement("tr");
    tr.dataset.active = String(run.run_id === state.selectedRunId);
    tr.innerHTML = `
      <td>${rowIdLabel(run.run_id)}</td>
      <td>${statusCell(run.status)}</td>
      <td>${rowIdLabel(run.execution_id)}</td>
    `;
    tr.addEventListener("click", () => {
      state.selectedRunId = run.run_id;
      state.runLogOffset = 0;
      runLogs.textContent = "";
      loadRuns().catch(console.error);
      loadRunLogs().catch(console.error);
    });
    runsBody.appendChild(tr);
  }
}

async function loadRunLogs() {
  if (!state.selectedRunId) {
    return;
  }
  const payload = await api(
    `/api/runs/${state.selectedRunId}/logs?offset=${state.runLogOffset}`
  );
  if (payload.lines.length) {
    runLogs.textContent += `${payload.lines.join("\n")}\n`;
    runLogs.scrollTop = runLogs.scrollHeight;
  }
  state.runLogOffset = payload.next_offset;
}

runForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  runError.textContent = "";
  const script = document.getElementById("scriptInput").value.trim();
  const task = document.getElementById("taskInput").value.trim();
  const argvText = document.getElementById("argvInput").value.trim();
  const config = document.getElementById("configInput").value.trim();
  const cwd = document.getElementById("cwdInput").value.trim();

  if (!script || !task) {
    runError.textContent = "Script and task are required.";
    return;
  }

  const body = {
    script,
    task,
    argv: splitArgs(argvText),
    config: config || null,
    cwd: cwd || null,
  };

  try {
    const run = await api("/api/runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    state.selectedRunId = run.run_id;
    state.runLogOffset = 0;
    runLogs.textContent = "";
    loadRuns().catch(console.error);
    loadRunLogs().catch(console.error);
  } catch (error) {
    runError.textContent = error.message;
  }
});

document
  .getElementById("refreshExecutionsButton")
  .addEventListener("click", () => loadExecutions().catch(console.error));
document
  .getElementById("refreshJobsButton")
  .addEventListener("click", () => loadJobs().catch(console.error));
document
  .getElementById("refreshRunsButton")
  .addEventListener("click", () => loadRuns().catch(console.error));

async function refreshLoop() {
  await Promise.allSettled([loadExecutions(), loadRuns()]);
  await Promise.allSettled([loadJobs(), loadRunLogs(), loadJobDetails()]);
  window.setTimeout(refreshLoop, state.pollMs);
}

async function bootstrap() {
  await loadHealth();
  await Promise.allSettled([loadExecutions(), loadRuns()]);
  refreshLoop();
}

bootstrap().catch(console.error);

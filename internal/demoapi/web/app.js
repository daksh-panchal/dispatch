import "./components/metric-card.js";
import { eventRow } from "./components/event-row.js";
import { jobCard } from "./components/job-card.js";
import { workerCard, workerStatus } from "./components/worker-card.js";
import { formatDuration } from "./components/format.js";

const EVENT_TYPES = [
  "JobSubmitted",
  "JobClaimed",
  "JobRetryScheduled",
  "JobSucceeded",
  "JobFailed",
  "JobRecovered",
  "JobCancelled",
];
const TERMINAL_STATES = new Set(["succeeded", "failed", "cancelled"]);
const state = {
  runId: "",
  runs: [],
  snapshot: null,
  events: [],
  eventSequences: new Set(),
  source: null,
  snapshotPending: false,
  leaseMaximums: new Map(),
};

const elements = Object.fromEntries([
  "connection", "connection-label", "run-heading", "run-status", "run-context",
  "run-form", "job-count", "worker-count", "work-duration", "start-run",
  "run-picker", "add-worker", "progress-label", "progress-fill", "queue-label",
  "metric-queued", "metric-running", "metric-retrying", "metric-succeeded", "metric-workers",
  "workers-summary", "worker-list", "event-list", "stream-state", "stream-label", "toast-region",
  "jobs-queued", "jobs-running", "jobs-retrying", "jobs-terminal",
  "count-queued", "count-running", "count-retrying", "count-terminal",
].map((id) => [id, document.getElementById(id)]));

initialize();

async function initialize() {
  elements["run-form"].addEventListener("submit", startRun);
  elements["run-picker"].addEventListener("change", (event) => selectRun(event.target.value));
  elements["add-worker"].addEventListener("click", addWorker);
  elements["worker-list"].addEventListener("click", (event) => {
    const button = event.target.closest("[data-kill-worker]");
    if (button) killWorker(button.dataset.killWorker, button);
  });

  await checkConnection();
  await loadRuns();
  window.setInterval(checkConnection, 10000);
  window.setInterval(() => refreshSnapshot(true), 750);
}

async function checkConnection() {
  try {
    const response = await fetch("/health/ready", { cache: "no-store" });
    if (!response.ok) throw new Error("Scheduler unavailable");
    setConnection("online", "Scheduler connected");
  } catch {
    setConnection("offline", "Scheduler unavailable");
  }
}

function setConnection(status, label) {
  elements.connection.className = `connection is-${status}`;
  elements["connection-label"].textContent = label;
}

async function loadRuns(preferredRunId = "") {
  try {
    const response = await api("/api/v1/demo/runs");
    state.runs = response.items || [];
    renderRunPicker();
    const nextRun = preferredRunId || state.runId || state.runs[0]?.id || "";
    if (nextRun && nextRun !== state.runId) await selectRun(nextRun);
    else if (!nextRun) renderEmptyConsole();
  } catch (error) {
    showToast(error.message, true);
  }
}

function renderRunPicker() {
  const picker = elements["run-picker"];
  picker.replaceChildren();
  if (!state.runs.length) {
    picker.append(new Option("No runs yet", ""));
    picker.disabled = true;
    return;
  }
  picker.disabled = false;
  for (const run of state.runs) {
    picker.append(new Option(`${runLabel(run)} · ${run.status}`, run.id, false, run.id === state.runId));
  }
}

async function selectRun(runId) {
  if (!runId) return;
  closeEventStream();
  Object.assign(state, { runId, snapshot: null, events: [] });
  state.eventSequences.clear();
  state.leaseMaximums.clear();
  elements["run-picker"].value = runId;
  setStreamState("connecting", "Connecting…");
  renderLoadingConsole();
  connectEventStream();
  await refreshSnapshot(false);
}

async function startRun(event) {
  event.preventDefault();
  setButtonBusy(elements["start-run"], true, "Starting…");
  try {
    const run = await api("/api/v1/demo/runs", {
      method: "POST",
      body: JSON.stringify({
        jobCount: Number(elements["job-count"].value),
        workerCount: Number(elements["worker-count"].value),
        workDurationMs: Number(elements["work-duration"].value),
      }),
    });
    await loadRuns(run.id);
    if (state.runId !== run.id) await selectRun(run.id);
    showToast(`${run.jobCount} jobs submitted to ${run.workers.length} workers.`);
  } catch (error) {
    showToast(error.message, true);
  } finally {
    setButtonBusy(elements["start-run"], false, "Start run");
  }
}

async function addWorker() {
  if (!state.runId) return;
  setButtonBusy(elements["add-worker"], true, "Adding…");
  try {
    await api(`/api/v1/demo/runs/${state.runId}/workers`, {
      method: "POST",
      body: JSON.stringify({ count: 1 }),
    });
    await refreshSnapshot(false);
    showToast("Replacement worker registered and started.");
  } catch (error) {
    showToast(error.message, true);
  } finally {
    setButtonBusy(elements["add-worker"], false, "+ Add worker");
    syncAddWorkerButton();
  }
}

async function killWorker(workerId, button) {
  if (!state.runId || !workerId) return;
  setButtonBusy(button, true, "Stopping…");
  try {
    await api(`/api/v1/demo/runs/${state.runId}/workers/${workerId}/kill`, { method: "POST" });
    await refreshSnapshot(false);
    showToast("Worker stopped. Its active lease was left for recovery.");
  } catch (error) {
    showToast(error.message, true);
    setButtonBusy(button, false, "Kill");
  }
}

async function refreshSnapshot(silent) {
  if (!state.runId || state.snapshotPending) return;
  state.snapshotPending = true;
  try {
    state.snapshot = await api(`/api/v1/demo/runs/${state.runId}/snapshot`);
    renderSnapshot();
    updateRunCache(state.snapshot.run);
  } catch (error) {
    if (!silent) showToast(error.message, true);
  } finally {
    state.snapshotPending = false;
  }
}

function updateRunCache(run) {
  const index = state.runs.findIndex((item) => item.id === run.id);
  if (index >= 0) state.runs[index] = run;
  else state.runs.unshift(run);
  renderRunPicker();
}

function connectEventStream() {
  if (!state.runId || !window.EventSource) {
    setStreamState("error", "Streaming unsupported");
    return;
  }
  state.source = new EventSource(`/api/v1/demo/runs/${state.runId}/events/stream?afterSequence=0`);
  state.source.onopen = () => setStreamState("live", "Live");
  state.source.onerror = () => setStreamState("error", "Reconnecting…");
  for (const type of EVENT_TYPES) {
    state.source.addEventListener(type, (message) => receiveEvent(type, message));
  }
}

function receiveEvent(type, message) {
  try {
    const event = JSON.parse(message.data);
    if (state.eventSequences.has(event.sequence)) return;
    state.eventSequences.add(event.sequence);
    event.type ||= type;
    state.events.unshift(event);
    state.events.sort((left, right) => right.sequence - left.sequence);
    state.events = state.events.slice(0, 40);
    renderEvents();
    window.setTimeout(() => refreshSnapshot(true), 80);
  } catch (error) {
    console.warn("Could not decode Work-Flow event", error);
  }
}

function closeEventStream() {
  state.source?.close();
  state.source = null;
}

function renderSnapshot() {
  const { run, jobs = [], workers = [] } = state.snapshot;
  const groups = groupJobs(jobs);
  const progress = run.jobCount ? Math.round((groups.terminal.length / run.jobCount) * 100) : 0;
  const succeeded = jobs.filter((job) => job.state === "succeeded").length;
  const activeWorkers = run.workers.filter((worker) => !["killed", "stopped"].includes(worker.status));
  const busyWorkers = run.workers.filter((worker) => worker.status === "working");

  elements["run-heading"].textContent = runLabel(run);
  elements["run-status"].textContent = run.status;
  elements["run-status"].className = `status-pill is-${run.status}`;
  elements["run-context"].textContent = `${run.jobCount} jobs · ${run.workers.length} workers · ${formatDuration(run.workDurationMs)} each`;
  elements["queue-label"].textContent = run.queue;
  elements["queue-label"].title = run.queue;
  elements["progress-label"].textContent = `${progress}%`;
  elements["progress-fill"].style.width = `${progress}%`;
  syncAddWorkerButton();

  elements["metric-queued"].update(groups.queued.length);
  elements["metric-running"].update(groups.running.length);
  elements["metric-retrying"].update(groups.retrying.length);
  elements["metric-succeeded"].update(succeeded, `${progress}% complete`);
  elements["metric-workers"].update(activeWorkers.length, `${busyWorkers.length} busy · ${Math.max(0, activeWorkers.length - busyWorkers.length)} idle`);
  elements["workers-summary"].textContent = `${run.workers.length} registered`;

  renderJobLane("queued", groups.queued, "No queued jobs");
  renderJobLane("running", groups.running, "No active leases");
  renderJobLane("retrying", groups.retrying, "No recovered jobs");
  renderJobLane("terminal", groups.terminal, "Nothing finished yet");
  renderWorkers(run.workers, new Map(workers.map((worker) => [worker.id, worker])));
}

function groupJobs(jobs) {
  const groups = { queued: [], running: [], retrying: [], terminal: [] };
  const sorted = [...jobs].sort((left, right) => right.priority - left.priority || left.createdAt.localeCompare(right.createdAt));
  for (const job of sorted) {
    if (TERMINAL_STATES.has(job.state)) groups.terminal.push(job);
    else if (job.state === "running") groups.running.push(job);
    else if (job.state === "queued" && job.attemptCount > 0) groups.retrying.push(job);
    else groups.queued.push(job);
  }
  return groups;
}

function renderJobLane(name, jobs, emptyMessage) {
  const container = elements[`jobs-${name}`];
  elements[`count-${name}`].textContent = jobs.length;
  if (!jobs.length) {
    container.innerHTML = `<p class="empty">${emptyMessage}</p>`;
    return;
  }
  const visible = jobs.slice(0, 10);
  container.innerHTML = visible.map((job) => jobCard(job, runningProgress(job))).join("");
  if (jobs.length > visible.length) container.insertAdjacentHTML("beforeend", `<p class="more">+${jobs.length - visible.length} more</p>`);
}

function runningProgress(job) {
  if (job.state !== "running") return 0;
  const duration = Number(job.payload?.durationMs || 0);
  const claimed = state.events.find((event) => event.type === "JobClaimed" && event.jobId === job.id);
  if (!duration || !claimed) return 35;
  return Math.max(4, Math.min(96, ((Date.now() - new Date(claimed.occurredAt).getTime()) / duration) * 100));
}

function renderWorkers(runWorkers, schedulerWorkers) {
  if (!runWorkers.length) {
    elements["worker-list"].innerHTML = '<p class="empty tall">Workers appear when a run starts.</p>';
    return;
  }
  elements["worker-list"].innerHTML = runWorkers.map((worker) => {
    const scheduler = schedulerWorkers.get(worker.id) || {};
    const status = workerStatus(worker, scheduler);
    return workerCard(worker, scheduler, leaseDisplay(worker.id, scheduler.leaseExpiresAt, status));
  }).join("");
}

function leaseDisplay(workerId, expiresAt, status) {
  if (status === "killed") return { percent: 0, label: "lost", tone: "is-danger" };
  if (!expiresAt || status !== "working") return { percent: 100, label: "—", tone: "" };
  const remaining = Math.max(0, new Date(expiresAt).getTime() - Date.now());
  const maximum = Math.max(state.leaseMaximums.get(workerId) || remaining, remaining, 1);
  state.leaseMaximums.set(workerId, maximum);
  const percent = Math.min(100, (remaining / maximum) * 100);
  const tone = percent < 15 ? "is-danger" : percent < 35 ? "is-warning" : "";
  return { percent, label: `${(remaining / 1000).toFixed(1)}s`, tone };
}

function renderEvents() {
  elements["event-list"].innerHTML = state.events.length
    ? state.events.slice(0, 14).map(eventRow).join("")
    : '<p class="empty">Lifecycle events will appear here.</p>';
}

function renderLoadingConsole() {
  elements["run-heading"].textContent = "Loading run…";
  elements["run-status"].textContent = "connecting";
  elements["run-status"].className = "status-pill is-starting";
  elements["run-context"].textContent = "Reading durable scheduler state.";
}

function renderEmptyConsole() {
  state.snapshot = null;
  elements["run-heading"].textContent = "No active run";
  elements["run-status"].textContent = "ready";
  elements["run-status"].className = "status-pill";
  elements["run-context"].textContent = "Start a run to watch jobs move through the scheduler.";
  elements["add-worker"].disabled = true;
}

function syncAddWorkerButton() {
  const status = state.snapshot?.run.status;
  elements["add-worker"].disabled = !status || status === "completed" || status === "failed";
}

function setStreamState(status, label) {
  elements["stream-state"].className = `stream-state is-${status}`;
  elements["stream-label"].textContent = label;
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (options.body) headers.set("Content-Type", "application/json");
  const response = await fetch(path, { ...options, headers, cache: "no-store" });
  if (!response.ok) {
    let message = `Request failed with HTTP ${response.status}`;
    try {
      const problem = await response.json();
      message = problem.detail || problem.title || message;
    } catch { /* Keep the HTTP status message. */ }
    throw new Error(message);
  }
  return response.status === 204 ? null : response.json();
}

function setButtonBusy(button, busy, label) {
  button.disabled = busy;
  (button.querySelector("span") || button).textContent = label;
}

function showToast(message, error = false) {
  const toast = document.createElement("div");
  toast.className = `toast${error ? " is-error" : ""}`;
  toast.textContent = message;
  elements["toast-region"].append(toast);
  window.setTimeout(() => toast.remove(), 4200);
}

function runLabel(run) {
  return `Run #${run.id.slice(0, 8).toUpperCase()}`;
}

window.addEventListener("beforeunload", closeEventStream);

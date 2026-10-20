import { escapeHTML, shortId } from "./format.js";

export function workerCard(worker, scheduler, lease) {
  const status = worker.status === "killed" ? "killed" : worker.status || scheduler.status || "idle";
  const jobId = worker.currentJobId || scheduler.currentJobId;
  const canKill = status !== "killed" && status !== "stopped";

  return `
    <article class="worker-card">
      <div class="worker-row">
        <div class="worker-info">
          <div class="worker-name">${escapeHTML(worker.name)}</div>
          <div class="worker-job">${escapeHTML(workerDetail(worker, status, jobId))}</div>
        </div>
        <span class="worker-status is-${escapeHTML(status)}">${escapeHTML(status)}</span>
        <button class="button danger" type="button" data-kill-worker="${escapeHTML(worker.id)}" ${canKill ? "" : "disabled"}>
          <span>${status === "killed" ? "Killed" : "Kill"}</span>
        </button>
      </div>
      <div class="lease-row">
        <div class="lease-track"><span class="${lease.tone}" style="width:${lease.percent}%"></span></div>
        <span class="lease-time">${lease.label}</span>
      </div>
    </article>`;
}

export function workerStatus(worker, scheduler) {
  return worker.status === "killed" ? "killed" : worker.status || scheduler.status || "idle";
}

function workerDetail(worker, status, jobId) {
  if (status === "killed") return jobId ? `Connection lost during job-${shortId(jobId)}` : "Connection lost";
  if (jobId) return `Executing job-${shortId(jobId)}`;
  return worker.lastError || "Waiting for work";
}

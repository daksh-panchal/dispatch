import { escapeHTML, formatDuration, shortId } from "./format.js";

export function jobCard(job, progress = 0) {
  const duration = Number(job.payload?.durationMs || 0);
  const tone = job.state === "queued" && job.attemptCount > 0 ? "retrying" : job.state;
  const detail = jobDetail(job);
  const progressBar = job.state === "running"
    ? `<div class="job-progress"><span style="width:${progress}%"></span></div>`
    : "";

  return `
    <article class="job-card is-${escapeHTML(tone)}" title="${escapeHTML(job.id)}">
      <div class="job-top">
        <span class="job-id">job-${shortId(job.id)}</span>
        <span class="job-priority">P${Number(job.priority)}</span>
      </div>
      <div class="job-meta">
        <span>${escapeHTML(detail)}</span>
        <span>${duration ? formatDuration(duration) : escapeHTML(job.state)}</span>
      </div>
      ${progressBar}
    </article>`;
}

function jobDetail(job) {
  if (job.state === "running") return `worker ${shortId(job.workerId)}`;
  if (job.state === "succeeded") return "completed";
  if (job.state === "failed") return "attempts exhausted";
  if (job.state === "cancelled") return "cancelled";
  return `attempt ${job.attemptCount}/${job.maximumAttempts}`;
}

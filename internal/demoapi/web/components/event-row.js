import { escapeHTML, formatClock, shortId } from "./format.js";

export function eventRow(event) {
  return `
    <div class="event-row ${eventTone(event.type)}">
      <span class="event-time">${formatClock(event.occurredAt)}</span>
      <span class="event-marker"></span>
      <span class="event-type">${escapeHTML(event.type)}</span>
      <span class="event-job">job-${shortId(event.jobId)}</span>
      <span class="event-detail">${escapeHTML(eventDetail(event))}</span>
    </div>`;
}

function eventDetail(event) {
  const payload = event.payload || {};
  const worker = payload.workerId ? `worker ${shortId(payload.workerId)}` : "a worker";
  switch (event.type) {
    case "JobSubmitted": return `queued with ${payload.maximumAttempts || 0} attempts`;
    case "JobClaimed": return `${worker} acquired attempt ${payload.attemptCount || 1}`;
    case "JobRecovered": return `expired lease recovered for attempt ${(payload.attemptCount || 0) + 1}`;
    case "JobRetryScheduled": return `retry scheduled after attempt ${payload.attemptCount || 0}`;
    case "JobSucceeded": return `completed after ${payload.attemptCount || 1} attempt(s)`;
    case "JobFailed": return payload.lastError || "retry budget exhausted";
    case "JobCancelled": return "removed from scheduling";
    default: return payload.state ? `state changed to ${payload.state}` : "scheduler state changed";
  }
}

function eventTone(type) {
  if (type === "JobSucceeded") return "is-success";
  if (type === "JobRecovered" || type === "JobRetryScheduled") return "is-warning";
  if (type === "JobFailed" || type === "JobCancelled") return "is-danger";
  return "";
}

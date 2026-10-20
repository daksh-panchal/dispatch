import { escapeHTML } from "./format.js";

class MetricCard extends HTMLElement {
  connectedCallback() {
    if (this.valueElement) return;
    this.innerHTML = `
      <span class="metric-label">${escapeHTML(this.getAttribute("label"))}</span>
      <strong class="metric-value">0</strong>
      <span class="metric-note">${escapeHTML(this.getAttribute("note"))}</span>`;
    this.valueElement = this.querySelector(".metric-value");
    this.noteElement = this.querySelector(".metric-note");
  }

  update(value, note) {
    this.valueElement.textContent = String(value);
    if (note !== undefined) this.noteElement.textContent = note;
  }
}

customElements.define("metric-card", MetricCard);

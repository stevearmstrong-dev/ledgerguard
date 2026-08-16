const scenarioCopy = {
  MATCHED: {
    title: "Matched transaction",
    description: "Payment and ledger amounts agree in the same currency.",
    expected: "Matched",
  },
  AMOUNT_MISMATCH: {
    title: "Amount mismatch",
    description: "The ledger records a different amount from the payment.",
    expected: "Review",
  },
  CURRENCY_MISMATCH: {
    title: "Currency mismatch",
    description: "Amounts agree, but the event currencies do not.",
    expected: "Review",
  },
  DUPLICATE_PAYMENT: {
    title: "Duplicate payment",
    description: "An upstream retry publishes the same payment event ID twice.",
    expected: "Deduplicated",
  },
  OUT_OF_ORDER_MATCH: {
    title: "Out-of-order match",
    description: "The ledger event arrives before its payment counterpart.",
    expected: "Matched",
  },
  MISSING_LEDGER_ENTRY: {
    title: "Missing ledger entry",
    description: "A payment waits for a ledger counterpart until event time advances.",
    expected: "Windowed",
  },
  MISSING_PAYMENT: {
    title: "Missing payment",
    description: "A ledger entry waits for a payment until event time advances.",
    expected: "Windowed",
  },
};

const state = {
  results: [],
  filter: "all",
  running: null,
  toastTimer: null,
  pollTimer: null,
};

const elements = {
  apiStatus: document.querySelector("#api-status"),
  scenarioGrid: document.querySelector("#scenario-grid"),
  currentTransaction: document.querySelector("#current-transaction"),
  currentRunState: document.querySelector("#current-run-state"),
  eventClock: document.querySelector("#event-clock"),
  pipeline: document.querySelector("#event-pipeline"),
  eventLedger: document.querySelector("#event-ledger"),
  outcomeBadge: document.querySelector("#outcome-badge"),
  outcomeEmpty: document.querySelector("#outcome-empty"),
  outcomeDetail: document.querySelector("#outcome-detail"),
  outcomeTransaction: document.querySelector("#outcome-transaction"),
  outcomeReason: document.querySelector("#outcome-reason"),
  paymentAmount: document.querySelector("#payment-amount"),
  paymentEvent: document.querySelector("#payment-event"),
  ledgerAmount: document.querySelector("#ledger-amount"),
  ledgerEvent: document.querySelector("#ledger-event"),
  resultFilter: document.querySelector("#result-filter"),
  clearResults: document.querySelector("#clear-results"),
  resultTable: document.querySelector("#result-table"),
  metricTotal: document.querySelector("#metric-total"),
  metricMatched: document.querySelector("#metric-matched"),
  metricReview: document.querySelector("#metric-review"),
  metricRate: document.querySelector("#metric-rate"),
  toast: document.querySelector("#toast"),
};

async function request(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: { Accept: "application/json", ...options.headers },
  });

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      message = body.detail || body.message || message;
    } catch {
      // Keep the HTTP status when the response has no JSON body.
    }
    throw new Error(message);
  }

  if (response.status === 204) return null;
  return response.json();
}

async function initialize() {
  updateClock();
  window.setInterval(updateClock, 1000);

  try {
    await request("/api");
    setApiStatus("online", "API connected");
    const scenarios = await request("/api/scenarios");
    renderScenarios(scenarios);
    await refreshResults();
    state.pollTimer = window.setInterval(refreshResults, 2000);
  } catch (error) {
    setApiStatus("error", "API unavailable");
    elements.scenarioGrid.innerHTML = `<p class="loading-copy">${escapeHtml(error.message)}</p>`;
  }
}

function renderScenarios(scenarios) {
  const ordered = [
    "MATCHED",
    "AMOUNT_MISMATCH",
    "CURRENCY_MISMATCH",
    "DUPLICATE_PAYMENT",
    "OUT_OF_ORDER_MATCH",
    "MISSING_LEDGER_ENTRY",
    "MISSING_PAYMENT",
  ].filter((scenario) => scenarios.includes(scenario));

  elements.scenarioGrid.innerHTML = ordered.map((scenario, index) => {
    const copy = scenarioCopy[scenario];
    return `
      <button class="scenario-card" type="button" data-scenario="${scenario}">
        <span class="scenario-card-number">${String(index + 1).padStart(2, "0")}</span>
        <h3>${copy.title}</h3>
        <p>${copy.description}</p>
        <span class="scenario-card-outcome"><span>${copy.expected}</span><span aria-hidden="true">Run →</span></span>
      </button>
    `;
  }).join("");

  elements.scenarioGrid.querySelectorAll("[data-scenario]").forEach((button) => {
    button.addEventListener("click", () => runScenario(button.dataset.scenario));
  });
}

async function runScenario(scenario) {
  if (state.running) return;
  state.running = scenario;
  setScenarioButtonsDisabled(true, scenario);
  resetPipeline();
  setPipelineStage("produce", "Publishing synthetic events to the payment and ledger topics.");
  elements.currentTransaction.textContent = "Submitting…";
  elements.currentRunState.textContent = scenarioCopy[scenario].title;

  try {
    await delay(260);
    const submission = await request(`/api/scenarios/${scenario.toLowerCase().replaceAll("_", "-")}`, {
      method: "POST",
    });

    elements.currentTransaction.textContent = submission.transactionId;
    elements.currentRunState.textContent = `${submission.publishedEvents.length} event${submission.publishedEvents.length === 1 ? "" : "s"} acknowledged by Kafka.`;
    setPipelineStage("broker", `${submission.publishedEvents.length} event${submission.publishedEvents.length === 1 ? "" : "s"} acknowledged by the broker.`);
    await delay(420);
    setPipelineStage("reconcile", "Kafka Streams is evaluating event time, deduplication state, and the outer join window.");

    const missingSideScenario = scenario.startsWith("MISSING_");
    const results = await waitForResult(submission.transactionId, missingSideScenario ? 5500 : 8500);

    if (results.length > 0) {
      const focusResult = results.find((result) => result.status !== "MATCHED") || results[0];
      const needsReview = focusResult.status !== "MATCHED";
      setPipelineStage(
        "outcome",
        `${focusResult.status.replaceAll("_", " ")} written to the audit${needsReview ? " and exception" : ""} topic.`,
        needsReview,
      );
      renderOutcome(focusResult);
      showToast(`${submission.transactionId} produced ${results.length} reconciliation outcome${results.length === 1 ? "" : "s"}.`);
    } else if (missingSideScenario) {
      showPendingWindow(submission.transactionId);
    } else {
      throw new Error("No reconciliation result arrived before the demo timeout.");
    }

    await refreshResults();
  } catch (error) {
    resetPipeline();
    elements.currentRunState.textContent = "Scenario failed before reconciliation.";
    showToast(error.message, true);
  } finally {
    state.running = null;
    setScenarioButtonsDisabled(false);
  }
}

async function waitForResult(transactionId, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const results = await request(`/api/reconciliations?transactionId=${encodeURIComponent(transactionId)}`);
    if (results.length > 0) return results;
    await delay(350);
  }
  return [];
}

function showPendingWindow(transactionId) {
  elements.currentRunState.textContent = "Waiting for stream time to advance beyond the join window.";
  elements.eventLedger.className = "event-ledger is-active";
  elements.eventLedger.innerHTML = `
    <span>WINDOW OPEN</span>
    <p>${escapeHtml(transactionId)} is intentionally unmatched. Run another scenario after the 10s window to advance event time and emit the missing-side outcome.</p>
  `;
  showToast("The unmatched event is held by the event-time window—this is expected Kafka Streams behavior.");
}

function setPipelineStage(stage, message, review = false) {
  const nodes = [...elements.pipeline.querySelectorAll("[data-stage]")];
  const targetIndex = nodes.findIndex((node) => node.dataset.stage === stage);
  nodes.forEach((node, index) => {
    node.classList.toggle("is-active", index <= targetIndex && !(review && index === targetIndex));
    node.classList.toggle("is-review", review && index === targetIndex);
  });
  elements.eventLedger.className = `event-ledger ${review ? "is-review" : "is-active"}`;
  elements.eventLedger.innerHTML = `<span>${review ? "REVIEW" : "PROCESSING"}</span><p>${escapeHtml(message)}</p>`;
}

function resetPipeline() {
  elements.pipeline.querySelectorAll("[data-stage]").forEach((node) => node.classList.remove("is-active", "is-review"));
  elements.eventLedger.className = "event-ledger";
  elements.eventLedger.innerHTML = "<span>WAITING</span><p>The topology is ready for a scenario.</p>";
}

async function refreshResults() {
  try {
    state.results = await request("/api/reconciliations");
    renderMetrics();
    renderTable();
    setApiStatus("online", "API connected");
  } catch (error) {
    setApiStatus("error", "API unavailable");
  }
}

function renderOutcome(result) {
  const matched = result.status === "MATCHED";
  elements.outcomeEmpty.hidden = true;
  elements.outcomeDetail.hidden = false;
  elements.outcomeBadge.textContent = result.status.replaceAll("_", " ");
  elements.outcomeBadge.className = `outcome-badge ${matched ? "badge-matched" : "badge-review"}`;
  elements.outcomeTransaction.textContent = result.transactionId;
  elements.outcomeReason.textContent = result.reason;
  elements.paymentAmount.textContent = formatAmount(result.paymentAmount, result.paymentCurrency);
  elements.paymentEvent.textContent = result.paymentEventId ? `EVENT ${shortId(result.paymentEventId)}` : "NO PAYMENT EVENT";
  elements.ledgerAmount.textContent = formatAmount(result.ledgerAmount, result.ledgerCurrency);
  elements.ledgerEvent.textContent = result.ledgerEventId ? `EVENT ${shortId(result.ledgerEventId)}` : "NO LEDGER EVENT";
}

function renderMetrics() {
  const total = state.results.length;
  const matched = state.results.filter((result) => result.status === "MATCHED").length;
  const review = total - matched;
  elements.metricTotal.textContent = String(total);
  elements.metricMatched.textContent = String(matched);
  elements.metricReview.textContent = String(review);
  elements.metricRate.textContent = total ? `${Math.round((matched / total) * 100)}%` : "—";
}

function renderTable() {
  const filtered = state.results.filter((result) => {
    if (state.filter === "matched") return result.status === "MATCHED";
    if (state.filter === "review") return result.status !== "MATCHED";
    return true;
  });

  if (!filtered.length) {
    elements.resultTable.innerHTML = '<tr class="empty-row"><td colspan="6">No reconciliation events match this view.</td></tr>';
    return;
  }

  elements.resultTable.innerHTML = filtered.map((result) => {
    const matched = result.status === "MATCHED";
    return `
      <tr>
        <td>${escapeHtml(formatTime(result.evaluatedAt))}</td>
        <td>${escapeHtml(result.transactionId)}</td>
        <td><span class="table-status ${matched ? "is-matched" : "is-review"}">${escapeHtml(result.status.replaceAll("_", " "))}</span></td>
        <td>${escapeHtml(formatAmount(result.paymentAmount, result.paymentCurrency))}</td>
        <td>${escapeHtml(formatAmount(result.ledgerAmount, result.ledgerCurrency))}</td>
        <td class="table-reason">${escapeHtml(result.reason)}</td>
      </tr>
    `;
  }).join("");
}

function setScenarioButtonsDisabled(disabled, activeScenario) {
  elements.scenarioGrid.querySelectorAll("[data-scenario]").forEach((button) => {
    button.disabled = disabled;
    button.classList.toggle("is-running", button.dataset.scenario === activeScenario);
  });
}

function setApiStatus(status, label) {
  elements.apiStatus.className = `status-pill ${status === "online" ? "is-online" : "is-error"}`;
  elements.apiStatus.innerHTML = `<i aria-hidden="true"></i>${escapeHtml(label)}`;
}

function updateClock() {
  elements.eventClock.textContent = `${new Intl.DateTimeFormat("en-CA", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: "UTC",
  }).format(new Date())} UTC`;
}

function formatAmount(amount, currency) {
  if (amount == null) return "—";
  try {
    return new Intl.NumberFormat("en-CA", { style: "currency", currency: currency || "CAD" }).format(amount);
  } catch {
    return `${amount} ${currency || ""}`.trim();
  }
}

function formatTime(value) {
  return new Intl.DateTimeFormat("en-CA", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function shortId(value) {
  return String(value).slice(0, 8).toUpperCase();
}

function showToast(message, isError = false) {
  window.clearTimeout(state.toastTimer);
  elements.toast.textContent = message;
  elements.toast.className = `toast${isError ? " is-error" : ""}`;
  elements.toast.hidden = false;
  state.toastTimer = window.setTimeout(() => {
    elements.toast.hidden = true;
  }, 4200);
}

function delay(milliseconds) {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

elements.resultFilter.addEventListener("change", (event) => {
  state.filter = event.target.value;
  renderTable();
});

elements.clearResults.addEventListener("click", async () => {
  try {
    await request("/api/reconciliations", { method: "DELETE" });
    state.results = [];
    renderMetrics();
    renderTable();
    showToast("The in-memory result projection was cleared.");
  } catch (error) {
    showToast(error.message, true);
  }
});

document.addEventListener("visibilitychange", () => {
  if (document.hidden && state.pollTimer) {
    window.clearInterval(state.pollTimer);
    state.pollTimer = null;
  } else if (!document.hidden && !state.pollTimer) {
    refreshResults();
    state.pollTimer = window.setInterval(refreshResults, 2000);
  }
});

initialize();

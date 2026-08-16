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

const traceDefaults = {
  validate: "Spring validates amounts, currencies, ordering, and event presence.",
  serialize: "Immutable Java records receive event IDs, references, and event timestamps.",
  broker: "Idempotent producers write JSON records, keyed by transaction ID.",
  stream: "Kafka Streams deduplicates event IDs and joins both sides in event time.",
  classify: "Domain rules compare event presence, amount, and currency.",
  project: "The result reaches the audit topic, exception topic when needed, and this query view.",
};

const duplicateStatuses = new Set(["DUPLICATE_PAYMENT", "DUPLICATE_LEDGER_ENTRY"]);

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
  transactionForm: document.querySelector("#transaction-form"),
  transactionIdInput: document.querySelector("#transaction-id-input"),
  regenerateTransaction: document.querySelector("#regenerate-transaction"),
  publishPayment: document.querySelector("#publish-payment"),
  publishLedger: document.querySelector("#publish-ledger"),
  paymentEditor: document.querySelector("#payment-editor"),
  ledgerEditor: document.querySelector("#ledger-editor"),
  paymentInputAmount: document.querySelector("#payment-input-amount"),
  paymentInputCurrency: document.querySelector("#payment-input-currency"),
  ledgerInputAmount: document.querySelector("#ledger-input-amount"),
  ledgerInputCurrency: document.querySelector("#ledger-input-currency"),
  eventOrderInput: document.querySelector("#event-order-input"),
  eventDelayInput: document.querySelector("#event-delay-input"),
  duplicatePaymentInput: document.querySelector("#duplicate-payment-input"),
  runTransaction: document.querySelector("#run-transaction"),
  currentTransaction: document.querySelector("#current-transaction"),
  currentRunState: document.querySelector("#current-run-state"),
  eventClock: document.querySelector("#event-clock"),
  pipeline: document.querySelector("#event-pipeline"),
  eventLedger: document.querySelector("#event-ledger"),
  traceStatus: document.querySelector("#trace-status"),
  processingTrace: document.querySelector("#processing-trace"),
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
  generateTransactionId();
  syncEventEditors();
  resetTrace();

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
  await executeRun({
    runKey: scenario,
    label: scenarioCopy[scenario].title,
    requestSummary: { preset: scenario },
    submit: () => request(`/api/scenarios/${scenario.toLowerCase().replaceAll("_", "-")}`, {
      method: "POST",
    }),
  });
}

async function runCustomTransaction(event) {
  event.preventDefault();
  if (state.running || !elements.transactionForm.reportValidity()) return;
  if (!elements.publishPayment.checked && !elements.publishLedger.checked) {
    showToast("Publish at least one payment or ledger event.", true);
    return;
  }

  const transaction = {
    transactionId: elements.transactionIdInput.value,
    paymentAmount: elements.publishPayment.checked ? Number(elements.paymentInputAmount.value) : null,
    paymentCurrency: elements.publishPayment.checked ? elements.paymentInputCurrency.value : null,
    ledgerAmount: elements.publishLedger.checked ? Number(elements.ledgerInputAmount.value) : null,
    ledgerCurrency: elements.publishLedger.checked ? elements.ledgerInputCurrency.value : null,
    eventOrder: elements.eventOrderInput.value,
    eventDelayMs: Number(elements.eventDelayInput.value),
    duplicatePayment: elements.publishPayment.checked && elements.duplicatePaymentInput.checked,
  };

  await executeRun({
    runKey: "CUSTOM",
    label: "Custom transaction",
    requestSummary: transaction,
    submit: () => request("/api/transactions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(transaction),
    }),
  });
}

async function executeRun({ runKey, label, requestSummary, submit }) {
  if (state.running) return;
  state.running = runKey;
  setControlsDisabled(true, runKey);
  resetPipeline();
  resetTrace();
  setTraceStep("validate", "active", "Request received. Applying bean validation and cross-field event rules.", requestSummary);
  elements.traceStatus.textContent = "PROCESSING";
  setPipelineStage("produce", "The demo API is validating the transaction and constructing event records.");
  elements.currentTransaction.textContent = "Submitting…";
  elements.currentRunState.textContent = label;

  try {
    const submission = await submit();
    const eventCount = submission.publishedEvents.length;
    const missingSide = isMissingSideSubmission(submission);

    elements.currentTransaction.textContent = submission.transactionId;
    elements.currentRunState.textContent = `${eventCount} event${eventCount === 1 ? "" : "s"} acknowledged by Kafka.`;

    setTraceStep(
      "validate",
      "complete",
      `${submission.runType} request accepted as ${submission.transactionId}.`,
      { transactionId: submission.transactionId, eventOrder: submission.eventOrder, delayMs: submission.eventDelayMs },
    );
    await delay(180);
    setTraceStep(
      "serialize",
      "complete",
      `${eventCount} immutable event record${eventCount === 1 ? " was" : "s were"} assigned IDs, references, and event timestamps.`,
      submission.publishedEvents.map(eventPayload),
    );
    await delay(220);
    setPipelineStage("broker", `${eventCount} event${eventCount === 1 ? "" : "s"} acknowledged by the broker.`);
    setTraceStep(
      "broker",
      "complete",
      `Kafka acknowledged ${eventCount} idempotent write${eventCount === 1 ? "" : "s"}, partitioned by ${submission.transactionId}.`,
      submission.publishedEvents.map((publishedEvent) => ({
        sequence: publishedEvent.sequence,
        topic: publishedEvent.topic,
        key: publishedEvent.transactionId,
        eventId: publishedEvent.eventId,
        duplicate: publishedEvent.duplicate,
      })),
    );
    await delay(260);
    setPipelineStage("reconcile", "Kafka Streams is evaluating event time, deduplication state, and the outer join window.");
    setTraceStep(
      "stream",
      "active",
      `Consuming by transaction key. The outer join uses a 10-second event-time window and 3-second grace period.`,
      {
        key: submission.transactionId,
        eventOrder: submission.eventOrder,
        eventDelayMs: submission.eventDelayMs,
        exactlyOnce: "exactly_once_v2",
        deduplicationRetention: "24h",
      },
    );

    const results = await waitForResult(submission.transactionId, missingSide ? 5500 : 8500);

    if (results.length > 0) {
      const focusResult = results.find((result) => result.status !== "MATCHED") || results[0];
      const needsReview = focusResult.status !== "MATCHED";
      setTraceStep(
        "stream",
        "complete",
        `Stream state produced ${results.length} reconciliation record${results.length === 1 ? "" : "s"}.`,
        { resultsProduced: results.length, transactionId: submission.transactionId },
      );
      await delay(220);
      setTraceStep(
        "classify",
        needsReview ? "review" : "complete",
        `${focusResult.status.replaceAll("_", " ")}: ${focusResult.reason}`,
        reconciliationPayload(focusResult),
      );
      await delay(220);
      setTraceStep(
        "project",
        needsReview ? "review" : "complete",
        `Written to reconciliations.v1${needsReview ? " and reconciliation-exceptions.v1" : ""}, then consumed by the query projection.`,
        {
          auditTopic: "reconciliations.v1",
          exceptionTopic: needsReview ? "reconciliation-exceptions.v1" : null,
          apiResource: `/api/reconciliations?transactionId=${submission.transactionId}`,
        },
      );
      setPipelineStage(
        "outcome",
        `${focusResult.status.replaceAll("_", " ")} written to the audit${needsReview ? " and exception" : ""} topic.`,
        needsReview,
      );
      elements.traceStatus.textContent = needsReview ? "REVIEW" : "COMPLETE";
      renderOutcome(focusResult);
      const duplicateCount = results.filter(isDuplicateResult).length;
      const outcomeCount = results.length - duplicateCount;
      const outcomeSummary = `${outcomeCount} reconciliation outcome${outcomeCount === 1 ? "" : "s"}`;
      const duplicateSummary = duplicateCount
        ? ` and ${duplicateCount} duplicate audit event${duplicateCount === 1 ? "" : "s"}`
        : "";
      showToast(`${submission.transactionId} produced ${outcomeSummary}${duplicateSummary}.`);
    } else if (missingSide) {
      showPendingWindow(submission.transactionId);
    } else {
      throw new Error("No reconciliation result arrived before the demo timeout.");
    }

    await refreshResults();
  } catch (error) {
    resetPipeline();
    markActiveTraceFailed(error.message);
    elements.currentRunState.textContent = "Submission failed before reconciliation.";
    elements.traceStatus.textContent = "FAILED";
    showToast(error.message, true);
  } finally {
    state.running = null;
    setControlsDisabled(false);
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

function isMissingSideSubmission(submission) {
  const types = new Set(submission.publishedEvents.map((publishedEvent) => publishedEvent.eventType));
  return !types.has("PAYMENT") || !types.has("LEDGER_ENTRY");
}

function eventPayload(publishedEvent) {
  return {
    sequence: publishedEvent.sequence,
    eventType: publishedEvent.eventType,
    eventId: publishedEvent.eventId,
    transactionId: publishedEvent.transactionId,
    amount: publishedEvent.amount,
    currency: publishedEvent.currency,
    reference: publishedEvent.reference,
    occurredAt: publishedEvent.occurredAt,
    duplicate: publishedEvent.duplicate,
  };
}

function reconciliationPayload(result) {
  return {
    reconciliationId: result.reconciliationId,
    transactionId: result.transactionId,
    paymentEventId: result.paymentEventId,
    ledgerEventId: result.ledgerEventId,
    payment: result.paymentAmount == null ? null : `${result.paymentAmount} ${result.paymentCurrency}`,
    ledger: result.ledgerAmount == null ? null : `${result.ledgerAmount} ${result.ledgerCurrency}`,
    status: result.status,
    evaluatedAt: result.evaluatedAt,
  };
}

function showPendingWindow(transactionId) {
  elements.currentRunState.textContent = "Waiting for stream time to advance beyond the join window.";
  elements.eventLedger.className = "event-ledger is-active";
  elements.eventLedger.innerHTML = `
    <span>WINDOW OPEN</span>
    <p>${escapeHtml(transactionId)} is intentionally unmatched. Run another transaction after the 10s window to advance event time and emit the missing-side outcome.</p>
  `;
  setTraceStep(
    "stream",
    "active",
    "One side is stored in the outer-join window. Kafka Streams uses event time, so another event must advance stream time past the window.",
    { transactionId, joinWindow: "10s", gracePeriod: "3s", state: "WAITING_FOR_COUNTERPART" },
  );
  elements.traceStatus.textContent = "WINDOW OPEN";
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
  elements.eventLedger.innerHTML = "<span>WAITING</span><p>The topology is ready for a transaction.</p>";
}

function resetTrace() {
  elements.processingTrace.querySelectorAll("[data-trace]").forEach((step) => {
    const key = step.dataset.trace;
    step.className = "trace-step";
    step.querySelector("p").textContent = traceDefaults[key];
    step.querySelector(".trace-state").textContent = "WAITING";
    const payload = step.querySelector("pre");
    payload.hidden = true;
    payload.textContent = "";
  });
  elements.traceStatus.textContent = "READY";
}

function setTraceStep(key, status, detail, payload) {
  const step = elements.processingTrace.querySelector(`[data-trace="${key}"]`);
  step.className = `trace-step is-${status}`;
  step.querySelector("p").textContent = detail;
  step.querySelector(".trace-state").textContent = status === "review" ? "REVIEW" : status.toUpperCase();
  const payloadElement = step.querySelector("pre");
  if (payload !== undefined) {
    payloadElement.hidden = false;
    payloadElement.textContent = JSON.stringify(payload, null, 2);
  }
}

function markActiveTraceFailed(message) {
  const active = elements.processingTrace.querySelector(".is-active")
    || elements.processingTrace.querySelector("[data-trace='validate']");
  active.className = "trace-step is-review";
  active.querySelector("p").textContent = message;
  active.querySelector(".trace-state").textContent = "FAILED";
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
  const transactions = groupResultsByTransaction(state.results);
  const total = transactions.length;
  const matched = transactions.filter((transaction) => transaction.primary.status === "MATCHED").length;
  const flagged = transactions.filter((transaction) => transaction.hasReview).length;
  elements.metricTotal.textContent = String(total);
  elements.metricMatched.textContent = String(matched);
  elements.metricReview.textContent = String(flagged);
  elements.metricRate.textContent = total ? `${Math.round((matched / total) * 100)}%` : "—";
}

function renderTable() {
  const filtered = groupResultsByTransaction(state.results).filter((transaction) => {
    if (state.filter === "matched") return transaction.primary.status === "MATCHED";
    if (state.filter === "review") return transaction.hasReview;
    return true;
  });

  if (!filtered.length) {
    elements.resultTable.innerHTML = '<tr class="empty-row"><td colspan="6">No reconciliation events match this view.</td></tr>';
    return;
  }

  elements.resultTable.innerHTML = filtered.map((transaction) => {
    const result = transaction.primary;
    const matched = result.status === "MATCHED";
    const auditCount = transaction.auditEvents.length;
    const auditOutcomes = transaction.auditEvents.map((auditEvent) => `
      <span class="table-status is-audit">${escapeHtml(auditLabel(auditEvent))}</span>
    `).join("");
    const auditReasons = transaction.auditEvents.map((auditEvent) => `
      <div class="table-audit-note">
        <strong>${escapeHtml(auditLabel(auditEvent))}</strong>
        <span>${escapeHtml(auditEvent.reason)}</span>
      </div>
    `).join("");

    return `
      <tr>
        <td>${escapeHtml(formatTime(transaction.latestEvaluatedAt))}</td>
        <td>
          <span class="table-transaction-id">${escapeHtml(transaction.transactionId)}</span>
          ${auditCount ? `<span class="table-audit-count">${auditCount} audit event${auditCount === 1 ? "" : "s"}</span>` : ""}
        </td>
        <td>
          <div class="table-outcomes">
            <span class="table-status ${matched ? "is-matched" : "is-review"}">${escapeHtml(result.status.replaceAll("_", " "))}</span>
            ${auditOutcomes}
          </div>
        </td>
        <td>${escapeHtml(formatAmount(result.paymentAmount, result.paymentCurrency))}</td>
        <td>${escapeHtml(formatAmount(result.ledgerAmount, result.ledgerCurrency))}</td>
        <td class="table-reason">
          <span>${escapeHtml(result.reason)}</span>
          ${auditReasons}
        </td>
      </tr>
    `;
  }).join("");
}

function groupResultsByTransaction(results) {
  const groups = new Map();

  results.forEach((result) => {
    const transactionId = result.transactionId || result.reconciliationId;
    if (!groups.has(transactionId)) groups.set(transactionId, []);
    groups.get(transactionId).push(result);
  });

  return [...groups.entries()].map(([transactionId, transactionResults]) => {
    const orderedResults = [...transactionResults].sort(
      (left, right) => resultTimestamp(right) - resultTimestamp(left),
    );
    const primary = orderedResults.find((result) => !isDuplicateResult(result)) || orderedResults[0];

    return {
      transactionId,
      primary,
      auditEvents: orderedResults.filter((result) => result !== primary),
      latestEvaluatedAt: orderedResults[0].evaluatedAt,
      hasReview: orderedResults.some((result) => result.status !== "MATCHED"),
    };
  }).sort((left, right) => resultTimestamp(null, right.latestEvaluatedAt)
    - resultTimestamp(null, left.latestEvaluatedAt));
}

function isDuplicateResult(result) {
  return duplicateStatuses.has(result.status);
}

function auditLabel(result) {
  if (result.status === "DUPLICATE_PAYMENT") return "Duplicate payment ignored";
  if (result.status === "DUPLICATE_LEDGER_ENTRY") return "Duplicate ledger entry ignored";
  return result.status.replaceAll("_", " ");
}

function resultTimestamp(result, fallback) {
  return new Date(result?.evaluatedAt || fallback || 0).getTime();
}

function setControlsDisabled(disabled, activeRun) {
  elements.scenarioGrid.querySelectorAll("[data-scenario]").forEach((button) => {
    button.disabled = disabled;
    button.classList.toggle("is-running", button.dataset.scenario === activeRun);
  });
  elements.transactionForm.querySelectorAll("button, input, select").forEach((control) => {
    control.disabled = disabled;
  });
  if (!disabled) syncEventEditors();
}

function syncEventEditors() {
  const paymentEnabled = elements.publishPayment.checked;
  const ledgerEnabled = elements.publishLedger.checked;
  elements.paymentEditor.classList.toggle("is-disabled", !paymentEnabled);
  elements.ledgerEditor.classList.toggle("is-disabled", !ledgerEnabled);
  elements.paymentInputAmount.disabled = !paymentEnabled;
  elements.paymentInputCurrency.disabled = !paymentEnabled;
  elements.ledgerInputAmount.disabled = !ledgerEnabled;
  elements.ledgerInputCurrency.disabled = !ledgerEnabled;
  elements.duplicatePaymentInput.disabled = !paymentEnabled;
  if (!paymentEnabled) elements.duplicatePaymentInput.checked = false;
}

function generateTransactionId() {
  const randomPart = crypto.getRandomValues(new Uint32Array(1))[0].toString(16).slice(0, 6).toUpperCase();
  elements.transactionIdInput.value = `DEMO-${randomPart}`;
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

elements.transactionForm.addEventListener("submit", runCustomTransaction);
elements.regenerateTransaction.addEventListener("click", generateTransactionId);
elements.publishPayment.addEventListener("change", syncEventEditors);
elements.publishLedger.addEventListener("change", syncEventEditors);

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

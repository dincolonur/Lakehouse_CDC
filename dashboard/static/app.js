const $ = (sel) => document.querySelector(sel);

let refreshTimer = null;

function setPill(id, ok, label, warn = false) {
  const el = $(id);
  el.textContent = label;
  el.classList.remove("ok", "bad", "warn");
  el.classList.add(warn ? "warn" : ok ? "ok" : "bad");
}

function setStep(name, state, detail) {
  // state: "ok" | "bad" | "warn"
  const dot = document.querySelector(`#step-${name} .step-dot`);
  const detailEl = $(`#step-${name}-detail`);
  dot.classList.remove("ok", "bad", "warn");
  dot.classList.add(state);
  detailEl.textContent = detail;
  detailEl.title = detail;
}

function renderTable(tableEl, rows, columns) {
  const thead = tableEl.querySelector("thead");
  const tbody = tableEl.querySelector("tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";

  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${columns.length}" style="color:var(--muted)">No rows yet</td></tr>`;
    thead.innerHTML = `<tr>${columns.map((c) => `<th>${c}</th>`).join("")}</tr>`;
    return;
  }

  thead.innerHTML = `<tr>${columns.map((c) => `<th>${c}</th>`).join("")}</tr>`;
  tbody.innerHTML = rows
    .map((row) => {
      const op = row.operation_type ? ` class="op-${row.operation_type}"` : "";
      return `<tr${op}>${columns.map((c) => `<td>${fmt(row[c])}</td>`).join("")}</tr>`;
    })
    .join("");
}

function fmt(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "number") return Number.isInteger(v) ? v : v.toFixed(2);
  return String(v).replace("T", " ").slice(0, 23);
}

async function refreshHealth() {
  try {
    const r = await fetch("/api/health");
    const data = await r.json();

    setStep("postgres", data.postgres_up ? "ok" : "bad", data.postgres_up ? "up" : "down");

    const cs = data.kafka_connect || {};
    if (!cs.reachable) {
      setStep("connect", "bad", "unreachable");
    } else if (cs.connector_state === "RUNNING") {
      setStep("connect", "ok", "RUNNING");
    } else {
      setStep("connect", "warn", cs.connector_state || "unknown");
    }

    const kafka = data.kafka || {};
    if (!kafka.reachable) {
      setStep("kafka", "bad", "unreachable");
    } else if (kafka.topic_exists) {
      setStep("kafka", "ok", "topic ready");
    } else {
      setStep("kafka", "warn", "no topic yet");
    }

    const streamer = data.spark_streamer || {};
    if (!streamer.reachable) {
      setStep("streamer", "bad", "unreachable");
    } else if (streamer.status === "running") {
      setStep("streamer", "ok", "running");
    } else {
      setStep("streamer", "warn", streamer.status || "unknown");
    }

    setStep(
      "silver-pipe",
      data.silver_table_exists ? "ok" : "warn",
      data.silver_table_exists ? `${data.silver_count ?? "?"} raw events` : "not created yet"
    );
    setStep(
      "gold-pipe",
      data.silver_table_exists ? "ok" : "warn",
      data.silver_table_exists ? `${data.gold_count ?? "?"} accounts` : "waiting on silver"
    );
  } catch (e) {
    ["postgres", "connect", "kafka", "streamer", "silver-pipe", "gold-pipe"].forEach((s) =>
      setStep(s, "bad", "dashboard unreachable")
    );
  }
}

async function refreshSource() {
  try {
    const r = await fetch("/api/source");
    const data = await r.json();
    $("#source-count").textContent = `(${data.rows.length} accounts)`;
    renderTable($("#source-table"), data.rows, [
      "account_id", "current_balance", "currency", "transaction_timestamp", "updated_at",
    ]);
  } catch (e) {
    $("#source-count").textContent = "(unavailable)";
  }
}

async function refreshSilver() {
  try {
    const r = await fetch("/api/silver?limit=100");
    const data = await r.json();
    $("#silver-count").textContent = `(${data.total_count} raw events, showing latest ${data.rows.length})`;
    renderTable($("#silver-table"), data.rows, [
      "account_id", "operation_type", "current_balance", "source_commit_lsn",
      "kafka_event_offset", "ingested_at",
    ]);
  } catch (e) {
    $("#silver-count").textContent = "(unavailable)";
  }
}

async function refreshGold() {
  try {
    const r = await fetch("/api/gold");
    const data = await r.json();
    $("#gold-count").textContent = `(${data.rows.length} current accounts)`;
    renderTable($("#gold-table"), data.rows, [
      "account_id", "current_balance", "currency", "source_commit_lsn", "transaction_timestamp",
    ]);
  } catch (e) {
    $("#gold-count").textContent = "(unavailable)";
  }
}

async function refreshAll() {
  await Promise.all([refreshHealth(), refreshSource(), refreshSilver(), refreshGold()]);
  $("#last-refresh").textContent = new Date().toLocaleTimeString();
}

function scheduleAutoRefresh() {
  if (refreshTimer) clearInterval(refreshTimer);
  if (!$("#auto-refresh").checked) return;
  const ms = parseInt($("#refresh-interval").value, 10);
  refreshTimer = setInterval(refreshAll, ms);
}

$("#refresh-now").addEventListener("click", refreshAll);
$("#auto-refresh").addEventListener("change", scheduleAutoRefresh);
$("#refresh-interval").addEventListener("change", scheduleAutoRefresh);

$("#action-select").addEventListener("change", (e) => {
  $("#balance-field").style.display = e.target.value === "delete" ? "none" : "flex";
});

$("#mutate-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const form = e.target;
  const body = {
    action: form.action.value,
    account_id: parseInt(form.account_id.value, 10),
  };
  if (form.action.value !== "delete") {
    body.balance = parseFloat(form.balance.value);
  }
  const resultEl = $("#mutate-result");
  resultEl.textContent = "Submitting…";
  resultEl.className = "result";
  try {
    const r = await fetch("/api/mutate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await r.json();
    if (r.ok) {
      resultEl.textContent = data.message;
      resultEl.className = "result ok";
      setTimeout(refreshAll, 300);
    } else {
      resultEl.textContent = data.detail || "Error";
      resultEl.className = "result bad";
    }
  } catch (err) {
    resultEl.textContent = String(err);
    resultEl.className = "result bad";
  }
});

$("#generate-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const events = parseInt(e.target.events.value, 10);
  const resultEl = $("#generate-result");
  resultEl.textContent = "Generating…";
  resultEl.className = "result";
  try {
    const r = await fetch("/api/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ events }),
    });
    const data = await r.json();
    if (r.ok) {
      resultEl.textContent = `${data.mutations.length} mutations committed to Postgres.`;
      resultEl.className = "result ok";
      setTimeout(refreshAll, 300);
    } else {
      resultEl.textContent = data.detail || "Error";
      resultEl.className = "result bad";
    }
  } catch (err) {
    resultEl.textContent = String(err);
    resultEl.className = "result bad";
  }
});

async function runAction(button, url) {
  const log = $("#action-log");
  log.hidden = false;
  button.disabled = true;
  log.textContent = `Running ${url} ... this can take up to ~30s the first time.`;
  try {
    const r = await fetch(url, { method: "POST" });
    const data = await r.json();
    log.textContent =
      `exit_code=${data.exit_code}  duration=${data.duration_seconds}s\n\n` +
      `--- stdout (tail) ---\n${data.stdout_tail || "(empty)"}\n\n` +
      `--- stderr (tail) ---\n${data.stderr_tail || "(empty)"}`;
    setTimeout(refreshAll, 500);
  } catch (err) {
    log.textContent = "Failed: " + err;
  } finally {
    button.disabled = false;
  }
}

$("#btn-compact").addEventListener("click", (e) => runAction(e.target, "/api/actions/compact"));
$("#btn-gold-view").addEventListener("click", (e) => runAction(e.target, "/api/actions/refresh-spark-gold-view"));

refreshAll();
scheduleAutoRefresh();

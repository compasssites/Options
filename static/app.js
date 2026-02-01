const SYMBOL_SELECT = document.getElementById("symbol");
const EXPIRY_SELECT = document.getElementById("expiry");
const STRIKE_STEP = document.getElementById("strikeStep");
const FETCH_BTN = document.getElementById("fetchBtn");
const REFRESH_BTN = document.getElementById("refreshBtn");
const EXPORT_BTN = document.getElementById("exportBtn");
const COPY_BTN = document.getElementById("copyBtn");
const AUTO_REFRESH = document.getElementById("autoRefresh");
const STATUS = document.getElementById("status");
const LAST_UPDATED = document.getElementById("lastUpdated");
const ROW_COUNT = document.getElementById("rowCount");
const CACHE_STATUS = document.getElementById("cacheStatus");
const TABLE = document.getElementById("chainTable");
const EXCHANGE_PILL = document.getElementById("exchangePill");

const STRIKE_DEFAULTS = {
  NIFTY: "50",
  SILVERM: "5000",
  GOLDM: "5000",
};

const HEADERS = [
  "CALL_OI_Lots",
  "CALL_Chng_in_OI",
  "CALL_Volume",
  "CALL_Abs_Chng",
  "CALL_Bid_Qty",
  "CALL_Bid_Price",
  "CALL_Ask_Price",
  "CALL_Ask_Qty",
  "CALL_LTP",
  "Strike_Price",
  "PUT_LTP",
  "PUT_Bid_Qty",
  "PUT_Bid_Price",
  "PUT_Ask_Price",
  "PUT_Ask_Qty",
  "PUT_Abs_Chng",
  "PUT_Volume",
  "PUT_Chng_in_OI",
  "PUT_OI_Lots",
];

let autoRefreshTimer = null;
let tokenValue = "";
let symbolSources = {};

function setStatus(message, tone = "neutral") {
  STATUS.textContent = message;
  STATUS.dataset.tone = tone;
}

function getToken() {
  return tokenValue;
}

function buildParams(extra = {}) {
  const symbol = SYMBOL_SELECT.value;
  const expiry = EXPIRY_SELECT.value || "";
  const stepValue = STRIKE_STEP.value;
  const allStrikes = false;

  const params = {
    symbol,
    expiry,
    all_strikes: allStrikes ? "true" : "false",
    strike_step: stepValue,
    ...extra,
  };

  const token = getToken();
  if (token) {
    params.token = token;
  }

  return params;
}

function toQuery(params) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      query.append(key, value);
    }
  });
  return query.toString();
}

async function fetchJson(path, params = {}) {
  const query = toQuery(params);
  const url = query ? `${path}?${query}` : path;
  const response = await fetch(url, {
    headers: tokenHeader(),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || response.statusText);
  }

  return response.json();
}

function tokenHeader() {
  const token = getToken();
  return token ? { "X-API-Token": token } : {};
}

async function loadSymbols() {
  setStatus("Loading symbols...");
  try {
    const data = await fetchJson("/api/symbols");
    SYMBOL_SELECT.innerHTML = "";
    symbolSources = data.sources || {};
    data.symbols.forEach((symbol) => {
      const option = document.createElement("option");
      option.value = symbol;
      option.textContent = symbol;
      SYMBOL_SELECT.appendChild(option);
    });
    let selected = "";
    if (data.symbols.length === 0) {
      setStatus("No symbols configured yet.", "warning");
    } else {
      selected = data.symbols.includes("SILVERM") ? "SILVERM" : data.symbols[0];
      SYMBOL_SELECT.value = selected;
      applySymbolDefaults(selected);
      setStatus("Symbols loaded.");
    }
    return selected;
  } catch (error) {
    setStatus(`Symbol load failed: ${error.message}`, "error");
    return "";
  }
}

async function loadExpiries(symbol) {
  setStatus("Loading expiries...");
  try {
    const data = await fetchJson("/api/expiries", { symbol });
    EXPIRY_SELECT.innerHTML = "";
    data.expiries.forEach((expiry) => {
      const option = document.createElement("option");
      option.value = expiry;
      option.textContent = expiry;
      EXPIRY_SELECT.appendChild(option);
    });
    if (data.expiries.length === 0) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "No expiries";
      EXPIRY_SELECT.appendChild(option);
      EXPIRY_SELECT.disabled = true;
    } else {
      EXPIRY_SELECT.disabled = false;
    }
    setStatus("Expiries loaded.");
  } catch (error) {
    setStatus(`Expiry load failed: ${error.message}`, "error");
  }
}

function applyStrikeStep(symbol) {
  const key = (symbol || "").toUpperCase();
  const target = STRIKE_DEFAULTS[key] || "5000";
  STRIKE_STEP.value = target;
}

function applyExchangePill(symbol) {
  if (!EXCHANGE_PILL) {
    return;
  }
  const key = (symbol || "").toUpperCase();
  const source = symbolSources[key] || (key === "NIFTY" ? "nse" : "mcx");
  EXCHANGE_PILL.textContent = source.toUpperCase();
}

function applySymbolDefaults(symbol) {
  applyStrikeStep(symbol);
  applyExchangePill(symbol);
}

function updateTable(rows) {
  const thead = TABLE.querySelector("thead");
  const tbody = TABLE.querySelector("tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";

  const headerRow = document.createElement("tr");
  HEADERS.forEach((header) => {
    const th = document.createElement("th");
    th.textContent = header.replace(/_/g, " ");
    applyColumnClasses(th, header);
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  rows.forEach((row) => {
    const tr = document.createElement("tr");
    HEADERS.forEach((header) => {
      const td = document.createElement("td");
      td.textContent = row[header] ?? "";
      applyColumnClasses(td, header);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

function applyColumnClasses(cell, header) {
  if (header.startsWith("CALL_")) {
    cell.classList.add("call-col");
  }
  if (header.startsWith("PUT_")) {
    cell.classList.add("put-col");
  }
  if (header === "Strike_Price") {
    cell.classList.add("strike-col");
  }
  if (header === "CALL_LTP" || header === "PUT_LTP" || header === "Strike_Price") {
    cell.classList.add("ltp-col");
  }
}

async function loadData({ force = false } = {}) {
  setStatus(force ? "Refreshing data..." : "Fetching data...");
  CACHE_STATUS.textContent = force ? "Refreshing" : "Fetching";

  const params = buildParams({ format: "json", force: force ? "true" : "false" });

  try {
    const data = await fetchJson("/api/option-chain", params);
    updateTable(data.rows || []);
    LAST_UPDATED.textContent = data.last_updated ? `Last updated ${data.last_updated}` : "No timestamp";
    ROW_COUNT.textContent = data.count || 0;
    CACHE_STATUS.textContent = force ? "Fresh" : "Cached";
    setStatus("Data ready.");
  } catch (error) {
    setStatus(`Fetch failed: ${error.message}`, "error");
    CACHE_STATUS.textContent = "Error";
  }
}

function exportCsv() {
  const params = buildParams({ format: "csv", download: "1" });
  const url = `/api/option-chain?${toQuery(params)}`;
  window.open(url, "_blank");
}

async function copyLink() {
  const params = buildParams({ format: "json", window: "10" });
  const url = `${window.location.origin}/api/option-chain-chat?${toQuery(params)}`;
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(url);
      setStatus("Live chat-friendly link copied.");
      return;
    }
    const textarea = document.createElement("textarea");
    textarea.value = url;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "absolute";
    textarea.style.left = "-9999px";
    document.body.appendChild(textarea);
    textarea.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(textarea);
    if (ok) {
      setStatus("Live chat-friendly link copied.");
      return;
    }
    throw new Error("Clipboard blocked");
  } catch (error) {
    setStatus(`Copy failed. ${url}`, "warning");
  }
}

function scheduleAutoRefresh() {
  if (autoRefreshTimer) {
    clearInterval(autoRefreshTimer);
  }
  if (!AUTO_REFRESH.checked) {
    return;
  }
  autoRefreshTimer = setInterval(() => {
    loadData({ force: false });
  }, 10 * 60 * 1000);
}

function initToken() {
  const params = new URLSearchParams(window.location.search);
  const urlToken = params.get("token");
  if (urlToken) {
    tokenValue = urlToken;
    localStorage.setItem("option_chain_token", urlToken);
    return;
  }
  tokenValue = localStorage.getItem("option_chain_token") || "";
}

FETCH_BTN.addEventListener("click", () => loadData({ force: false }));
REFRESH_BTN.addEventListener("click", () => loadData({ force: true }));
EXPORT_BTN.addEventListener("click", exportCsv);
COPY_BTN.addEventListener("click", copyLink);
AUTO_REFRESH.addEventListener("change", scheduleAutoRefresh);

SYMBOL_SELECT.addEventListener("change", async () => {
  applySymbolDefaults(SYMBOL_SELECT.value);
  await loadExpiries(SYMBOL_SELECT.value);
});

window.addEventListener("load", async () => {
  initToken();
  const selected = await loadSymbols();
  if (selected) {
    await loadExpiries(selected);
  }
  scheduleAutoRefresh();
});

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/sw.js").catch(() => {});
  });
}

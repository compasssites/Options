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
const TICKER_BAR = document.getElementById("tickerBar");
const TICKER_STATUS = document.getElementById("tickerStatus");
const TICKER_ITEMS = {
  gold: {
    price: document.getElementById("tickerGoldPrice"),
    change: document.getElementById("tickerGoldChange"),
    unit: document.getElementById("tickerGoldUnit"),
  },
  silver: {
    price: document.getElementById("tickerSilverPrice"),
    change: document.getElementById("tickerSilverChange"),
    unit: document.getElementById("tickerSilverUnit"),
  },
};

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
  "CALL_Prev_Close",
  "CALL_Pct_Chng",
  "Strike_Price",
  "PUT_LTP",
  "PUT_Prev_Close",
  "PUT_Pct_Chng",
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
let tickerTimer = null;

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

function formatTickerNumber(value, decimals = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return String(value);
  }
  return num.toLocaleString(undefined, { maximumFractionDigits: decimals });
}

function setTickerTone(element, value) {
  if (!element) {
    return;
  }
  const num = Number(value);
  if (!Number.isFinite(num) || num === 0) {
    element.dataset.tone = "flat";
    return;
  }
  element.dataset.tone = num > 0 ? "up" : "down";
}

function setPercentTone(element, value) {
  if (!element) {
    return;
  }
  const num = Number(value);
  if (!Number.isFinite(num) || num === 0) {
    element.dataset.tone = "flat";
    return;
  }
  element.dataset.tone = num > 0 ? "up" : "down";
}

function parseErrorDetail(message) {
  if (!message) {
    return "Ticker unavailable";
  }
  const trimmed = message.trim();
  if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
    try {
      const payload = JSON.parse(trimmed);
      if (payload && payload.detail) {
        return payload.detail;
      }
    } catch {
      return message;
    }
  }
  return message;
}

function updateTickerItem(item) {
  const name = (item?.name || "").toLowerCase();
  const target = name === "gold" ? TICKER_ITEMS.gold : name === "silver" ? TICKER_ITEMS.silver : null;
  if (!target) {
    return;
  }

  const last = item?.last ?? "";
  const change = item?.change ?? "";
  const changePct = item?.change_pct ?? "";

  if (target.price) {
    target.price.textContent = formatTickerNumber(last, 2);
  }

  const changeNum = Number(change);
  const changePrefix = Number.isFinite(changeNum) && changeNum > 0 ? "+" : "";
  const changeText = change !== "" ? `${changePrefix}${formatTickerNumber(change, 2)}` : "";
  const pctText = changePct !== "" ? `${changePrefix}${formatTickerNumber(changePct, 2)}%` : "";
  if (target.change) {
    target.change.textContent = [changeText, pctText].filter(Boolean).join(" ");
    setTickerTone(target.change, change);
  }

  if (target.unit) {
    target.unit.textContent = item?.unit || "";
  }
}

async function loadTicker() {
  if (!TICKER_BAR) {
    return;
  }
  try {
    const data = await fetchJson("/api/ticker");
    const items = Array.isArray(data?.items) ? data.items : [];
    if (items.length === 0) {
      if (TICKER_STATUS) {
        TICKER_STATUS.textContent = "Ticker unavailable";
      }
      return;
    }
    items.forEach((item) => updateTickerItem(item));
    if (TICKER_STATUS) {
      const source = data?.source ? String(data.source).toUpperCase() : "";
      const suffix = source ? ` · ${source}` : "";
      TICKER_STATUS.textContent = data?.last_updated ? `Updated ${data.last_updated}${suffix}` : `Updated${suffix}`;
    }
  } catch (error) {
    if (TICKER_STATUS) {
      TICKER_STATUS.textContent = parseErrorDetail(error.message || "");
    }
  }
}

function scheduleTicker() {
  if (!TICKER_BAR) {
    return;
  }
  if (tickerTimer) {
    clearInterval(tickerTimer);
  }
  tickerTimer = setInterval(() => {
    loadTicker();
  }, 60 * 1000);
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
      if (header.endsWith("Pct_Chng")) {
        setPercentTone(td, row[header]);
      }
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  requestAnimationFrame(() => centerKeyColumns());
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
  if (header === "PUT_LTP") {
    cell.classList.add("put-ltp-col");
  }
  if (header === "CALL_LTP") {
    cell.classList.add("call-ltp-col");
  }
  if (header === "CALL_LTP" || header === "PUT_LTP" || header === "Strike_Price") {
    cell.classList.add("ltp-col");
  }
  if (header.endsWith("Pct_Chng")) {
    cell.classList.add("pct-col");
  }
}

function centerKeyColumns() {
  if (window.innerWidth > 960) {
    return;
  }
  const container = document.querySelector(".table-wrap");
  if (!container) {
    return;
  }
  const strike = container.querySelector("th.strike-col");
  const put = container.querySelector("th.put-ltp-col");
  if (!strike || !put) {
    return;
  }

  const containerRect = container.getBoundingClientRect();
  const strikeRect = strike.getBoundingClientRect();
  const putRect = put.getBoundingClientRect();

  const strikeCenter = strikeRect.left - containerRect.left + container.scrollLeft + strikeRect.width / 2;
  const putCenter = putRect.left - containerRect.left + container.scrollLeft + putRect.width / 2;
  const targetCenter = (strikeCenter + putCenter) / 2;

  const desiredScrollLeft = Math.max(0, targetCenter - container.clientWidth / 2);
  container.scrollLeft = desiredScrollLeft;
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
  const params = buildParams({ format: "json", pretty: "1", as_text: "1", window: "60" });
  const url = `${window.location.origin}/api/option-chain?${toQuery(params)}`;
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(url);
      setStatus("Pretty JSON link copied.");
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
      setStatus("Pretty JSON link copied.");
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
  await loadTicker();
  scheduleTicker();
});

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/sw.js").catch(() => {});
  });
}

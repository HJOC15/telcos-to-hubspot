// src/providers/claro.js
import "dotenv/config";
import path from "path";
import { createRequire } from "module";
import axios from "axios";
import crypto from "crypto";

const DEBUG = String(process.env.DEBUG_CLARO || "").toLowerCase() === "1";

/* =========================
   UTILIDADES COMPARTIDAS
   ========================= */
function toISO(d) { return new Date(d).toISOString(); }
function rfc1123(date, noComma = false) {
  const s = new Date(date).toUTCString();
  return noComma ? s.replace(",", "") : s;
}
function buildParamString(paramsObj) {
  const entries = Object.entries(paramsObj)
    .filter(([_, v]) => v !== undefined && v !== null && String(v) !== "");
  entries.sort(([a], [b]) => a.localeCompare(b)); // orden por key
  return entries.map(([k, v]) =>
    `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`
  ).join("&");
}
function sign({ apiKey, apiSecret, dateHeader, paramString, pathPart = "" }) {
  const toSign = `${apiKey}${dateHeader}${pathPart}${paramString}`;
  return crypto.createHmac("sha1", apiSecret).update(toSign).digest("base64");
}
function getBase() {
  const raw = process.env.CLARO_BASE_URL || "";
  if (!raw) throw new Error("Falta CLARO_BASE_URL en .env");
  return raw.replace(/\/+$/, "");
}

const PKG_CANDIDATES = ["im-contactosms-sdk-js","im-csms-sdk-javascript-v4"];

function pathToFileUrl(p) {
  const { URL } = globalThis;
  const url = new URL("file:///" + p.replace(/\\/g, "/"));
  return url.href;
}

async function loadSmsApiClass() {
  // ESM por ruta directa
  for (const base of PKG_CANDIDATES) {
    try {
      const mod = await import(`${base}/src/sdk/SmsApi.js`);
      const SmsApi = mod?.SmsApi || mod?.default;
      if (SmsApi) return SmsApi;
    } catch {}
  }
  // CJS resolve + import file://
  const req = createRequire(import.meta.url);
  for (const base of PKG_CANDIDATES) {
    try {
      const mainPath = req.resolve(base);
      const pkgDir = path.dirname(mainPath);
      const smsPath = path.join(pkgDir, "src", "sdk", "SmsApi.js");
      const mod = await import(pathToFileUrl(smsPath));
      const SmsApi = mod?.SmsApi || mod?.default;
      if (SmsApi) return SmsApi;
    } catch {}
  }
  return null;
}

async function trySdkMessages({ start, end, limit }) {
  const SmsApi = await loadSmsApiClass();
  if (!SmsApi) return { ok: false, reason: "SDK_NOT_FOUND" };

  const apiKey    = process.env.CLARO_API_KEY;
  const apiSecret = process.env.CLARO_API_SECRET;
  const baseUrl   = process.env.CLARO_BASE_URL || process.env.URL;
  if (!apiKey || !apiSecret || !baseUrl) throw new Error("Faltan CLARO_API_KEY/CLARO_API_SECRET/CLARO_BASE_URL");

  const startISO = toISO(start);
  const endISO   = toISO(end);
  const startRFC = rfc1123(start, false);
  const endRFC   = rfc1123(end,   false);

  const api = new SmsApi(apiKey, apiSecret, baseUrl);
  const params = {
    startDate: startISO,
    endDate: endISO,
    start_date: startRFC,
    end_date: endRFC,
    delivery_status_enable: true,
    direction: "MT",
    limit
  };

  try {
    const resp = await api.messages.listMessages(params);
    const payload = resp?.data ?? resp?.items ?? resp ?? [];
    const arr = Array.isArray(payload) ? payload : (Array.isArray(payload?.items) ? payload.items : []);
    if (DEBUG) console.log("[CLARO][SDK][OK] count=", arr?.length ?? 0);
    return { ok: true, data: Array.isArray(arr) ? arr : [] };
  } catch (e) {
    const status = e?.response?.status;
    const data = e?.response?.data;
    if (DEBUG) console.warn("[CLARO][SDK][ERR]", { status, data, message: e?.message });
    return { ok: false, status, data, reason: "SDK_CALL_FAILED" };
  }
}

/* =========================
   2) INTENTO MANUAL FIRMANDO
   ========================= */
async function tryManualMessages({ start, end, limit }) {
  const apiKey = process.env.CLARO_API_KEY;
  const apiSecret = process.env.CLARO_API_SECRET;
  if (!apiKey || !apiSecret) throw new Error("Faltan CLARO_API_KEY/CLARO_API_SECRET");

  const base = getBase();
  const bases = [`${base}/messages`, `${base}/mensajes`];

  const startRFC = rfc1123(start, false);
  const endRFC   = rfc1123(end, false);
  const startRFCnc = rfc1123(start, true);
  const endRFCnc   = rfc1123(end, true);

  const variants = [
    { params: { start_date: startRFC,   end_date: endRFC,   limit }, includePathInSig: false, dateNoComma: false, label:"noPath+comma" },
    { params: { start_date: startRFC,   end_date: endRFC,   limit }, includePathInSig: true,  dateNoComma: false, label:"withPath+comma" },
    { params: { start_date: startRFCnc, end_date: endRFCnc, limit }, includePathInSig: false, dateNoComma: true,  label:"noPath+noComma" },
    { params: { start_date: startRFCnc, end_date: endRFCnc, limit }, includePathInSig: true,  dateNoComma: true,  label:"withPath+noComma" },
  ];

  for (const basePath of bases) {
    for (const t of variants) {
      const paramString = buildParamString(t.params);
      const url = `${basePath}?${paramString}`;

      const dateHeader = t.dateNoComma ? String(t.params.end_date).replace(",", "") : String(t.params.end_date);
      const pathOnly = new URL(basePath).pathname;

      const signature = sign({
        apiKey, apiSecret,
        dateHeader,
        paramString,
        pathPart: t.includePathInSig ? pathOnly : ""
      });

      const headers = {
        Date: dateHeader,
        Authorization: `IM ${apiKey}:${signature}`,
        "Content-Type": "application/json; charset=utf-8",
        "X-IM-ORIGIN": "IM_SDK_JAVASCRIPT_V4",
      };

      try {
        if (DEBUG) console.log("[CLARO][MANUAL][REQ]", { url, variant: t.label });
        const { data, status } = await axios.get(url, { headers, timeout: 30000 });
        if (DEBUG) console.log("[CLARO][MANUAL][RES]", { status, sample: Array.isArray(data) ? data.slice(0, 1) : data });
        const arr = Array.isArray(data?.items || data) ? (data.items || data) : [];
        return Array.isArray(arr) ? arr : [];
      } catch (e) {
        const status = e?.response?.status;
        if (DEBUG) console.warn("[CLARO][MANUAL][ERR]", { status, data:e?.response?.data, label:t.label });
        if (status === 401 || status === 403) { throw e?.response?.data || { code: status, error: "No autorizado" }; }
      }
    }
  }
  return [];
}

/* =========================
   API pública para el job
   ========================= */
export async function claroListMessages({ limit = 500, days } = {}) {
  // 1) Si el server setea una ventana puntual, úsala
  const envStart = (process.env.CLARO_START_DATE || process.env.EXTRACT_START_DATE || "").trim();
  const envEnd   = (process.env.CLARO_END_DATE   || process.env.EXTRACT_END_DATE   || "").trim();

  const hasWindow =
    /^\d{4}-\d{2}-\d{2}$/.test(envStart) &&
    /^\d{4}-\d{2}-\d{2}$/.test(envEnd);

  let start, end;

  if (hasWindow) {
    // Interpretación: [inicio 00:00:00, fin 23:59:59] hora local
    start = new Date(`${envStart}T00:00:00`);
    end   = new Date(`${envEnd}T23:59:59`);
    if (DEBUG) console.log(`[CLARO] modo=puntual start=${start.toISOString()} end=${end.toISOString()}`);
  } else {
    // 2) Modo recurrente por "days" como hoy
    const d = Number(days ?? process.env.CLARO_MESSAGES_DAYS ?? 30);
    end = new Date();
    start = new Date(Date.now() - d * 24 * 60 * 60 * 1000);
    if (DEBUG) console.log(`[CLARO] modo=recurrente days=${d} start=${start.toISOString()} end=${end.toISOString()}`);
  }

  // 3) SDK primero
  const viaSdk = await trySdkMessages({ start, end, limit });
  if (viaSdk.ok) return viaSdk.data;

  // 4) Fallback manual firmado
  const manual = await tryManualMessages({ start, end, limit });
  return manual;
}


/* ==================================================
   CONTACTOS (derivados de mensajes) para CLARO
   - Genera una lista de "contactos" a partir de los msisdn
   - Útil para alimentar src/jobs/sync.js (upsert a HubSpot)
   ================================================== */
export async function claroListContacts({ limit = 1000, days } = {}) {
  const d = Number(days ?? process.env.CLARO_CONTACTS_DAYS ?? process.env.CLARO_MESSAGES_DAYS ?? 30);

  // Pedimos más mensajes para dedupe cómodo
  const msgs = await claroListMessages({ limit: Math.max(limit, 2000), days: d });

  const onlyDigits = (s) => String(s || "").replace(/\D/g, "");

  function getNumber(m) {
    return (
      onlyDigits(m?.msisdn) ||
      onlyDigits(m?.msisdnTo) ||
      onlyDigits(m?.msisdn_from) ||
      onlyDigits(m?.phone) ||
      onlyDigits(m?.phoneNumber) ||
      onlyDigits(m?.to) ||
      ""
    );
  }

  const seen = new Set();
  const out = [];

  for (const m of Array.isArray(msgs) ? msgs : []) {
    const num = getNumber(m);
    if (!num) continue;
    if (seen.has(num)) continue;
    seen.add(num);

    out.push({
      msisdn: num,
      // Si tu payload de Claro incluye estos, los puedes mapear:
      // email: m.email || undefined,
      // name: m.name || m.fullName || undefined,
    });

    if (out.length >= limit) break;
  }

  if (DEBUG) {
    console.log(`[CLARO][CONTACTS] generados=${out.length} únicos a partir de mensajes (days=${d}, limit=${limit})`);
  }

  return out;
}

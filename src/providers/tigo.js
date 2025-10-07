// src/providers/tigo.js
import axios from "axios";

const BASE     = process.env.TIGO_B2B_BASE || "https://prod.api.tigo.com/v1";
const ORG_ID   = process.env.TIGO_B2B_ORG_ID;
const TOKEN    = process.env.TIGO_B2B_TOKEN;
const APIKEY   = process.env.TIGO_B2B_API_KEY;    // {{APIKey}} Postman
const APISEC   = process.env.TIGO_B2B_API_SECRET; // {{APISecret}} Postman

const DEBUG    = String(process.env.DEBUG_TIGO || "0") === "1";
const FORCE_SCHEME = (process.env.TIGO_PAGING_SCHEME || "").trim(); 
// ej: TIGO_PAGING_SCHEME=pageNumber_pageSize

function authHeaders() {
  if (!TOKEN) throw new Error("Falta TIGO_B2B_TOKEN en .env");
  const h = {
    Authorization: `Bearer ${TOKEN}`,
    Accept: "*/*",
    "Content-Type": "application/json",
  };
  if (APIKEY) h.APIKey = APIKEY;
  if (APISEC) h.APISecret = APISEC;
  return h;
}

// --- fecha utils ---
const pad = (n)=>String(n).padStart(2,"0");
const toISO = d => (d instanceof Date? d : new Date(d)).toISOString();
const toRFC = d => (d instanceof Date? d : new Date(d)).toUTCString();
function toYmdHms(d){
  const x = (d instanceof Date? d : new Date(d));
  const Y = x.getUTCFullYear(); const M = pad(x.getUTCMonth()+1);
  const D = pad(x.getUTCDate()); const h = pad(x.getUTCHours());
  const m = pad(x.getUTCMinutes()); const s = pad(x.getUTCSeconds());
  return `${Y}-${M}-${D} ${h}:${m}:${s}`;
}

// --- detecta/forza esquema de paginación ---
async function detectPagingScheme(url, baseParams, pageSize, headers) {
  const candidates = [
    { name: "page_size",            build: (i)=>({ page: i,        size: pageSize }) },
    { name: "pageNumber_pageSize",  build: (i)=>({ pageNumber: i,  pageSize }) },
    { name: "number_size",          build: (i)=>({ number: i,      size: pageSize }) },
    { name: "offset_size",          build: (i)=>({ offset: i*pageSize, size: pageSize }) },
  ];

  // Si se forzó por .env, úsalo directamente
  if (FORCE_SCHEME) {
    const forced = candidates.find(c => c.name === FORCE_SCHEME);
    if (!forced) throw new Error(`TIGO_PAGING_SCHEME inválido: ${FORCE_SCHEME}`);
    if (DEBUG) console.log(`[TIGO][PAGING FORZADO] ${FORCE_SCHEME}`);
    const params = { ...baseParams, ...forced.build(0) };
    const first = await axios.get(url, { headers, params });
    return { scheme: forced.name, first };
  }

  // Auto-detección
  let lastErr;
  for (const c of candidates) {
    try {
      const params = { ...baseParams, ...c.build(0) };
      const res = await axios.get(url, { headers, params });
      if (DEBUG) console.log(`[TIGO][PAGING OK] ${c.name} size=${pageSize} content=${Array.isArray(res.data?.content) ? res.data.content.length : 0}`);
      return { scheme: c.name, first: res };
    } catch (e) {
      lastErr = e;
      const status = e?.response?.status;
      const data = e?.response?.data;
      if (DEBUG) console.log(`[TIGO][PAGING FAIL] ${c.name} status=${status}`, typeof data === "string" ? data.slice(0,120) : data);
      if (status === 401 || status === 403) throw e;
    }
  }
  throw lastErr || new Error("No se pudo detectar esquema de paginación");
}

async function getAllPagedFlex(url, paramsBase = {}, { pageSize = 25, maxPages = 200 } = {}) {
  const headers = authHeaders();
  const out = [];

  const { scheme, first } = await detectPagingScheme(url, paramsBase, pageSize, headers);
  const getParams = (i) => {
    switch (scheme) {
      case "page_size":           return { page: i,        size: pageSize };
      case "pageNumber_pageSize": return { pageNumber: i,  pageSize };
      case "number_size":         return { number: i,      size: pageSize };
      case "offset_size":         return { offset: i*pageSize, size: pageSize };
      default:                    return { page: i, size: pageSize };
    }
  };

  const pushFrom = (res) => {
    const items = Array.isArray(res.data?.content) ? res.data.content : [];
    out.push(...items);
    return items;
  };
  let items = pushFrom(first);
  let page = 1;

  const isLast = (res, len) => {
    if (typeof res.data?.last === "boolean") return res.data.last;
    if (typeof res.data?.numberOfElements === "number") return res.data.numberOfElements < pageSize;
    return len < pageSize;
  };

  while (page < maxPages && items.length > 0) {
    const params = { ...paramsBase, ...getParams(page) };
    const res = await axios.get(url, { headers, params });
    items = pushFrom(res);
    if (isLast(res, items.length)) break;
    page += 1;
  }

  return out;
}

// === CONTACTOS ===
export async function tigoListContacts({ pageSize = Number(process.env.TIGO_PAGE_SIZE || 100), maxPages = 200 } = {}) {
  if (!ORG_ID) throw new Error("Falta TIGO_B2B_ORG_ID en .env");
  const url = `${BASE}/tigo/b2b/gt/comcorp/contacts/organizations/${ORG_ID}`;
  return getAllPagedFlex(url, {}, { pageSize, maxPages });
}

// === MENSAJES ===
async function getMessagesWithParams(baseParams, { pageSize, maxPages }) {
  if (!ORG_ID) throw new Error("Falta TIGO_B2B_ORG_ID en .env");
  const url = `${BASE}/tigo/b2b/gt/comcorp/messages/organizations/${ORG_ID}`;
  return getAllPagedFlex(url, baseParams, { pageSize, maxPages });
}

export async function tigoListMessages({
  direction = "MT",
  days = Number(process.env.TIGO_B2B_DAYS || 7),
  pageSize = Number(process.env.TIGO_PAGE_SIZE || 100),
  maxPages = 200,
  startDate,
  endDate
} = {}) {
  if (!ORG_ID) throw new Error("Falta TIGO_B2B_ORG_ID en .env");

  const end   = endDate  ? new Date(endDate)  : new Date();
  const start = startDate? new Date(startDate): new Date(end.getTime() - days*24*60*60*1000);
  const dirParam = (direction && direction !== "ALL") ? { direction } : {};

  const variants = [
    { name: "NO_DATES",         params: { ...dirParam } },
    { name: "RFC_start_date",   params: { ...dirParam, start_date: toRFC(start),     end_date: toRFC(end) } },
    { name: "ISO_startDate",    params: { ...dirParam, startDate: toISO(start),      endDate: toISO(end) } },
    { name: "YMDHMS_startDate", params: { ...dirParam, startDate: toYmdHms(start),   endDate: toYmdHms(end) } },
    { name: "ISO_from_to",      params: { ...dirParam, from: toISO(start),           to: toISO(end) } },
  ];

  let lastErr;
  for (const v of variants) {
    try {
      const data = await getMessagesWithParams(v.params, { pageSize, maxPages });
      if (DEBUG) console.log(`[TIGO][MSG VAR OK] ${v.name} => ${data.length} registros`);
      return data;
    } catch (e) {
      lastErr = e;
      const status = e?.response?.status;
      const data = e?.response?.data;
      if (DEBUG) console.log(`[TIGO][MSG VAR FAIL] ${v.name} status=${status}`, typeof data === "string" ? data.slice(0,120) : data);
      if (status === 401 || status === 403) throw e;
    }
  }
  throw lastErr || new Error("Todas las variantes de mensajes fallaron");
}

// src/providers/tigo.js
import axios from "axios";

const BASE   = process.env.TIGO_B2B_BASE || "https://prod.api.tigo.com/v1";
const ORG_ID = process.env.TIGO_B2B_ORG_ID;
const TOKEN  = process.env.TIGO_B2B_TOKEN;
const APIKEY = process.env.TIGO_B2B_API_KEY;
const APISEC = process.env.TIGO_B2B_API_SECRET;

function authHeaders() {
  if (!TOKEN || !ORG_ID) throw new Error("Faltan TIGO_B2B_TOKEN o TIGO_B2B_ORG_ID en .env");
  const h = {
    Authorization: `Bearer ${TOKEN}`,
    Accept: "*/*",
    "Content-Type": "application/json",
  };
  if (APIKEY) h.APIKey = APIKEY;
  if (APISEC) h.APISecret = APISEC;
  return h;
}

/**
 * Paginación explícita con ?size=...&page=... (1-index).
 * Corta por:
 *  - res.data.last === true
 *  - page >= res.data.totalPages (si viene)
 *  - menos de pageSize elementos
 *  - alcanzar maxPages
 */
export async function tigoListMessagesPaged({
  direction = "MT",
  pageSize = 500,
  maxPages = 20,
  startPage = 1,       // *** 1-index requerido ***
  includeDates = false,
  startDate,
  endDate
} = {}) {
  if (!ORG_ID) throw new Error("Falta TIGO_B2B_ORG_ID en .env");
  const url = `${BASE}/tigo/b2b/gt/comcorp/messages/organizations/${ORG_ID}`;
  const headers = authHeaders();

  const base = {};
  if (direction && direction !== "ALL") base.direction = direction;
  if (includeDates && (startDate || endDate)) {
    base.startDate = startDate;
    base.endDate   = endDate;
  }

  const out = [];
  let pagesFetched = 0;

  for (let page = startPage; pagesFetched < maxPages; page++, pagesFetched++) {
    try {
      const params = { ...base, size: pageSize, page };
      const res = await axios.get(url, { headers, params });

      const items = Array.isArray(res.data?.content) ? res.data.content : [];
      out.push(...items);

      const last = res.data?.last === true;
      const totalPages = Number.isFinite(res.data?.totalPages) ? res.data.totalPages : null;

      if (last) break;
      if (totalPages && page >= totalPages) break;
      if (items.length < pageSize) break;
    } catch (e) {
      const status = e?.response?.status || 0;
      const msg = e?.response?.data || e.message;
      // Si la API devuelve 400 por página inválida (> totalPages), cortamos limpio
      if (status === 400) {
        console.warn(`[TIGO][PAG] 400 en page=${page} → corto paginación.`, msg);
        break;
      }
      throw e;
    }
  }

  return out;
}

/**
 * CONTACTOS: misma paginación 1-index (?size=...&page=...).
 * Corta por:
 *  - res.data.last === true
 *  - page >= res.data.totalPages (si viene)
 *  - menos de pageSize
 *  - alcanzar maxPages
 */
export async function tigoListContactsPaged({
  pageSize = 500,
  maxPages = 20,
  startPage = 1
} = {}) {
  if (!ORG_ID) throw new Error("Falta TIGO_B2B_ORG_ID en .env");
  const url = `${BASE}/tigo/b2b/gt/comcorp/contacts/organizations/${ORG_ID}`;
  const headers = authHeaders();

  const out = [];
  let pagesFetched = 0;

  for (let page = startPage; pagesFetched < maxPages; page++, pagesFetched++) {
    try {
      const params = { size: pageSize, page };
      const res = await axios.get(url, { headers, params });

      const items = Array.isArray(res.data?.content) ? res.data.content : [];
      out.push(...items);

      const last = res.data?.last === true;
      const totalPages = Number.isFinite(res.data?.totalPages) ? res.data.totalPages : null;

      if (last) break;
      if (totalPages && page >= totalPages) break;
      if (items.length < pageSize) break;
    } catch (e) {
      const status = e?.response?.status || 0;
      const msg = e?.response?.data || e.message;
      if (status === 400) {
        console.warn(`[TIGO][CONTACTOS] 400 en page=${page} → fin de paginación.`, msg);
        break;
      }
      throw e;
    }
  }

  return out;
}

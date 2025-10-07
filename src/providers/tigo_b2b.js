// src/providers/tigo_b2b.js
import "dotenv/config";
import axios from "axios";

const ROOT = process.env.TIGO_B2B_BASE || "https://prod.api.tigo.com/v1";
const BASE = `${ROOT}/tigo/b2b/gt/comcorp`;

const ORG    = process.env.TIGO_ORG_ID || process.env.TIGO_B2B_ORG_ID || process.env.OrganizationId;
const TOKEN  = process.env.TIGO_B2B_TOKEN || process.env.Token;
const APIKEY = process.env.TIGO_B2B_API_KEY || process.env.APIKey;

const DEBUG = String(process.env.DEBUG_TIGO || "").toLowerCase() === "1";

function assertEnv() {
  if (!ORG)    throw new Error("Falta TIGO_ORG_ID / TIGO_B2B_ORG_ID / OrganizationId en .env");
  if (!TOKEN)  throw new Error("Falta TIGO_B2B_TOKEN / Token en .env");
  if (!APIKEY) throw new Error("Falta TIGO_B2B_API_KEY / APIKey en .env");
}

function authHeaders() {
  return {
    Authorization: `Bearer ${TOKEN}`,
    Accept: "application/json",
    "Content-Type": "application/json",
    apikey: APIKEY,      // Apigee
    "x-api-key": APIKEY, // fallback
  };
}

const http = axios.create({
  headers: authHeaders(),
  timeout: 30000,
});

export async function tigoListMessages({ start, end, direction = "BOTH", size = 100 }) {
  assertEnv();
  let page = 0;
  const out = [];
  while (true) {
    const url = `${BASE}/messages/organizations/${encodeURIComponent(ORG)}`;
    const params = {
      page, size, direction,
      start_date: start.toUTCString(),
      end_date: end.toUTCString(),
    };
    if (DEBUG) console.log("[TIGO][MSG][REQ]", { url, params });
    const { data } = await http.get(url, { params });
    const content = Array.isArray(data?.content) ? data.content : [];
    out.push(...content);
    const last = Boolean(data?.last) || content.length === 0;
    if (DEBUG) console.log(`[TIGO][MSG] page=${page} items=${content.length} total=${out.length} last=${last}`);
    if (last) break;
    page += 1;
  }
  return out;
}

export function toE164GT(msisdn) {
  const digits = String(msisdn || "").replace(/\D/g, "");
  if (digits.startsWith("502")) return `+${digits}`;
  return `+502${digits}`;
}

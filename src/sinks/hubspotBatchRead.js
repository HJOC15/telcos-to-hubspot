import axios from "axios";
import "dotenv/config";

const TOKEN = process.env.HUBSPOT_TOKEN;

const BATCH = Number(process.env.HS_BATCH_READ_SIZE || 100);
const RETRY_MAX = Number(process.env.HS_RETRY_MAX || 8);
const RETRY_BASE_MS = Number(process.env.HS_RETRY_BASE_MS || 700);
const BETWEEN_CALLS_MS = Number(process.env.HS_BATCH_READ_DELAY_MS || 120);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function postWith429Retry(url, body) {
  for (let attempt = 0; attempt <= RETRY_MAX; attempt++) {
    try {
      return await axios.post(url, body, {
        headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" },
        timeout: 60000,
      });
    } catch (e) {
      const status = e?.response?.status;
      if (status !== 429) throw e;

      const retryAfter = Number(e?.response?.headers?.["retry-after"] || 0);
      const waitMs = retryAfter
        ? retryAfter * 1000
        : Math.min(30000, RETRY_BASE_MS * Math.pow(2, attempt));

      console.warn(`[HS][429] rate limit. sleep ${waitMs}ms (attempt ${attempt + 1}/${RETRY_MAX + 1})`);
      await sleep(waitMs);
    }
  }
  throw new Error("[HS] agoté retries por 429");
}

/**
 * Devuelve Map<idValue, recordId>
 * - objectType: "contacts" o "2-50592224"
 * - idProperty: "numero_telefono_id_unico" o "id_mensaje_unico"
 * - ids: array de valores de esa propiedad
 */
export async function batchReadIdsByProperty({ objectType, idProperty, ids }) {
  const out = new Map();
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(objectType)}/batch/read`;

  for (let i = 0; i < ids.length; i += BATCH) {
    const chunk = ids.slice(i, i + BATCH);

    const body = {
      idProperty,
      properties: [idProperty],
      inputs: chunk.map(v => ({ id: String(v) }))
    };

    const { data } = await postWith429Retry(url, body);

    for (const r of (data?.results || [])) {
      const key = r?.properties?.[idProperty];
      if (key != null) out.set(String(key), String(r.id));
    }

    if (BETWEEN_CALLS_MS > 0) await sleep(BETWEEN_CALLS_MS);
  }

  return out;
}

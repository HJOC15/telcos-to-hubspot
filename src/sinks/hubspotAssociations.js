// src/sinks/hubspotAssociations.js
import "dotenv/config";
import axios from "axios";

const HS = axios.create({
  baseURL: "https://api.hubapi.com",
  timeout: 30000,
  headers: {
    Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
    "Content-Type": "application/json",
  },
});

const CONTACTS = "contacts";
const CUSTOM   = process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes";
const CONTACT_UNIQUE_PROP = process.env.HUBSPOT_ID_PROPERTY || "numero_telefono_id_unico";
const MSG_UNIQUE_PROP     = process.env.HUBSPOT_MESSAGES_ID_PROPERTY || "id_mensaje_unico";

const DEBUG = String(process.env.DEBUG_ASSOC || "").toLowerCase() === "1";

// cache simple para typeId
let _assocTypeId = null;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/** Llamada con reintentos para RATE_LIMIT (429 / policy SECONDLY) */
async function hsRequestWithRetry(fn, { label = "HS", maxRetries = 5 } = {}) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (e) {
      const status = e?.response?.status;
      const data   = e?.response?.data;
      const isRate = status === 429 || data?.errorType === "RATE_LIMIT" || data?.policyName === "SECONDLY";

      if (isRate && attempt < maxRetries) {
        const retryAfter = Number(e?.response?.headers?.["retry-after"]) || 0;
        const backoff = retryAfter > 0 ? (retryAfter * 1000) : (1200 * (attempt + 1)); // 1.2s, 2.4s, 3.6s...
        if (DEBUG) console.log(`[${label}] rate-limit; intento ${attempt + 1}/${maxRetries}, esperando ${backoff}ms`);
        await sleep(backoff);
        attempt++;
        continue;
      }
      throw e;
    }
  }
}

function toE164GT(raw) {
  const digits = String(raw || "").replace(/\D/g, "");
  if (!digits) return "";
  if (digits.startsWith("502") && digits.length === 11) return `+${digits}`;
  if (digits.length === 8) return `+502${digits}`;
  if (String(raw || "").trim().startsWith("+")) return String(raw).trim();
  if (digits.startsWith("502")) return `+${digits}`;
  return `+${digits}`;
}

async function findContactIdByPhone(phone) {
  const body = {
    filterGroups: [{ filters: [{ propertyName: CONTACT_UNIQUE_PROP, operator: "EQ", value: phone }] }],
    properties: [],
    limit: 1
  };
  const { data } = await hsRequestWithRetry(
    () => HS.post("/crm/v3/objects/contacts/search", body),
    { label: "CONTACT-SEARCH" }
  );
  return data?.results?.[0]?.id || null;
}

async function findMessageIdByUnique(uniqueValue) {
  const body = {
    filterGroups: [{ filters: [{ propertyName: MSG_UNIQUE_PROP, operator: "EQ", value: String(uniqueValue) }] }],
    properties: [],
    limit: 1
  };
  const { data } = await hsRequestWithRetry(
    () => HS.post(`/crm/v3/objects/${CUSTOM}/search`, body),
    { label: "MSG-SEARCH" }
  );
  return data?.results?.[0]?.id || null;
}

async function resolveAssociationTypeId() {
  if (_assocTypeId) return _assocTypeId;
  const { data } = await hsRequestWithRetry(
    () => HS.get(`/crm/v4/associations/${CUSTOM}/${CONTACTS}/labels`),
    { label: "ASSOC-LABELS" }
  );
  const type = data?.results?.[0];
  if (!type?.typeId) {
    throw new Error(`No hay typeId de asociación entre ${CUSTOM} y ${CONTACTS}. Crea un label en HubSpot (Settings → Objects → Associations).`);
  }
  _assocTypeId = type.typeId;
  return _assocTypeId;
}

function chunk(arr, size = 100) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * rows: [{ mensajeIdValue, numero }]
 * - número puede venir sin '+', se normaliza aquí
 * Optimizaciones:
 *  - Deduplicamos teléfonos (1 búsqueda por número)
 *  - Throttling suave (sleep entre búsquedas)
 *  - Reintentos automáticos ante RATE_LIMIT
 */
export async function associateMessagesToContactsByPhone(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return { ok: true, created: 0, skipped: 0, notFoundContacts: 0, notFoundMessages: 0 };
  }

  const typeId = await resolveAssociationTypeId();

  // Normaliza y prepara
  const normalized = rows.map(r => ({
    msgKey: r?.mensajeIdValue ?? r?.[MSG_UNIQUE_PROP],
    phone: toE164GT(r?.numero)
  })).filter(x => x.msgKey && x.phone);

  if (normalized.length === 0) {
    return { ok: true, created: 0, skipped: rows.length, notFoundContacts: 0, notFoundMessages: 0 };
  }

  // 1) Buscar contactos por teléfono (deduplicado)
  const uniquePhones = Array.from(new Set(normalized.map(x => x.phone)));
  const phoneToContactId = new Map();
  let notFoundContacts = 0;

  for (let i = 0; i < uniquePhones.length; i++) {
    const p = uniquePhones[i];
    const id = await findContactIdByPhone(p).catch(() => null);
    if (id) phoneToContactId.set(p, id);
    else notFoundContacts++;
    // throttling suave para no golpear SECONDLY
    await sleep(120); // ~8-9 requests/seg
    if (DEBUG && (i + 1) % 50 === 0) {
      console.log(`[ASSOC] buscados contactos: ${i + 1}/${uniquePhones.length}`);
    }
  }

  // 2) Buscar mensajes por clave única (sin deduplicar porque cada uno es distinto)
  const msgKeyToMsgId = new Map();
  let notFoundMessages = 0;

  for (let i = 0; i < normalized.length; i++) {
    const k = normalized[i].msgKey;
    if (msgKeyToMsgId.has(k)) continue; // ya buscado
    const id = await findMessageIdByUnique(k).catch(() => null);
    if (id) msgKeyToMsgId.set(k, id);
    else notFoundMessages++;
    await sleep(120);
    if (DEBUG && (i + 1) % 50 === 0) {
      console.log(`[ASSOC] buscados mensajes: ${i + 1}/${normalized.length}`);
    }
  }

  // 3) Armar inputs
  const inputs = [];
  let skipped = 0;

  for (const r of normalized) {
    const msgId = msgKeyToMsgId.get(r.msgKey);
    const contactId = phoneToContactId.get(r.phone);
    if (!msgId || !contactId) {
      if (DEBUG) console.log("[ASSOC][SKIP]", { msgUnique: r.msgKey, phoneE164: r.phone, msgId, contactId });
      skipped++;
      continue;
    }
    inputs.push({ from: { id: String(msgId) }, to: { id: String(contactId) }, type: typeId });
  }

  if (inputs.length === 0) {
    return { ok: true, created: 0, skipped, notFoundContacts, notFoundMessages };
  }

  // 4) Crear asociaciones en batch con reintentos (y pausas)
  let created = 0;
  const parts = chunk(inputs, 100);

  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    const { data } = await hsRequestWithRetry(
      () => HS.post(`/crm/v4/associations/${CUSTOM}/${CONTACTS}/batch/create`, { inputs: part }),
      { label: "ASSOC-CREATE" }
    );
    created += data?.results?.length || 0;
    // pequeña pausa entre lotes
    await sleep(250);
    if (DEBUG) console.log(`[ASSOC] lote ${i + 1}/${parts.length} creado=${data?.results?.length || 0}`);
  }

  return { ok: true, created, skipped, notFoundContacts, notFoundMessages };
}

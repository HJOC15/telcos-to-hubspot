// src/jobs/associate_tigo_messages_contacts.js
import "dotenv/config";
import axios from "axios";
import { batchReadIdsByProperty } from "../sinks/hubspotBatchRead.js";
import {
  getAssociationTypeIdEitherDirection,
  batchAssociateDirected
} from "../sinks/hubspotAssoc.js";

const TOKEN = process.env.HUBSPOT_TOKEN;

const CONTACT_UNIQUE  = (process.env.HUBSPOT_ID_PROPERTY || "numero_telefono_id_unico").trim();

const MSG_OBJECT = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const MSG_UNIQUE = (process.env.HUBSPOT_MESSAGES_ID_PROPERTY || "id_mensaje_unico").trim();

const MSG_NUMBER_PROP = "numero";
const PAGE_SIZE = 100;

// Ojo: para /crm/v3/objects, contactos siempre es "contacts"
const CONTACTS_OBJECT_V3 = "contacts";

const RATE_DELAYMS = Number(process.env.SYNC_RATE_DELAY_MS || 250);
const ASSOC_BATCH_SIZE = Number(process.env.ASSOC_BATCH_SIZE || 100);

// --- helpers ---
const onlyDigits = (s) => String(s || "").replace(/\D/g, "");

// Normaliza a E.164 GT: +502XXXXXXXX (para que matchee con tu numero_telefono_id_unico)
function toE164GT(raw) {
  const d = onlyDigits(raw);
  if (!d) return "";
  if (String(raw || "").trim().startsWith("+")) return String(raw).trim();
  if (d.startsWith("502") && d.length === 11) return `+${d}`;
  if (d.length === 8) return `+502${d}`;
  return `+${d}`;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Reintenta automático si HubSpot te tira 429
async function withRetry429(label, fn, max = 8) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (e) {
      const status = e?.response?.status;
      if (status !== 429 || attempt >= max) throw e;

      // HubSpot a veces manda Retry-After (segundos)
      const ra = Number(e?.response?.headers?.["retry-after"] || 0);
      const waitMs = ra > 0 ? ra * 1000 : (1000 + attempt * 900); // backoff suave
      console.warn(`[RATE_LIMIT][${label}] 429. Reintentando en ${waitMs}ms (attempt ${attempt + 1}/${max})`);
      await sleep(waitMs);
      attempt++;
    }
  }
}

async function listMessagesFromHS({ after } = {}) {
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(MSG_OBJECT)}`;
  const headers = { Authorization: `Bearer ${TOKEN}` };
  const params = {
    limit: PAGE_SIZE,
    properties: [MSG_UNIQUE, MSG_NUMBER_PROP, "compania"].join(","),
    after
  };

  const { data } = await withRetry429("listMessages", () => axios.get(url, { headers, params }));
  return data;
}

async function fetchAllMessagesFromHS() {
  const out = [];
  let after;
  do {
    const data = await listMessagesFromHS({ after });
    out.push(...(Array.isArray(data?.results) ? data.results : []));
    after = data?.paging?.next?.after;
    if (RATE_DELAYMS > 0 && after) await sleep(RATE_DELAYMS);
  } while (after);
  return out;
}

export async function associateTigoMessagesToContacts() {
  console.log("== Asociar Mensajes → Contactos ==");

  try {
    const messages = await fetchAllMessagesFromHS();
    console.log(`[HS:${MSG_OBJECT}] mensajes encontrados=${messages.length}`);
    if (!messages.length) {
      console.warn("No hay mensajes en HS para asociar.");
      console.log("== End Asociaciones ==");
      return;
    }

    // Indexar mensajes por número (normalizado)
    const numberToMessageIds = new Map();
    for (const m of messages) {
      const id = String(m.id);
      const numeroRaw = String(m?.properties?.[MSG_NUMBER_PROP] || "").trim();
      const numero = toE164GT(numeroRaw);
      if (!numero) continue;

      if (!numberToMessageIds.has(numero)) numberToMessageIds.set(numero, []);
      numberToMessageIds.get(numero).push(id);
    }
    console.log(`[IDX] numeros_distintos=${numberToMessageIds.size}`);

    const allNumbers = Array.from(numberToMessageIds.keys());

    // ✅ Resolver contactos por batch-read (evita /search y su RATE_LIMIT)
    const contactIdByNumber = await withRetry429("batchReadContacts", () =>
      batchReadIdsByProperty({
        objectType: CONTACTS_OBJECT_V3, // "contacts"
        idProperty: CONTACT_UNIQUE,
        ids: allNumbers
      })
    );

    console.log(`[BATCH-READ] contactos encontrados=${contactIdByNumber.size}/${allNumbers.length}`);

    // Construir pares [mensajeId, contactoId]
    const pairs = [];
    for (const [numero, msgIds] of numberToMessageIds.entries()) {
      const contactId = contactIdByNumber.get(numero);
      if (!contactId) continue;
      for (const mid of msgIds) pairs.push([mid, contactId]);
    }

    console.log(`[PAIRS] asociaciones a crear=${pairs.length}`);
    if (!pairs.length) {
      console.warn("No hay pares por número. Revisa formato E.164 / carga de contactos.");
      console.log("== End Asociaciones ==");
      return;
    }

    // Obtener associationTypeId y dirección válida (A->B o B->A)
    // Ojo: aquí sí usamos objectTypeId tipo "0-1" para associations (tu helper lo resuelve)
    const { associationTypeId, fromId, toId, reversed } =
      await getAssociationTypeIdEitherDirection(MSG_OBJECT, "contacts");
    console.log(`[ASSOC] typeId=${associationTypeId} (ruta ${fromId} → ${toId}) reversed=${reversed}`);

    const FINAL_PAIRS = reversed ? pairs.map(([mid, cid]) => [cid, mid]) : pairs;

    let done = 0;
    for (let i = 0; i < FINAL_PAIRS.length; i += ASSOC_BATCH_SIZE) {
      const slice = FINAL_PAIRS.slice(i, i + ASSOC_BATCH_SIZE);

      await withRetry429("batchAssociate", () =>
        batchAssociateDirected({
          fromId,
          toId,
          associationTypeId,
          pairs: slice
        })
      );

      done += slice.length;
      console.log(`[ASSOC] creadas=${done}/${FINAL_PAIRS.length}`);
      if (RATE_DELAYMS > 0) await sleep(RATE_DELAYMS);
    }

    console.log(`[FIN] asociaciones creadas=${done}`);
  } catch (e) {
    const msg = e?.response?.data ?? e.message ?? e;
    console.error("[ASOC] error:", msg);
  }

  console.log("== End Asociaciones ==");
}

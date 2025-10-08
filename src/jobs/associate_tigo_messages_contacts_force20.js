// src/jobs/associate_tigo_messages_contacts_force20.js
import "dotenv/config";
import axios from "axios";
import { searchManyByProperty } from "../sinks/hubspotSearch.js";
import { resolveObjectTypeId } from "../sinks/hubspotAssoc.js";

const TOKEN = process.env.HUBSPOT_TOKEN;

const CONTACTS_OBJECT = (process.env.HUBSPOT_CONTACTS_OBJECT || "contacts").trim();
const CONTACT_UNIQUE  = (process.env.HUBSPOT_ID_PROPERTY || "numero_telefono_id_unico").trim();

const MSG_OBJECT = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const MSG_UNIQUE = (process.env.HUBSPOT_MESSAGES_ID_PROPERTY || "id_mensaje_unico").trim();

const MSG_NUMBER_PROP = "numero";
const PAGE_SIZE = 100;
const BATCH = 100;
const RATE_MS = Number(process.env.SYNC_RATE_DELAY_MS || 150);

// === lee mensajes desde HS (v3)
async function listMessagesPage({ after } = {}) {
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(MSG_OBJECT)}`;
  const headers = { Authorization: `Bearer ${TOKEN}` };
  const params = { limit: PAGE_SIZE, properties: [MSG_UNIQUE, MSG_NUMBER_PROP].join(","), after };
  const { data } = await axios.get(url, { headers, params });
  return data;
}
async function fetchAllMessages() {
  const all = [];
  let after;
  do {
    const data = await listMessagesPage({ after });
    all.push(...(Array.isArray(data?.results) ? data.results : []));
    after = data?.paging?.next?.after;
    if (after && RATE_MS) await new Promise(r => setTimeout(r, RATE_MS));
  } while (after);
  return all;
}

// === crea asociaciones v4 con typeId 20 (p_mensajes -> contacts)
async function createAssocV4({ fromTypeId, toTypeId, pairs, associationTypeId = 20 }) {
  if (!pairs.length) return { created: 0 };
  const url = `https://api.hubapi.com/crm/v4/associations/${encodeURIComponent(fromTypeId)}/${encodeURIComponent(toTypeId)}/batch/create`;
  const inputs = pairs.map(([fromId, toId]) => ({
    from: { id: String(fromId) },
    to:   { id: String(toId) },
    types: [{ associationCategory: "USER_DEFINED", associationTypeId }]
  }));
  const { data } = await axios.post(url, { inputs }, {
    headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" }
  });
  return { created: inputs.length, raw: data };
}

// === verificación rápida v4 (lee asociaciones de unos cuantos mensajes)
async function readAssocV4({ fromTypeId, toTypeId, fromIds }) {
  const url = `https://api.hubapi.com/crm/v4/associations/${encodeURIComponent(fromTypeId)}/${encodeURIComponent(toTypeId)}/batch/read`;
  const inputs = fromIds.slice(0, 5).map(id => ({ id: String(id) })); // muestra
  const { data } = await axios.post(url, { inputs }, {
    headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" }
  });
  return Array.isArray(data?.results) ? data.results : [];
}

export async function associateTigoMessagesToContactsForce20() {
  console.log("== Asociar Mensajes (p_mensajes → contacts) con typeId=20 ==");

  try {
    // 1) Resolver objectTypeIds reales
    const msgTypeId = await resolveObjectTypeId(MSG_OBJECT);
    const ctcTypeId = await resolveObjectTypeId(CONTACTS_OBJECT);

    // 2) Traer mensajes y agrupar por número
    const messages = await fetchAllMessages();
    console.log(`[HS:${msgTypeId}] mensajes encontrados=${messages.length}`);
    if (!messages.length) return console.warn("No hay mensajes para asociar.");

    const numberToMsgIds = new Map();
    for (const m of messages) {
      const id = String(m.id);
      const num = String(m?.properties?.[MSG_NUMBER_PROP] || "").trim();
      if (!num) continue;
      if (!numberToMsgIds.has(num)) numberToMsgIds.set(num, []);
      numberToMsgIds.get(num).push(id);
    }
    console.log(`[IDX] numeros_distintos=${numberToMsgIds.size}`);

    // 3) Buscar contactos por su propiedad única
    const allNumbers = Array.from(numberToMsgIds.keys());
    const contactIdByNumber = await searchManyByProperty({
      objectType: CONTACTS_OBJECT,
      propertyName: CONTACT_UNIQUE,
      values: allNumbers
    });
    console.log(`[SEARCH] contactos encontrados=${contactIdByNumber.size}/${allNumbers.length}`);

    // 4) Construir pares (from=mensaje -> to=contacto)
    const pairs = [];
    for (const [numero, msgIds] of numberToMsgIds.entries()) {
      const contactId = contactIdByNumber.get(numero);
      if (!contactId) continue;
      for (const mid of msgIds) pairs.push([mid, contactId]);
    }
    console.log(`[PAIRS] asociaciones a crear=${pairs.length}`);
    if (!pairs.length) return console.warn("No hay pares por número (revisa formato E.164 y la propiedad única del contacto).");

    // 5) Crear asociaciones (forzado label id=20)
    let created = 0;
    for (let i = 0; i < pairs.length; i += BATCH) {
      const slice = pairs.slice(i, i + BATCH);
      const res = await createAssocV4({ fromTypeId: msgTypeId, toTypeId: ctcTypeId, pairs: slice, associationTypeId: 20 });
      created += res.created || 0;
      console.log(`[ASSOC] creadas=${created}/${pairs.length}`);
      if (RATE_MS) await new Promise(r => setTimeout(r, RATE_MS));
    }

    // 6) Verificación rápida sobre 5 mensajes al azar
    const sampleMsgIds = pairs.slice(0, 5).map(([m]) => m);
    if (sampleMsgIds.length) {
      const read = await readAssocV4({ fromTypeId: msgTypeId, toTypeId: ctcTypeId, fromIds: sampleMsgIds });
      console.log("[VERIFY] lectura v4 (muestra):", JSON.stringify(read, null, 2));
      const totals = read.map(r => ({ msgId: r.fromId, count: (r.to || []).length }));
      console.log("[VERIFY] conteos:", totals);
    }

    console.log(`[FIN] asociaciones creadas=${created} (typeId=20, p_mensajes→contacts)`);
  } catch (e) {
    const msg = e?.response?.data ?? e.message ?? e;
    console.error("[ASOC] error:", msg);
  }

  console.log("== End Asociaciones ==");
}

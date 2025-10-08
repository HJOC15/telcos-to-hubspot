// src/jobs/associate_tigo_messages_contacts.js
import "dotenv/config";
import axios from "axios";
import { searchManyByProperty } from "../sinks/hubspotSearch.js";
import {
  getAssociationTypeIdEitherDirection,
  batchAssociateDirected
} from "../sinks/hubspotAssoc.js";

const TOKEN = process.env.HUBSPOT_TOKEN;

const CONTACTS_OBJECT = (process.env.HUBSPOT_CONTACTS_OBJECT || "contacts").trim();
const CONTACT_UNIQUE  = (process.env.HUBSPOT_ID_PROPERTY || "numero_telefono_id_unico").trim();

const MSG_OBJECT = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const MSG_UNIQUE = (process.env.HUBSPOT_MESSAGES_ID_PROPERTY || "id_mensaje_unico").trim();

const MSG_NUMBER_PROP = "numero";
const PAGE_SIZE = 100;
const RATE_DELAYMS = Number(process.env.SYNC_RATE_DELAY_MS || 250);

async function listMessagesFromHS({ after } = {}) {
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(MSG_OBJECT)}`;
  const headers = { Authorization: `Bearer ${TOKEN}` };
  const params = {
    limit: PAGE_SIZE,
    properties: [MSG_UNIQUE, MSG_NUMBER_PROP, "compania"].join(","),
    after
  };
  const { data } = await axios.get(url, { headers, params });
  return data;
}

async function fetchAllMessagesFromHS() {
  const out = [];
  let after;
  do {
    const data = await listMessagesFromHS({ after });
    out.push(...(Array.isArray(data?.results) ? data.results : []));
    after = data?.paging?.next?.after;
    if (RATE_DELAYMS > 0 && after) await new Promise(r => setTimeout(r, RATE_DELAYMS));
  } while (after);
  return out;
}

export async function associateTigoMessagesToContacts() {
  console.log("== Asociar Mensajes (Tigo) → Contactos ==");

  try {
    const messages = await fetchAllMessagesFromHS();
    console.log(`[HS:${MSG_OBJECT}] mensajes encontrados=${messages.length}`);
    if (!messages.length) { console.warn("No hay mensajes en HS para asociar."); console.log("== End Asociaciones =="); return; }

    // Indexar mensajes por número
    const numberToMessageIds = new Map();
    for (const m of messages) {
      const id = String(m.id);
      const numero = String(m?.properties?.[MSG_NUMBER_PROP] || "").trim();
      if (!numero) continue;
      if (!numberToMessageIds.has(numero)) numberToMessageIds.set(numero, []);
      numberToMessageIds.get(numero).push(id);
    }
    console.log(`[IDX] numeros_distintos=${numberToMessageIds.size}`);

    // Buscar contactos por número (prop única)
    const allNumbers = Array.from(numberToMessageIds.keys());
    const contactIdByNumber = await searchManyByProperty({
      objectType: CONTACTS_OBJECT,
      propertyName: CONTACT_UNIQUE,
      values: allNumbers
    });
    console.log(`[SEARCH] contactos encontrados=${contactIdByNumber.size}/${allNumbers.length}`);

    // Construir pares [mensajeId, contactoId]
    const pairs = [];
    for (const [numero, msgIds] of numberToMessageIds.entries()) {
      const contactId = contactIdByNumber.get(numero);
      if (!contactId) continue;
      for (const mid of msgIds) pairs.push([mid, contactId]);
    }
    console.log(`[PAIRS] asociaciones a crear=${pairs.length}`);
    if (!pairs.length) { console.warn("No hay pares por número. Revisa formato E.164 / carga de contactos."); console.log("== End Asociaciones =="); return; }

    // Obtener associationTypeId y dirección válida (A->B o B->A)
    const { associationTypeId, fromId, toId, reversed } =
      await getAssociationTypeIdEitherDirection(MSG_OBJECT, CONTACTS_OBJECT);
    console.log(`[ASSOC] typeId=${associationTypeId} (ruta ${fromId} → ${toId}) reversed=${reversed}`);

    // Si la dirección es inversa, invertimos cada par [msgId, contactId] -> [contactId, msgId]
    const FINAL_PAIRS = reversed ? pairs.map(([mid, cid]) => [cid, mid]) : pairs;

    // Batch create
    const chunk = 100;
    let done = 0;
    for (let i = 0; i < FINAL_PAIRS.length; i += chunk) {
      const slice = FINAL_PAIRS.slice(i, i + chunk);
      await batchAssociateDirected({
        fromId,
        toId,
        associationTypeId,
        pairs: slice
      });
      done += slice.length;
      console.log(`[ASSOC] creadas=${done}/${FINAL_PAIRS.length}`);
      if (RATE_DELAYMS > 0) await new Promise(r => setTimeout(r, RATE_DELAYMS));
    }
    console.log(`[FIN] asociaciones creadas=${done}`);
  } catch (e) {
    const msg = e?.response?.data ?? e.message ?? e;
    console.error("[ASOC] error:", msg);
  }

  console.log("== End Asociaciones ==");
}

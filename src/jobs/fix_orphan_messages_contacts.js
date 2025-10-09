// src/jobs/fix_orphan_messages_contacts.js
import axios from "axios";
import { searchManyByProperty } from "../sinks/hubspotSearch.js";
import { batchUpsertCustomObject } from "../sinks/hubspotCustom.js";
import { getAssociationTypeId, batchAssociate } from "../sinks/hubspotAssoc.js";

const TOKEN = process.env.HUBSPOT_TOKEN;

// Objetos/props en HubSpot
const MSG_OBJECT        = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const MSG_UNIQUE_PROP   = (process.env.HUBSPOT_MESSAGES_ID_PROPERTY || "id_mensaje_unico").trim();
const MSG_NUMBER_PROP   = "numero";     // teléfono en Mensajes
const MSG_COMPANIA_PROP = "compania";   // compañía en Mensajes (si no viene, usamos un fallback)

const CONTACTS_OBJECT   = (process.env.HUBSPOT_CONTACTS_OBJECT || "contacts").trim();
const CONTACT_ID_PROP   = (process.env.HUBSPOT_ID_PROPERTY || "numero_telefono_id_unico").trim();

const PAGE_SIZE    = Number(process.env.ORPHANS_PAGE_SIZE || 100);
const CHUNK_ASSOC  = 100;
const RATE_DELAYMS = Number(process.env.SYNC_RATE_DELAY_MS || 250);

// Fallback si un mensaje no trae "compania"
const DEFAULT_COMPANIA = (process.env.ORPHAN_DEFAULT_COMPANIA || "").trim(); // ej: "Claro" o "Tigo"

// ---------- Helpers ----------
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function listMessagesPage({ after } = {}) {
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(MSG_OBJECT)}`;
  const params = {
    limit: PAGE_SIZE,
    properties: [MSG_UNIQUE_PROP, MSG_NUMBER_PROP, MSG_COMPANIA_PROP].join(","),
    after
  };
  const headers = { Authorization: `Bearer ${TOKEN}` };
  const { data } = await axios.get(url, { headers, params });
  return data;
}

async function fetchAllMessages() {
  const out = [];
  let after;
  do {
    const page = await listMessagesPage({ after });
    const rows = Array.isArray(page?.results) ? page.results : [];
    out.push(...rows);
    after = page?.paging?.next?.after;
    if (after && RATE_DELAYMS > 0) await sleep(RATE_DELAYMS);
  } while (after);
  return out;
}

function buildNumeroIndex(messages) {
  // numero -> { companyGuess, messageIds: [] }
  const index = new Map();
  for (const m of messages) {
    const msgId   = String(m.id);
    const numero  = String(m?.properties?.[MSG_NUMBER_PROP] || "").trim();
    if (!numero) continue;

    const compMsg = String(m?.properties?.[MSG_COMPANIA_PROP] || "").trim();
    const company = compMsg || DEFAULT_COMPANIA || ""; // usa fallback si el msg no trae

    if (!index.has(numero)) index.set(numero, { companyGuess: company, messageIds: [] });
    const bucket = index.get(numero);
    // si alguna vez obtenemos compañía explícita, sobreescribimos el guess vacío
    if (company && !bucket.companyGuess) bucket.companyGuess = company;

    bucket.messageIds.push(msgId);
  }
  return index;
}

function makePlaceholderNames(numero) {
  const digits = (numero || "").replace(/\D/g, "");
  const place  = `nombre_vacio_${digits || "sinnum"}`;
  return { firstname: place, lastname: place };
}

async function upsertMissingContacts({ missingNumeros, numeroIndex }) {
  if (!missingNumeros.length) return { created: 0 };

  const records = missingNumeros.map(num => {
    const guess = numeroIndex.get(num)?.companyGuess || DEFAULT_COMPANIA || "";
    const { firstname, lastname } = makePlaceholderNames(num);
    return {
      [CONTACT_ID_PROP]: num,     // upsert por número E.164 (+502…)
      phone: num,
      mobilephone: num,
      firstname,
      lastname,
      ...(guess ? { compania: guess } : {}) // sólo envía si hay valor
    };
  });

  const res = await batchUpsertCustomObject({
    token: TOKEN,
    objectType: CONTACTS_OBJECT,    // "contacts"
    idProperty: CONTACT_ID_PROP,    // "numero_telefono_id_unico"
    records
  });

  return { created: res?.sent || 0 };
}

export async function fixOrphanMessagesContacts() {
  console.log("== Orphans: crear contactos y asociar Mensajes → Contactos ==");

  try {
    // 1) Cargar todos los mensajes
    const messages = await fetchAllMessages();
    console.log(`[LOAD] mensajes=${messages.length} (obj=${MSG_OBJECT})`);

    // 2) Index por número
    const numeroIndex = buildNumeroIndex(messages);
    const allNumeros  = Array.from(numeroIndex.keys());
    console.log(`[INDEX] numeros_distintos=${allNumeros.length}`);

    if (!allNumeros.length) {
      console.warn("[INDEX] No hay números en mensajes.");
      console.log("== End Orphans ==");
      return;
    }

    // 3) Buscar contactos existentes por número
    const contactIdByNumber = await searchManyByProperty({
      objectType: CONTACTS_OBJECT,
      propertyName: CONTACT_ID_PROP,
      values: allNumeros
    });
    console.log(`[SEARCH] contactos_encontrados=${contactIdByNumber.size}/${allNumeros.length}`);

    // 4) Determinar números faltantes y crear contactos
    const missingNumeros = allNumeros.filter(n => !contactIdByNumber.has(n));
    if (missingNumeros.length) {
      console.log(`[CREATE] faltan=${missingNumeros.length} → creando contactos… (compania desde mensaje o DEFAULT="${DEFAULT_COMPANIA}")`);
      const { created } = await upsertMissingContacts({ missingNumeros, numeroIndex });
      console.log(`[CREATE] contactos_creados=${created}`);

      // re-cargar IDs para los que acabamos de crear
      const newIds = await searchManyByProperty({
        objectType: CONTACTS_OBJECT,
        propertyName: CONTACT_ID_PROP,
        values: missingNumeros
      });
      for (const [num, cid] of newIds.entries()) contactIdByNumber.set(num, cid);
    } else {
      console.log("[CREATE] no hay faltantes — todos los números ya existen como contactos.");
    }

    // 5) Preparar asociaciones (msgId → contactId)
    const pairs = [];
    for (const [numero, bucket] of numeroIndex.entries()) {
      const contactId = contactIdByNumber.get(numero);
      if (!contactId) continue; // si aún no lo conseguimos, saltamos
      for (const mid of bucket.messageIds) pairs.push([mid, contactId]);
    }
    console.log(`[ASSOC] total_pairs=${pairs.length}`);

    if (!pairs.length) {
      console.warn("[ASSOC] no hay pares que asociar.");
      console.log("== End Orphans ==");
      return;
    }

    // 6) Resolver associationTypeId (p_mensajes → contacts)
    const { associationTypeId } = await getAssociationTypeId(MSG_OBJECT, CONTACTS_OBJECT);
    if (!associationTypeId) throw new Error("[ASSOC] associationTypeId indefinido.");
    console.log(`[ASSOC] typeId=${associationTypeId} (${MSG_OBJECT} → ${CONTACTS_OBJECT})`);

    // 7) Crear asociaciones en lotes
    let done = 0;
    for (let i = 0; i < pairs.length; i += CHUNK_ASSOC) {
      const slice = pairs.slice(i, i + CHUNK_ASSOC);
      await batchAssociate({
        fromObject: MSG_OBJECT,
        toObject: CONTACTS_OBJECT,
        associationTypeId,
        pairs: slice
      });
      done += slice.length;
      console.log(`[ASSOC] creadas=${done}/${pairs.length}`);
      if (RATE_DELAYMS > 0) await sleep(RATE_DELAYMS);
    }

    // 8) Resumen
    const newContacts = missingNumeros.length;
    console.log(`[FIN] contactos_creados=${newContacts} asociaciones_creadas=${pairs.length}`);
  } catch (e) {
    console.error("[ORPHANS] error:", e?.response?.data ?? e.message ?? e);
  }

  console.log("== End Orphans ==");
}

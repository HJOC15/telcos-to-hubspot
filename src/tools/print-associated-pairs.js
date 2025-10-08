// src/tools/print-associated-pairs.js
import "dotenv/config";
import fs from "fs";
import path from "path";
import axios from "axios";

const TOKEN = process.env.HUBSPOT_TOKEN;
const PORTAL = process.env.HUBSPOT_PORTAL_ID || ""; // ej: 49515759 para links

// Objetos / props
const MSG_OBJECT = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const CONTACTS_OBJECT = (process.env.HUBSPOT_CONTACTS_OBJECT || "contacts").trim();
const MSG_NUMBER_PROP = (process.env.MSG_NUMBER_PROP || "numero").trim();
const CONTACT_NUMBER_PROP = (process.env.HUBSPOT_ID_PROPERTY || "numero_telefono_id_unico").trim();

// Paginación / lotes
const LIMIT = Number(process.env.CHECK_LIMIT || 999999);
const PAGE_SIZE = 100;
const BATCH_READ = Number(process.env.CHECK_BATCH || 100); // v4 associations read
const BATCH_GET = 100;                                     // v3 batch read contacts

// Filtro opcional por typeId (label). Déjalo vacío para ver todas.
const ONLY_LABEL_ID = process.env.CHECK_ASSOC_TYPE_ID
  ? Number(process.env.CHECK_ASSOC_TYPE_ID)
  : null;

if (!TOKEN) {
  console.error("Falta HUBSPOT_TOKEN en .env");
  process.exit(1);
}
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));

const msgUrl = (id)=> PORTAL ? `https://app.hubspot.com/contacts/${PORTAL}/record/${MSG_OBJECT}/${id}` : "";
const contactUrl = (id)=> PORTAL ? `https://app.hubspot.com/contacts/${PORTAL}/record/0-1/${id}` : "";

// 1) Trae mensajes con su número
async function fetchAllMessagesBasic() {
  const headers = { Authorization: `Bearer ${TOKEN}` };
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(MSG_OBJECT)}`;
  const out = [];
  let after;
  do {
    const params = { limit: PAGE_SIZE, properties: MSG_NUMBER_PROP, after };
    const { data } = await axios.get(url, { headers, params });
    const results = Array.isArray(data?.results) ? data.results : [];
    for (const r of results) {
      if (out.length >= LIMIT) break;
      out.push({
        id: String(r.id),
        numero: String(r?.properties?.[MSG_NUMBER_PROP] || "").trim()
      });
    }
    after = data?.paging?.next?.after;
    console.log(`[LOAD] mensajes acumulados=${out.length}${LIMIT<999999?` (límite ${LIMIT})`:""}`);
    if (!after || out.length >= LIMIT) break;
    await sleep(60);
  } while (true);
  return out;
}

// 2) Lee asociaciones v4 mensaje→contacto
async function readAssociationsV4(fromIds) {
  const url = `https://api.hubapi.com/crm/v4/associations/${encodeURIComponent(MSG_OBJECT)}/${encodeURIComponent(CONTACTS_OBJECT)}/batch/read`;
  const headers = { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" };
  const out = [];
  for (let i = 0; i < fromIds.length; i += BATCH_READ) {
    const slice = fromIds.slice(i, i + BATCH_READ);
    const body = { inputs: slice.map(id => ({ id })) };
    const { data } = await axios.post(url, body, { headers });
    out.push(...(Array.isArray(data?.results) ? data.results : []));
    console.log(`[ASSOC READ] ${Math.min(i+BATCH_READ, fromIds.length)}/${fromIds.length}`);
    await sleep(60);
  }
  return out;
}

// 3) Lee contactos por lote para traer su número
async function batchReadContacts(ids) {
  if (!ids.length) return new Map();
  const headers = { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" };
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(CONTACTS_OBJECT)}/batch/read`;
  const out = new Map();
  for (let i = 0; i < ids.length; i += BATCH_GET) {
    const slice = ids.slice(i, i + BATCH_GET);
    const body = { properties: [CONTACT_NUMBER_PROP], inputs: slice.map(id => ({ id: String(id) })) };
    const { data } = await axios.post(url, body, { headers });
    const res = Array.isArray(data?.results) ? data.results : [];
    for (const r of res) {
      out.set(String(r.id), String(r?.properties?.[CONTACT_NUMBER_PROP] || "").trim());
    }
    await sleep(60);
  }
  return out;
}

function toCsv(rows) {
  const header = [
    "msg_id","msg_numero","contact_id","contact_numero","typeIds","labels","msg_link","contact_link"
  ];
  const esc = s => String(s ?? "").replace(/"/g,'""');
  const lines = [header.join(",")];
  for (const r of rows) {
    lines.push([
      `"${esc(r.msg_id)}"`,
      `"${esc(r.msg_numero)}"`,
      `"${esc(r.contact_id)}"`,
      `"${esc(r.contact_numero)}"`,
      `"${esc(r.typeIds)}"`,
      `"${esc(r.labels)}"`,
      `"${esc(r.msg_link)}"`,
      `"${esc(r.contact_link)}"`
    ].join(","));
  }
  return lines.join("\n");
}

async function main() {
  console.log(`== Reporte asociaciones ${MSG_OBJECT} → contacts (con teléfonos) ==`);

  // Mensajes + número
  const messages = await fetchAllMessagesBasic(); // {id, numero}
  console.log(`[MSG] leídos=${messages.length} (límite ${LIMIT})`);
  if (!messages.length) return console.warn("No hay mensajes para revisar.");
  const msgIdToNumero = new Map(messages.map(m => [m.id, m.numero]));

  // Asociaciones
  const assoc = await readAssociationsV4(messages.map(m => m.id));

  // Contactos únicos y sus números
  const contactIdsSet = new Set();
  for (const r of assoc) {
    const tos = Array.isArray(r?.to) ? r.to : [];
    for (const t of tos) {
      if (ONLY_LABEL_ID != null) {
        const ok = Array.isArray(t.associationTypes) &&
          t.associationTypes.some(a => Number(a.typeId) === Number(ONLY_LABEL_ID));
        if (!ok) continue;
      }
      contactIdsSet.add(String(t.toObjectId));
    }
  }
  const contactIds = Array.from(contactIdsSet);
  console.log(`[CTC] distintos=${contactIds.length}`);
  const contactNumById = await batchReadContacts(contactIds);

  // Armar filas (solo asociaciones encontradas)
  const rows = [];
  let withAssoc = 0, withoutAssoc = 0;
  for (const r of assoc) {
    const mid = String(r?.from?.id || "");
    const msgNum = msgIdToNumero.get(mid) || "";
    const tos = Array.isArray(r?.to) ? r.to : [];

    const filteredTos = ONLY_LABEL_ID == null
      ? tos
      : tos.filter(t => Array.isArray(t.associationTypes)
          && t.associationTypes.some(a => Number(a.typeId) === Number(ONLY_LABEL_ID)));

    if (!filteredTos.length) { withoutAssoc++; continue; }

    for (const t of filteredTos) {
      withAssoc++;
      const cid = String(t.toObjectId);
      const cNum = contactNumById.get(cid) || "";
      const typeIds = (t.associationTypes || []).map(a => a.typeId).join("|");
      const labels  = (t.associationTypes || []).map(a => a.label || "").join("|");
      rows.push({
        msg_id: mid,
        msg_numero: msgNum,
        contact_id: cid,
        contact_numero: cNum,
        typeIds, labels,
        msg_link: msgUrl(mid),
        contact_link: contactUrl(cid)
      });
    }
  }

  console.log(`[RESUMEN] con_asociación=${withAssoc} sin_asociación=${withoutAssoc} (rows=${rows.length})`);
  rows.slice(0, 25).forEach((r, i) => {
    console.log(`[OK ${i+1}] msg=${r.msg_id} (${r.msg_numero}) → contact=${r.contact_id} (${r.contact_numero}) [${r.typeIds}/${r.labels}]`);
    if (PORTAL) console.log(`      ${r.msg_link}  |  ${r.contact_link}`);
  });

  const dir = path.join(process.cwd(), "reports");
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const outPath = path.join(dir, `assoc_pairs_with_numbers_${Date.now()}.csv`);
  fs.writeFileSync(outPath, toCsv(rows), "utf8");
  console.log(`[CSV] generado: ${outPath}`);
  console.log("== Fin reporte ==");
}

main().catch(e => {
  console.error(e?.response?.data ?? e.message ?? e);
  process.exit(1);
});

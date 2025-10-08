// src/tools/check-assoc-fast.js
import "dotenv/config";
import axios from "axios";
import fs from "fs";
import path from "path";
import { resolveObjectTypeId } from "../sinks/hubspotAssoc.js";

const TOKEN = process.env.HUBSPOT_TOKEN;
const MSG_OBJECT = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const CONTACTS_OBJECT = (process.env.HUBSPOT_CONTACTS_OBJECT || "contacts").trim();

// Límite de mensajes a revisar (rápido). Sube si quieres.
const LIMIT = Number(process.env.CHECK_LIMIT || 1000);

// Si quieres contar solo asociaciones con un label específico (ej. 20: "Contactos"):
// exporta env: CHECK_LABEL_ID=20
const FILTER_LABEL_ID = process.env.CHECK_LABEL_ID ? Number(process.env.CHECK_LABEL_ID) : null;

const BATCH = 100;
const RATE_DELAYMS = Number(process.env.SYNC_RATE_DELAY_MS || 150);
const SHOW_SAMPLE = 20;
const OUT_DIR = "reports";

async function getPortalId() {
  try {
    const url = "https://api.hubapi.com/account-info/v3/details";
    const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
    return String(data?.portalId || data?.portal_id || "");
  } catch {
    return "";
  }
}

function recordUrl({ portalId, objectTypeId, recordId }) {
  if (!portalId || !objectTypeId || !recordId) return "";
  return `https://app.hubspot.com/contacts/${portalId}/record/${encodeURIComponent(objectTypeId)}/${recordId}`;
}

async function listMessagesPage({ after }) {
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(MSG_OBJECT)}`;
  const params = {
    limit: 100,
    properties: "id_mensaje_unico,numero,compania,hs_createdate",
    after
  };
  const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` }, params });
  return data;
}

async function fetchMessages(limitTotal) {
  const all = [];
  let after;
  while (all.length < limitTotal) {
    const page = await listMessagesPage({ after });
    const results = Array.isArray(page?.results) ? page.results : [];
    all.push(...results);
    after = page?.paging?.next?.after;
    console.log(`[LOAD] mensajes acumulados=${all.length}`);
    if (!after) break;
  }
  return all.slice(0, limitTotal);
}

async function batchReadAssociationsV4(fromTypeId, toTypeId, fromIds) {
  if (!fromIds.length) return [];
  const url = `https://api.hubapi.com/crm/v4/associations/${encodeURIComponent(fromTypeId)}/${encodeURIComponent(toTypeId)}/batch/read`;
  const inputs = fromIds.map(id => ({ id: String(id) }));
  const { data } = await axios.post(
    url,
    { inputs },
    { headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" } }
  );
  // Devuelve results: [{ fromId, to: [{ toObjectId, types: [{associationTypeId,...}] }] }]
  return Array.isArray(data?.results) ? data.results : [];
}

async function batchReadContacts(contactIds) {
  if (!contactIds.length) return new Map();
  const url = `https://api.hubapi.com/crm/v3/objects/contacts/batch/read`;
  const inputs = contactIds.map(id => ({ id: String(id) }));
  const { data } = await axios.post(
    url,
    {
      inputs,
      properties: ["firstname","lastname","email","numero_telefono_id_unico","compania"]
    },
    { headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" } }
  );
  const map = new Map();
  for (const r of (data?.results || [])) map.set(String(r.id), r.properties || {});
  return map;
}

function filterToByLabel(toArr) {
  if (!FILTER_LABEL_ID) return toArr;
  return toArr.filter(t => {
    const types = Array.isArray(t?.types) ? t.types : [];
    return types.some(tt => Number(tt?.associationTypeId) === FILTER_LABEL_ID);
  });
}

async function main() {
  console.log("== Check asociaciones (FAST) p_mensajes → contacts ==");
  const portalId = await getPortalId();

  const msgTypeId = await resolveObjectTypeId(MSG_OBJECT);
  const ctcTypeId = await resolveObjectTypeId(CONTACTS_OBJECT);

  const messages = await fetchMessages(LIMIT);
  console.log(`[MSG] leídos=${messages.length} (límite ${LIMIT})`);

  const msgIds = messages.map(m => String(m.id));
  const assocByMsg = new Map(); // msgId -> array de to (filtrados por label si aplica)

  for (let i = 0; i < msgIds.length; i += BATCH) {
    const slice = msgIds.slice(i, i + BATCH);
    const res = await batchReadAssociationsV4(msgTypeId, ctcTypeId, slice);
    for (const row of res) {
      const toList = Array.isArray(row?.to) ? filterToByLabel(row.to) : [];
      assocByMsg.set(String(row.fromId), toList);
    }
    console.log(`[ASSOC READ] ${Math.min(i + BATCH, msgIds.length)}/${msgIds.length}`);
    if (RATE_DELAYMS > 0) await new Promise(r => setTimeout(r, RATE_DELAYMS));
  }

  // recolectar contactos únicos
  const allContactIds = Array.from(
    new Set([].concat(...Array.from(assocByMsg.values()).map(arr => arr.map(t => String(t.toObjectId)))))
  );

  const contactMap = await batchReadContacts(allContactIds);

  // armar rows
  const rows = messages.map(m => {
    const props = m.properties || {};
    const msgId = String(m.id);
    const toList = assocByMsg.get(msgId) || [];
    const link = recordUrl({ portalId, objectTypeId: msgTypeId, recordId: msgId });

    const contactsPretty = toList.map(t => {
      const cid = String(t.toObjectId);
      const p = contactMap.get(cid) || {};
      const name = [p.firstname, p.lastname].filter(Boolean).join(" ").trim();
      // Si hay types con label, muéstralo
      const typeStr = Array.isArray(t.types) && t.types.length
        ? t.types.map(tt => `${tt.associationTypeId}`).join("|")
        : "";
      return `${cid} | ${p.numero_telefono_id_unico || ""} | ${name || "(sin nombre)"} | ${p.email || ""} | types=${typeStr}`;
    });

    return {
      msg_id: msgId,
      msg_numero: props.numero || "",
      msg_compania: props.compania || "",
      msg_link: link,
      contacts_count: toList.length,
      contacts_info: contactsPretty.join(" || ")
    };
  });

  const sin = rows.filter(r => r.contacts_count === 0);
  const con = rows.filter(r => r.contacts_count > 0);

  console.log(`\n[RESUMEN] sin_asociación=${sin.length} con_asociación=${con.length}`);
  if (con.length) {
    console.log(`\n--- Mensajes CON asociaciones (sample ${Math.min(SHOW_SAMPLE, con.length)}) ---`);
    con.slice(0, SHOW_SAMPLE).forEach(r => {
      console.log(`[MSG ${r.msg_id}] numero=${r.msg_numero} link=${r.msg_link}`);
      console.log(`   → contacts(${r.contacts_count}): ${r.contacts_info}`);
    });
  } else {
    console.log(`\n(No se encontraron asociaciones en el rango muestreado. Prueba sin filtro de label o ajusta CHECK_LIMIT.)`);
  }

  // CSV
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(OUT_DIR, `assoc_fast_${Date.now()}.csv`);
  const headers = ["msg_id","msg_numero","msg_compania","msg_link","contacts_count","contacts_info"];
  const csv = [
    headers.join(","),
    ...rows.map(r => headers.map(h => {
      const v = String(r[h] ?? "");
      return /[",\n]/.test(v) ? `"${v.replace(/"/g,'""')}"` : v;
    }).join(","))
  ].join("\n");
  fs.writeFileSync(outPath, csv);
  console.log(`\n[CSV] generado: ${outPath}`);
}

main().catch(e => {
  console.error("Check FAST error:", e?.response?.data ?? e.message ?? e);
});

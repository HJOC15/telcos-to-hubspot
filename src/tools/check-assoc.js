// src/tools/check-assoc.js
import "dotenv/config";
import axios from "axios";
import fs from "fs";
import path from "path";

const TOKEN = process.env.HUBSPOT_TOKEN;
const MSG_OBJECT = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const CONTACTS_OBJECT = "contacts";

const PAGE_LIMIT_MESSAGES = Number(process.env.CHECK_LIMIT || 500);
const SHOW_SAMPLE = 20;
const OUT_DIR = "reports";
const ONLY_ASSOC = String(process.env.CHECK_ONLY_ASSOC || "0") === "1";

async function getPortalId() {
  const url = "https://api.hubspot.com/account-info/v3/details";
  const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
  return String(data?.portalId || data?.portal_id || "");
}
async function getObjectTypeId(objectName) {
  if (/^\d+-\d+$/.test(objectName)) return objectName;
  const url = "https://api.hubapi.com/crm/v3/schemas";
  const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
  const list = Array.isArray(data?.results) ? data.results : [];
  const hit = list.find(s =>
    s?.name === objectName ||
    s?.fullyQualifiedName === objectName ||
    s?.labels?.singular === objectName ||
    s?.labels?.plural === objectName
  );
  return hit?.objectTypeId || objectName;
}
function recordUrl({ portalId, objectTypeId, recordId }) {
  if (!portalId || !objectTypeId || !recordId) return "";
  return `https://app.hubspot.com/contacts/${portalId}/record/${objectTypeId}/${recordId}`;
}

async function listMessagesPage({ after }) {
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(MSG_OBJECT)}`;
  const params = { limit: 100, properties: "id_mensaje_unico,numero,compania,hs_createdate", after };
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
    if (!after) break;
  }
  return all.slice(0, limitTotal);
}
async function batchReadAssociations(fromIds) {
  if (!fromIds.length) return [];
  const url = `https://api.hubapi.com/crm/v4/associations/${encodeURIComponent(MSG_OBJECT)}/${CONTACTS_OBJECT}/batch/read`;
  const inputs = fromIds.map(id => ({ id: String(id) }));
  const { data } = await axios.post(url, { inputs }, {
    headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" }
  });
  return data?.results || [];
}
async function batchReadContacts(contactIds) {
  if (!contactIds.length) return new Map();
  const url = `https://api.hubapi.com/crm/v3/objects/${CONTACTS_OBJECT}/batch/read`;
  const inputs = contactIds.map(id => ({ id: String(id) }));
  const { data } = await axios.post(url, {
    inputs,
    properties: ["firstname","lastname","email","numero_telefono_id_unico","compania"]
  }, {
    headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" }
  });
  const map = new Map();
  for (const r of (data?.results || [])) map.set(String(r.id), r.properties || {});
  return map;
}

async function main() {
  console.log("== Check asociaciones p_mensajes → contacts ==");
  const portalId = await getPortalId();
  const msgObjectTypeId = await getObjectTypeId(MSG_OBJECT);

  const messages = await fetchMessages(PAGE_LIMIT_MESSAGES);
  console.log(`[MSG] leídos=${messages.length} (mostrando hasta ${PAGE_LIMIT_MESSAGES})`);

  const msgIds = messages.map(m => String(m.id));
  const assocRows = [];
  for (let i = 0; i < msgIds.length; i += 100) {
    const slice = msgIds.slice(i, i + 100);
    const rows = await batchReadAssociations(slice);
    assocRows.push(...rows);
  }

  const msgIdToContactIds = new Map();
  for (const row of assocRows) {
    const toList = Array.isArray(row?.to) ? row.to : [];
    msgIdToContactIds.set(String(row.fromId), toList.map(t => String(t.toObjectId)));
  }

  const allContactIds = Array.from(new Set([].concat(...Array.from(msgIdToContactIds.values()))));
  const contactMap = await batchReadContacts(allContactIds);

  const rows = messages.map(m => {
    const props = m.properties || {};
    const mids = msgIdToContactIds.get(String(m.id)) || [];
    const msgLink = recordUrl({ portalId, objectTypeId: msgObjectTypeId, recordId: m.id });
    const contactsPretty = mids.map(cid => {
      const p = contactMap.get(cid) || {};
      const name = [p.firstname, p.lastname].filter(Boolean).join(" ").trim();
      return `${cid} | ${p.numero_telefono_id_unico || ""} | ${name || "(sin nombre)"} | ${p.email || ""}`;
    });
    return {
      msg_id: m.id,
      msg_numero: props.numero || "",
      msg_compania: props.compania || "",
      msg_created: props.hs_createdate || "",
      msg_link: msgLink,
      contacts_count: mids.length,
      contacts_info: contactsPretty.join(" || ")
    };
  });

  const sin = rows.filter(r => r.contacts_count === 0);
  const con = rows.filter(r => r.contacts_count > 0);

  if (ONLY_ASSOC) {
    console.log(`\n[SOLO ASOCIADOS] count=${con.length}`);
    con.slice(0, SHOW_SAMPLE).forEach(r => {
      console.log(`[MSG ${r.msg_id}] numero=${r.msg_numero} link=${r.msg_link}`);
      console.log(`   → contacts(${r.contacts_count}): ${r.contacts_info}`);
    });
  } else {
    console.log(`\n[RESUMEN] sin_asociación=${sin.length} con_asociación=${con.length}`);
    if (sin.length) {
      console.log(`\n--- Mensajes SIN asociaciones (sample ${Math.min(SHOW_SAMPLE, sin.length)}) ---`);
      sin.slice(0, SHOW_SAMPLE).forEach(r => {
        console.log(`[MSG ${r.msg_id}] numero=${r.msg_numero} compania=${r.msg_compania} link=${r.msg_link}`);
      });
    }
    if (con.length) {
      console.log(`\n--- Mensajes CON asociaciones (sample ${Math.min(SHOW_SAMPLE, con.length)}) ---`);
      con.slice(0, SHOW_SAMPLE).forEach(r => {
        console.log(`[MSG ${r.msg_id}] numero=${r.msg_numero} link=${r.msg_link}`);
        console.log(`   → contacts(${r.contacts_count}): ${r.contacts_info}`);
      });
    }
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(OUT_DIR, `assoc_audit_${Date.now()}.csv`);
  const headers = ["msg_id","msg_numero","msg_compania","msg_created","msg_link","contacts_count","contacts_info"];
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
  console.error("Check error:", e?.response?.data ?? e.message ?? e);
});

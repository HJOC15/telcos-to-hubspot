import "dotenv/config";
import axios from "axios";

const TOKEN = process.env.HUBSPOT_TOKEN;
const MSG_OBJECT = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const CONTACT_PROP = (process.env.HUBSPOT_ID_PROPERTY || "numero_telefono_id_unico").trim();

const MSG_LIMIT = Number(process.env.DIAG_MSG_LIMIT || 3000);
const CONTACT_LIMIT = Number(process.env.DIAG_CONTACT_LIMIT || 5000);

const onlyDigits = (s) => String(s || "").replace(/\D/g, "");

async function listAllMessages(limit) {
  const out = [];
  let after;
  while (out.length < limit) {
    const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(MSG_OBJECT)}`;
    const params = { limit: 100, properties: "numero", after };
    const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` }, params });
    out.push(...(data?.results || []));
    after = data?.paging?.next?.after;
    if (!after) break;
  }
  return out.slice(0, limit);
}

async function listAllContacts(limit) {
  const out = [];
  let after;
  while (out.length < limit) {
    const url = `https://api.hubapi.com/crm/v3/objects/contacts`;
    const params = { limit: 100, properties: CONTACT_PROP, after };
    const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` }, params });
    out.push(...(data?.results || []));
    after = data?.paging?.next?.after;
    if (!after) break;
  }
  return out.slice(0, limit);
}

async function main() {
  console.log("== Diagnóstico de matching de teléfonos ==");
  const msgs = await listAllMessages(MSG_LIMIT);
  const contacts = await listAllContacts(CONTACT_LIMIT);

  const msgSet = new Set();
  for (const m of msgs) {
    const num = String(m?.properties?.numero || "").trim();
    if (!num) continue;
    msgSet.add(onlyDigits(num));
  }

  const contactSet = new Set();
  for (const c of contacts) {
    const v = String(c?.properties?.[CONTACT_PROP] || "").trim();
    if (!v) continue;
    contactSet.add(onlyDigits(v));
  }

  let matches = 0;
  for (const dn of msgSet) if (contactSet.has(dn)) matches++;

  console.log(`[MSGS] distintos=${msgSet.size}`);
  console.log(`[CTCS] distintos=${contactSet.size}`);
  console.log(`[MATCH] por dígitos exactos = ${matches}`);

  // muestra 20 ejemplos que aparecen en mensajes pero no en contactos
  const notInContacts = [];
  for (const dn of msgSet) {
    if (!contactSet.has(dn)) {
      notInContacts.push(dn);
      if (notInContacts.length >= 20) break;
    }
  }
  console.log(`\n[Ejemplos no encontrados en contacts] (${notInContacts.length})`);
  console.log(notInContacts.join(", "));
}

main().catch(e => {
  console.error("Diagnóstico error:", e?.response?.data ?? e.message ?? e);
});

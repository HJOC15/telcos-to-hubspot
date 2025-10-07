// src/test-original.js
import "dotenv/config";
import { createRequire } from "module";

async function loadSdk() {
  try {
    const mod = await import("im-contactosms-sdk-js");
    return mod.default || mod.Client || mod.ImContactoClient || mod.ImContactosms;
  } catch (e) {
    // fallback si el paquete es CJS
    try {
      const req = createRequire(import.meta.url);
      const mod = req("im-contactosms-sdk-js");
      return mod.default || mod.Client || mod.ImContactoClient || mod.ImContactosms || mod;
    } catch (e2) {
      throw new Error(`No pude cargar el SDK: ${e2.message}`);
    }
  }
}

function makeClient(Client) {
  const orgId    = process.env.CLARO_ORG_ID;
  const apiKey   = process.env.CLARO_API_KEY;
  const apiSecret= process.env.CLARO_API_SECRET;
  if (!orgId || !apiKey || !apiSecret) throw new Error("Faltan CLARO_ORG_ID/CLARO_API_KEY/CLARO_API_SECRET en .env");
  return new Client({ organizationId: orgId, apiKey, apiSecret });
}

async function tryMany(name, fns) {
  for (const [label, fn] of fns) {
    try {
      const out = await fn();
      const arr = Array.isArray(out?.items || out) ? (out.items || out) : [];
      console.log(`OK ${name} (${label}) → count=${arr.length}`);
      if (arr.length) console.dir(arr[0], { depth: 5 });
      return arr;
    } catch (e) {
      console.log(`fail ${name} (${label}):`, e?.response?.data ?? e.message);
    }
  }
  return [];
}

(async () => {
  const Client = await loadSdk();
  const client = makeClient(Client);

  // prueba CONTACTS
  await tryMany("contacts", [
    ["contacts.list",   () => client?.contacts?.list?.({ limit: 5 })],
    ["contacts.getAll", () => client?.contacts?.getAll?.({ limit: 5 })],
    ["listContacts",    () => client?.listContacts?.({ limit: 5 })],
  ]);

  // prueba MESSAGES últimas 24h
  const endISO   = new Date().toISOString();
  const startISO = new Date(Date.now() - 24*60*60*1000).toISOString();

  await tryMany("messages", [
    ["messages.list",   () => client?.messages?.list?.({ limit: 5, startDate: startISO, endDate: endISO })],
    ["messages.search", () => client?.messages?.search?.({ limit: 5, startDate: startISO, endDate: endISO })],
    ["listMessages",    () => client?.listMessages?.({ limit: 5, startDate: startISO, endDate: endISO })],
  ]);
})().catch(e => {
  console.error("ERROR:", e?.response?.data ?? e.message);
  process.exit(1);
});

// src/jobs/sync_tigo_contacts.js
import axios from "axios";
import { tigoListContactsPaged } from "../providers/tigo.js";
import { batchUpsertCustomObject } from "../sinks/hubspotCustom.js";

const TOKEN    = process.env.HUBSPOT_TOKEN;
const OBJECT   = (process.env.HUBSPOT_CONTACTS_OBJECT || "contacts").trim(); // estándar: "contacts"
const ID_PROP  = (process.env.HUBSPOT_ID_PROPERTY || "numero_telefono_id_unico").trim(); // única
const BATCH_SIZE   = Number(process.env.SYNC_BATCH_SIZE || 100);
const RATE_DELAYMS = Number(process.env.SYNC_RATE_DELAY_MS || 250);

// ------------ helpers ------------
const onlyDigits = (s) => String(s || "").replace(/\D/g, "");
function toE164GT(input) {
  const d = onlyDigits(input);
  if (!d) return "";
  if (d.startsWith("502") && d.length === 11) return `+${d}`;
  if (d.length === 8) return `+502${d}`;
  if (String(input || "").startsWith("+")) return String(input);
  if (d.length === 11) return `+${d}`;
  return `+${d}`;
}
function splitName(full) {
  const s = String(full || "").trim();
  if (!s) return { firstname: "", lastname: "" };
  const parts = s.split(/\s+/);
  if (parts.length === 1) return { firstname: parts[0], lastname: "" };
  return { firstname: parts.slice(0, -1).join(" "), lastname: parts.slice(-1)[0] };
}

// ------------ preflight: validar propiedad única en Contacts ------------
async function preflightUniqueProp() {
  try {
    const url = `https://api.hubapi.com/crm/v3/properties/contacts/${encodeURIComponent(ID_PROP)}`;
    const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
    if (!data?.hasUniqueValue) {
      console.warn(`[HS:preflight] contacts.${ID_PROP} existe pero NO es única en HS (hasUniqueValue=false).`);
    } else {
      console.log(`[HS:preflight] contacts.${ID_PROP} existe y es única ✔`);
    }
  } catch (e) {
    console.warn("[HS:preflight] No se pudo leer la propiedad única de Contacts:", e?.response?.data ?? e.message);
  }
}

// ------------ mapeo Tigo -> HubSpot Contact ------------
function mapTigoContactToHS(c) {
  // ajusta estas llaves a tu payload real de Tigo
  const phoneRaw = c?.msisdn || c?.phone || c?.phoneNumber || c?.msisdnTo || c?.msisdnFrom;
  const phoneE164 = toE164GT(phoneRaw);
  const email = (c?.email || c?.mail || "").trim().toLowerCase() || undefined;
  const fullName = c?.name || c?.fullName || c?.nombre || "";
  const { firstname, lastname } = splitName(fullName);

  // Debe existir el idProperty en el payload:
  if (!phoneE164) return null;

  return {
    // idProperty único para upsert:
    [ID_PROP]: phoneE164,

    // propiedades estándar útiles de Contact:
    phone: phoneE164,
    mobilephone: phoneE164,
    email,
    firstname,
    lastname,

    // propiedad nueva pedida:
    compania: "Tigo",
  };
}

// ------------ envío por lotes sin 1x1 ------------
export async function runTigoContactsSync() {
  console.log("== Sync contactos Tigo → HubSpot ==");
  try {
    await preflightUniqueProp();

    // 1-index: páginas de 500, hasta 20 (o corta por last/totalPages)
    const contacts = await tigoListContactsPaged({
      pageSize: 500,
      maxPages: 20,
      startPage: 1,
    });

    const arr = Array.isArray(contacts) ? contacts : [];
    console.log(`[TIGO:contactos] recibidos=${arr.length} (paginado 1-index)`);

    const mapped = arr.map(mapTigoContactToHS).filter(Boolean);
    if (!mapped.length) {
      console.warn("[TIGO:contactos] nada para enviar — revisa el mapeo de teléfono y la propiedad única.");
      return console.log("== End Tigo contactos ==");
    }

    let sentTotal = 0;
    for (let i = 0; i < mapped.length; i += BATCH_SIZE) {
      const chunk = mapped.slice(i, i + BATCH_SIZE);

      // muy importante: el helper usará OBJECT="contacts" e ID_PROP=numero_telefono_id_unico
      const res = await batchUpsertCustomObject({
        token: TOKEN,
        objectType: OBJECT,   // "contacts"
        idProperty: ID_PROP,  // "numero_telefono_id_unico"
        records: chunk,
      });

      const sent = res?.sent || 0;
      sentTotal += sent;
      console.log(`[HS:batch][contacts] ${i + 1}-${i + chunk.length}/${mapped.length} enviados=${sent}`);

      if (RATE_DELAYMS > 0) await new Promise(r => setTimeout(r, RATE_DELAYMS));
    }

    console.log(`[TIGO→HS:contactos] enviados=${sentTotal}`);
  } catch (e) {
    const msg = e?.response?.data ?? e.message ?? e;
    console.error("[TIGO:contactos] error:", msg);
  }
  console.log("== End Tigo contactos ==");
}

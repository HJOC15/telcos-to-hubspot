// src/jobs/sync.js
import { claroListContacts } from "../providers/claro.js";
import { batchUpsertCustomObject } from "../sinks/hubspotCustom.js";

const TOKEN   = process.env.HUBSPOT_TOKEN;
const OBJECT  = (process.env.HUBSPOT_CONTACTS_OBJECT || "contacts").trim(); // usaremos el genérico con "contacts"
const ID_PROP = (process.env.HUBSPOT_ID_PROPERTY || "numero_telefono_id_unico").trim();

// 1) número base: tomamos msisdn o country_code+phone_number o phone_number, sólo dígitos
function numeroDesdeClaro(c) {
  const raw =
    c?.msisdn
    || (c?.country_code && c?.phone_number ? `${c.country_code}${c.phone_number}` : null)
    || c?.phone_number
    || "";
  return String(raw).replace(/\D/g, ""); // sólo números
}

// 2) E.164: anteponemos '+'
function toE164(digits) {
  if (!digits) return "";
  return digits.startsWith("+") ? digits : `+${digits}`;
}

// 3) mapeo Claro → Contacto (con nombre_vacio_{{numero}} cuando falte)
function mapClaroToContact(c) {
  const numeroDigits = numeroDesdeClaro(c);      // ej: "50259515736"
  if (!numeroDigits) return null;

  const phoneE164 = toE164(numeroDigits);        // ej: "+50259515736"

  const first = (c?.first_name || "").trim();
  const last  = (c?.last_name  || "").trim();
  const safeFirst = first || `nombre_vacio_${numeroDigits}`;
  const safeLast  = last  || `nombre_vacio_${numeroDigits}`;

  return {
    // estándar de Contactos
    phone: phoneE164,

    // tu propiedad de upsert (tipo teléfono) CON '+'
    [ID_PROP]: phoneE164,

    // opcional: compat si tienes otra propiedad en el portal
    numero_telefono_id: numeroDigits,

    // nombres
    firstname: safeFirst,
    lastname:  safeLast,

    // NUEVO: propiedad custom que quieres ver en Contactos
    compania: "Claro",
  };
}

export async function runSync() {
  console.log("== Sync start (Contactos Claro) ==");

  try {
    const list = await claroListContacts({ limit: 1000 });
    console.log(`[CLARO] recibidos=${Array.isArray(list) ? list.length : 0}`);

    const mapped = (Array.isArray(list) ? list : [])
      .map(mapClaroToContact)
      .filter(Boolean);

    if (!mapped.length) {
      console.warn("[CLARO] sin registros mapeados — revisa msisdn/country_code/phone_number.");
      return console.log("== Sync end ==");
    }

    // Filtra por la clave real de upsert (debe existir)
    const records = mapped.filter(r => r[ID_PROP]);
    console.log(`[CLARO] mapeados=${mapped.length} para upsert=${records.length} (idProperty=${ID_PROP})`);

    if (!TOKEN) {
      console.warn("[HS] Falta HUBSPOT_TOKEN. No se enviará nada.");
    } else if (records.length) {
      // 👇 Usamos el helper genérico (igual que Tigo) para que NO filtre props
      const res = await batchUpsertCustomObject({
        token: TOKEN,
        objectType: OBJECT,   // "contacts"
        idProperty: ID_PROP,  // "numero_telefono_id_unico"
        records
      });
      console.log(`[CLARO→HS:contacts] enviados=${res.sent} mode=${res.mode || (res.dryRun ? "dry-run" : "batch")} idProperty=${ID_PROP}`);
    } else {
      console.warn("[CLARO] ninguno tiene la clave de upsert. Revisa HUBSPOT_ID_PROPERTY y el mapeo.");
    }
  } catch (e) {
    console.error("[CLARO] error:", e?.response?.data ?? e.message);
  }

  console.log("== Sync end ==");
}

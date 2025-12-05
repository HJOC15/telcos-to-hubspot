// src/jobs/sync_messages.js
import { claroListMessages } from "../providers/claro.js";
import { batchUpsertCustomObject } from "../sinks/hubspotCustom.js";

const TOKEN   = process.env.HUBSPOT_TOKEN;
const OBJECT  = process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes";
const UNIQUE  = process.env.HUBSPOT_MESSAGES_ID_PROPERTY || "id_mensaje_unico";

// Helpers
const onlyDigits = (s) => String(s || "").replace(/\D/g, "");

// Normaliza a formato E.164 de Guatemala: +502XXXXXXXX
function toE164GT(raw) {
  const digits = onlyDigits(raw);
  if (!digits) return "";
  if (digits.startsWith("502") && digits.length === 11) return `+${digits}`;
  if (digits.length === 8) return `+502${digits}`;
  if (String(raw || "").trim().startsWith("+")) return String(raw).trim();
  if (digits.startsWith("502")) return `+${digits}`;
  return `+${digits}`;
}

// Mapea un mensaje de Claro → HubSpot
const REQUIRED = (process.env.HUBSPOT_MSG_REQUIRED_PROP || "id_mensaje").trim();

function mapClaroMessageToHS(m) {
  const msgId =
    m?.id || m?.message_id || m?.uid || m?.messageUid || m?.external_id || m?.profile_uid || m?.messageId || "";

  const numeroIn =
    m?.msisdn || m?.to ||
    (m?.country_code && m?.phone_number ? `${m.country_code}${m.phone_number}` : m?.phone_number) || "";

  const numeroE164 = toE164GT(numeroIn);

  const contenido = m?.text || m?.message || m?.body || m?.content || "";
  const estado    = m?.status || m?.state || m?.delivery_status || m?.deliveryStatus || "";
  const fecha =
    m?.created_at || m?.createdAt || m?.sent_at || m?.sentAt ||
    m?.timestamp || m?.delivered_at || m?.received_at || new Date().toISOString();

  const uniqueVal = String(msgId || `${onlyDigits(numeroE164)}-${fecha}`);

  const props = {
    [UNIQUE]: uniqueVal,

    // propiedad requerida tu HS (id_mensaje por defecto)
    [REQUIRED]: uniqueVal,

    numero: numeroE164,
    contenido: contenido || "(sin_contenido)",
    estado,
    fecha,
    compania: "Claro",
  };

  return props;
}


export async function runMessagesSync() {
  console.log("== Sync mensajes Claro → HubSpot (solo upsert, sin asociaciones) ==");
  try {
    const days = Number(process.env.CLARO_MESSAGES_DAYS || 30);

    // 1) Trae mensajes de Claro (SDK o firmado manual)
    const msgs = await claroListMessages({ limit: 500, days });
    const arr  = Array.isArray(msgs) ? msgs : [];
    console.log(`[CLARO:mensajes] recibidos=${arr.length} (últimos ${days} días)`);

    // 2) Mapea y filtra
    const mapped = arr
      .map(mapClaroMessageToHS)
      .filter(x => x[UNIQUE] && x.numero && x.numero.startsWith("+"));

    if (!mapped.length) {
      console.warn("[CLARO:mensajes] nada mapeado (revisa normalización a +502…).");
      return console.log("== End mensajes ==");
    }

    // 3) Upsert de mensajes (custom object) — **sin asociaciones aquí**
    const res = await batchUpsertCustomObject({
      token: TOKEN,
      objectType: OBJECT,
      idProperty: UNIQUE,
      records: mapped
    });
    console.log(`[CLARO→HS:mensajes] enviados=${res.sent} mode=${res.mode || (res.dryRun ? "dry-run" : "batch")}`);
  } catch (e) {
    console.error("[CLARO:mensajes] error:", e?.response?.data ?? e.message);
  }
  console.log("== End mensajes ==");
}

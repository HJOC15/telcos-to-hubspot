// src/jobs/sync_messages.js
import { claroListMessages } from "../providers/claro.js";
import { batchUpsertCustomObject } from "../sinks/hubspotCustom.js";
import { associateMessagesToContactsByPhone } from "../sinks/hubspotAssociations.js";

const TOKEN   = process.env.HUBSPOT_TOKEN;
const OBJECT  = process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes";
const UNIQUE  = process.env.HUBSPOT_MESSAGES_ID_PROPERTY || "id_mensaje_unico";

// Helpers
const onlyDigits = (s) => String(s || "").replace(/\D/g, "");

// Normaliza a formato E.164 de Guatemala: +502XXXXXXXX
function toE164GT(raw) {
  const digits = onlyDigits(raw);
  if (!digits) return "";
  // ya viene con 502 + 8 dígitos -> agrega "+"
  if (digits.startsWith("502") && digits.length === 11) return `+${digits}`;
  // viene solo con 8 dígitos locales -> antepone +502
  if (digits.length === 8) return `+502${digits}`;
  // si por alguna razón ya trae "+"
  if (String(raw || "").trim().startsWith("+")) return String(raw).trim();
  // fallback: si empieza con 502 pero longitud distinta, intenta forzar "+"
  if (digits.startsWith("502")) return `+${digits}`;
  // último recurso: agrega "+" al bloque de dígitos
  return `+${digits}`;
}

// Mapea un mensaje de Claro → HubSpot
function mapClaroMessageToHS(m) {
  // ID del mensaje (toma el primero que exista)
  const msgId =
    m?.id || m?.message_id || m?.uid || m?.messageUid || m?.external_id || m?.profile_uid || m?.messageId || "";

  // número: msisdn, to, o country+phone
  const numeroIn =
    m?.msisdn || m?.to ||
    (m?.country_code && m?.phone_number ? `${m.country_code}${m.phone_number}` : m?.phone_number) || "";

  // normaliza a +502XXXXXXXX
  const numeroE164 = toE164GT(numeroIn);

  // contenido / estado / fecha
  const contenido = m?.text || m?.message || m?.body || m?.content || "";
  const estado    = m?.status || m?.state || m?.delivery_status || m?.deliveryStatus || "";
  const fecha =
    m?.created_at || m?.createdAt || m?.sent_at || m?.sentAt ||
    m?.timestamp || m?.delivered_at || m?.received_at || new Date().toISOString();

  // propiedades para HubSpot (asegúrate que 'numero' es tu propiedad tipo phone)
  const props = {
    [UNIQUE]: String(msgId || `${onlyDigits(numeroE164)}-${fecha}`),
    numero: numeroE164,                    // ⬅️ AHORA VA EN +502…
    contenido: contenido || "(sin_contenido)",
    estado,
    fecha,                                 // ISO recomendado si tu propiedad es datetime
  };

  return props;
}

export async function runMessagesSync() {
  console.log("== Sync mensajes Claro → HubSpot ==");
  try {
    const days = Number(process.env.CLARO_MESSAGES_DAYS || 30);

    // 1) Trae mensajes de Claro (SDK)
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

    // 3) Upsert de mensajes (custom object)
    const res = await batchUpsertCustomObject({
      token: TOKEN,
      objectType: OBJECT,
      idProperty: UNIQUE,
      records: mapped
    });
    console.log(`[CLARO→HS:mensajes] enviados=${res.sent} mode=${res.mode || (res.dryRun ? "dry-run" : "batch")}`);

    // 4) Asociaciones Mensaje → Contacto por número (+502…)
    const rowsForAssoc = mapped.map(r => ({
      mensajeIdValue: r[UNIQUE],
      numero: r.numero
    }));

    const assoc = await associateMessagesToContactsByPhone(rowsForAssoc);
    console.log(`[CLARO→HS:asociaciones] creadas=${assoc.created} saltadas=${assoc.skipped}`);
  } catch (e) {
    console.error("[CLARO:mensajes] error:", e?.response?.data ?? e.message);
  }
  console.log("== End mensajes ==");
}

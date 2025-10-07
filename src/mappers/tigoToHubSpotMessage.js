// src/mappers/tigoToHubSpotMessage.js
function toEpochMs(s) {
  if (!s) return undefined;
  const t = Date.parse(s);
  return Number.isFinite(t) ? t : undefined;
}

function toE164(msisdn) {
  if (!msisdn) return undefined;
  const s = String(msisdn);
  return s.startsWith("+") ? s : (s.startsWith("502") ? "+" + s : "+502" + s);
}

/**
 * Ajusta las propiedades a las que SÍ existen en tu objeto p_mensajes.
 * NO mandamos: shortcode, protocolo, tipo_envio (dan PROPERTY_DOESNT_EXIST)
 */
export function mapTigoMsgToHS(msg) {
  const fecha =
    toEpochMs(msg?.sentAt) ||
    toEpochMs(msg?.createdDate) ||
    undefined;

  return {
    // id de upsert (ya configuraste en .env):
    // HUBSPOT_MESSAGES_ID_PROPERTY = id_mensaje_unico
    id_mensaje_unico: `tigo:${msg?.id}`,   // deja rastro de origen

    // requerido por tu env:
    // HUBSPOT_MSG_REQUIRED_PROP = id_mensaje
    id_mensaje: String(msg?.id || ""),

    // el resto: usa sólo las que ya te funcionaron con Claro
    // (ajusta nombres a tus propiedades reales en p_mensajes)
    msisdn: toE164(msg?.msisdn),           // o "telefono", según tu schema real
    cuerpo: msg?.body,                     // si tu schema lo llama "mensaje", cambia aquí
    fecha                                 : fecha,  // long (epoch ms)
    // ⚠ NO enviar: shortcode, protocolo, tipo_envio
  };
}

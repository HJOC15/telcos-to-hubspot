// src/mappers/tigoToClaroSchema.js

function safeISO(...candidates) {
  for (const c of candidates) {
    if (!c) continue;
    const t = Date.parse(c);
    if (Number.isFinite(t)) return new Date(t).toISOString();
  }
  return undefined;
}

function cleanProps(obj) {
  const out = {};
  for (const k of Object.keys(obj || {})) {
    const v = obj[k];
    if (v === undefined || v === null) continue;
    // HubSpot a veces se confunde si le mandas objetos anidados
    if (typeof v === "object") continue;
    out[k] = String(v);
  }
  return out;
}

/** Mapea un registro Tigo al esquema que ya usas en HubSpot (como Claro) */
export function mapTigoMsgToClaro(msg) {
  const estado = msg?.sentAt ? "SENT" : "QUEUED";

  const props = cleanProps({
    mensaje_id: String(msg?.id || ""),
    numero: String(msg?.msisdn || ""),
    contenido: msg?.body || "",
    estado,
    // ISO como en tu ejemplo de Claro
    fecha: safeISO(msg?.sentAt, msg?.createdDate),
  });

  return {
    id: props.mensaje_id,   // <- sólo 'id'
    properties: props,      // <- y 'properties'
  };
}

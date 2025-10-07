// src/mappers/tigoToHS.js

function parseToEpochMillis(...candidates) {
  for (const c of candidates) {
    if (!c) continue;
    // soporta "YYYY-MM-DD HH:mm:ss" y ISO
    // si viene con espacio, lo convertimos a ISO Z para forzar UTC
    const normalized = typeof c === "string" && c.includes(" ")
      ? c.replace(" ", "T") + "Z"
      : c;
    const t = Date.parse(normalized);
    if (Number.isFinite(t)) return t; // ms epoch
  }
  return undefined;
}

function withPlus(msisdn) {
  if (!msisdn) return undefined;
  const s = String(msisdn).trim();
  return s.startsWith("+") ? s : `+${s}`;
}

/**
 * Mapea un mensaje Tigo → HubSpot (según formato que pediste)
 * - id → id
 * - idProperty → "id_mensaje" (repetido por item como lo quieres)
 * - properties:
 *    id_mensaje → id
 *    numero     → +msisdn
 *    contenido  → body
 *    estado     → "SENT"
 *    fecha      → epoch long (ms) usando createdDate (fallback sentAt)
 */
export function mapTigoMsgToHS(msg, { idProperty = "id_mensaje" } = {}) {
  const id = String(msg?.id || "").trim();
  const numero = withPlus(msg?.msisdn);
  const contenido = msg?.body ?? "";
  const estado = "SENT";

  // createdDate tiene el valor que pediste; si no viniera, sentAt como respaldo
  const fecha = parseToEpochMillis(msg?.createdDate, msg?.sentAt);

  return {
    id,
    idProperty, // <- lo quieres también por item
    properties: {
      [idProperty]: id,
      numero,
      contenido,
      estado,
      fecha,
    },
  };
}

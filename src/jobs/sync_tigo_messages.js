// src/jobs/sync_tigo_messages.js
import { tigoListMessagesPaged } from "../providers/tigo.js";
import { batchUpsertCustomObject } from "../sinks/hubspotCustom.js";

const TOKEN    = process.env.HUBSPOT_TOKEN;
const OBJECT   = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const UNIQUE   = (process.env.HUBSPOT_MESSAGES_ID_PROPERTY || "id_mensaje_unico").trim();
const REQUIRED = (process.env.HUBSPOT_MSG_REQUIRED_PROP || "id_mensaje").trim();

const BATCH_SIZE   = Number(process.env.SYNC_BATCH_SIZE || 100);
const RATE_DELAYMS = Number(process.env.SYNC_RATE_DELAY_MS || 250);

// Helpers
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
function toEpochMs(v) {
  if (!v) return Date.now();
  if (typeof v === "number") return v < 1e12 ? v * 1000 : v;
  if (v instanceof Date) return v.getTime();
  const s = String(v).trim();
  let ts = Date.parse(s);
  if (!isNaN(ts)) return ts < 1e12 ? ts * 1000 : ts;
  const maybeIso = s.replace(" ", "T") + "Z";
  ts = Date.parse(maybeIso);
  return isNaN(ts) ? Date.now() : ts;
}

// Clave única: usa el ID nativo si está presente; si no, número+fecha.
function makeUniqueKey(m) {
  const rawId = String(m?.id || m?.messageId || m?.uuid || "").trim();
  if (rawId) return rawId;
  const numero  = toE164GT(m?.msisdn || m?.msisdnTo || m?.msisdnFrom);
  const whenMs  = toEpochMs(m?.sentAt || m?.createdDate || m?.timestamp);
  return `${numero}_${whenMs}`;
}

// Map Tigo -> HubSpot (incluye propiedad nueva `compania`)
function mapTigoMessageToHS(m) {
  const numeroE164   = toE164GT(m?.msisdn || m?.msisdnTo || m?.msisdnFrom);
  const fechaEpochMs = toEpochMs(m?.sentAt || m?.createdDate || m?.timestamp);
  const estado       = String(m?.status ?? "");
  const unique       = makeUniqueKey(m);
  const tigoId       = String(m?.id || m?.messageId || m?.uuid || unique);

  return {
    [UNIQUE]: unique,
    [REQUIRED]: tigoId,
    numero: numeroE164,
    contenido: m?.body || m?.message || "(sin_contenido)",
    estado,
    fecha: fechaEpochMs,
    compania: "Tigo"   // Asegúrate de que la propiedad exista en HS con id EXACTO "compania"
  };
}

export async function runTigoMessagesSync() {
  console.log("== Sync mensajes Tigo → HubSpot ==");
  try {
    // Pide 20 páginas de 500 empezando en page=1 (1-index)
    const msgs = await tigoListMessagesPaged({
      direction: "MT",
      pageSize: 500,
      maxPages: 20,
      startPage: 1
    });

    const arr = Array.isArray(msgs) ? msgs : [];
    console.log(`[TIGO:mensajes] recibidos=${arr.length} (paginado 1-index)`);

    const mapped = arr.map(mapTigoMessageToHS).filter(x => x?.[UNIQUE] && x?.[REQUIRED]);
    if (!mapped.length) {
      console.warn("[TIGO:mensajes] nada para enviar — revisa mapeo/propiedades en HubSpot.");
      return console.log("== End Tigo mensajes ==");
    }

    let sentTotal = 0;
    for (let i = 0; i < mapped.length; i += BATCH_SIZE) {
      const chunk = mapped.slice(i, i + BATCH_SIZE);

      const res = await batchUpsertCustomObject({
        token: TOKEN,
        objectType: OBJECT,   // si tienes el objectTypeId (ej. "2-123456"), lo puedes usar aquí
        idProperty: UNIQUE,
        records: chunk
      });

      const sent = res?.sent || 0;
      sentTotal += sent;
      console.log(`[HS:batch] ${i + 1}-${i + chunk.length}/${mapped.length} enviados=${sent}`);

      if (RATE_DELAYMS > 0) await new Promise(r => setTimeout(r, RATE_DELAYMS));
    }

    console.log(`[TIGO→HS:mensajes] enviados=${sentTotal}`);
  } catch (e) {
    const msg = e?.response?.data ?? e.message ?? e;
    console.error("[TIGO:mensajes] error:", msg);
  }
  console.log("== End Tigo mensajes ==");
}

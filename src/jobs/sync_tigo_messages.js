// src/jobs/sync_tigo_messages.js
import axios from "axios";
import { tigoListMessages } from "../providers/tigo.js";
import { batchUpsertCustomObject } from "../sinks/hubspotCustom.js";

const TOKEN    = process.env.HUBSPOT_TOKEN;
const OBJECT   = (process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes").trim();
const UNIQUE   = (process.env.HUBSPOT_MESSAGES_ID_PROPERTY || "id_mensaje_unico").trim();
const REQUIRED = (process.env.HUBSPOT_MSG_REQUIRED_PROP || "id_mensaje").trim();

const DEBUG        = String(process.env.DEBUG_SYNC || "0") === "1";
const DO_DEDUPE    = String(process.env.SYNC_DEDUPE || "1") === "1";
const BATCH_SIZE   = Number(process.env.SYNC_BATCH_SIZE || 100);
const RATE_DELAYMS = Number(process.env.SYNC_RATE_DELAY_MS || 300);

// ---------- helpers básicos ----------
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
function djb2(str) { let h=5381; for (let i=0;i<str.length;i++) h=((h<<5)+h)+str.charCodeAt(i); return (h>>>0).toString(36); }

// ---------- clave única compuesta ----------
function makeUniqueKey(m) {
  const numero  = toE164GT(m?.msisdn || m?.msisdnTo || m?.msisdnFrom);
  const rawDate = m?.sentAt || m?.createdDate || m?.timestamp || "";
  const whenMs  = toEpochMs(rawDate);
  const dir     = String(m?.direction || "");
  const channel = String(m?.channel || m?.shortCode || "");
  const content = String(m?.body || m?.message || "");
  const base    = `${numero}|${rawDate}|${dir}|${channel}|${content.slice(0,64)}`;
  const h       = djb2(base);
  return `${numero}_${whenMs}_${dir}_${h}`;
}

// ---------- mapping ----------
function mapTigoMessageToHS(m) {
  const numeroE164   = toE164GT(m?.msisdn || m?.msisdnTo || m?.msisdnFrom);
  const fechaEpochMs = toEpochMs(m?.sentAt || m?.createdDate || m?.timestamp);
  const estado       = String(m?.status ?? "");
  const unique       = makeUniqueKey(m);
  const tigoId       = String(m?.id || m?.messageId || m?.uuid || "");

  return {
    __rawId: tigoId, // <-- guardamos el id de Tigo para imprimirlo
    [UNIQUE]: unique,
    [REQUIRED]: tigoId || unique,
    numero: numeroE164,
    contenido: m?.body || m?.message || "(sin_contenido)",
    estado,
    fecha: fechaEpochMs
  };
}

function dedupeByUnique(arr) {
  const seen = new Set(); const out = [];
  for (const x of arr) { const k = x?.[UNIQUE]; if (!k || seen.has(k)) continue; seen.add(k); out.push(x); }
  return out;
}

// ---------- diagnóstico: imprimir IDs crudos y duplicados ----------
function printRawIdDuplicates(raw) {
  // preferimos m.id; si no hay, intentamos messageId/uuid; si tampoco, msisdn+fecha cruda
  const keyOf = (m) => {
    const id = String(m?.id || m?.messageId || m?.uuid || "").trim();
    if (id) return id;
    const ms = String(m?.msisdn || m?.msisdnTo || m?.msisdnFrom || "");
    const dt = String(m?.sentAt || m?.createdDate || m?.timestamp || "");
    return `${ms}|${dt}`;
  };

  const freq = new Map();
  for (const m of raw) {
    const k = keyOf(m);
    freq.set(k, (freq.get(k) || 0) + 1);
  }

  const entries = Array.from(freq.entries()).sort((a,b)=>b[1]-a[1]);
  const dup = entries.filter(([,c]) => c > 1);
  console.log(`[IDS] total_registros=${raw.length} ids_distintos=${entries.length} ids_duplicados=${dup.length}`);

  if (dup.length) {
    console.log(`[IDS] Top 20 IDs duplicados (id → repeticiones):`);
    dup.slice(0,20).forEach(([k,c],i)=>console.log(`  ${i+1}. ${k} → ${c}`));
  }

  // Si tu jefe quiere ver TODOS los IDs crudos, imprímelos (puede ser mucho):
  // entries.forEach(([k,c]) => console.log(`[IDS] ${k} → ${c}`));
}

// ---------- impresión por registro (cola de envío) ----------
function printEachRecord(records, offsetStart, total, idProperty) {
  for (let idx = 0; idx < records.length; idx++) {
    const r = records[idx];
    const n = offsetStart + idx + 1;
    const rawId = r.__rawId || "(sin_rawId)";
    console.log(`[ADD] ${n}/${total} rawId="${rawId}" ${idProperty}="${r[idProperty]}"`);
  }
}

// ---------- resolver object type ----------
async function resolveObjectTypeId() {
  if (/^\d+-\d+$/.test(OBJECT)) return OBJECT;
  try {
    const { data } = await axios.get("https://api.hubapi.com/crm/v3/schemas", {
      headers: { Authorization: `Bearer ${TOKEN}` }
    });
    const all = Array.isArray(data?.results) ? data.results : [];
    const hit = all.find(s =>
      s?.name === OBJECT || s?.fullyQualifiedName === OBJECT || s?.labels?.singular === OBJECT || s?.labels?.plural === OBJECT
    );
    if (hit?.objectTypeId) return hit.objectTypeId;
  } catch {}
  return OBJECT;
}

// ---------- preflight de propiedad única (mismo objectType) ----------
async function preflight(objectTypeId) {
  try {
    const url = `https://api.hubapi.com/crm/v3/properties/${encodeURIComponent(objectTypeId)}/${encodeURIComponent(UNIQUE)}`;
    const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
    const exists = !!data?.name; const isUnique = !!data?.hasUniqueValue;
    if (!exists)  console.warn(`[HS:preflight] ${objectTypeId}.${UNIQUE} NO existe en HubSpot.`);
    else if (!isUnique) console.warn(`[HS:preflight] ${objectTypeId}.${UNIQUE} existe pero NO es única.`);
    else console.log(`[HS:preflight] ${objectTypeId}.${UNIQUE} existe y es única ✔`);
  } catch (e) {
    console.warn("[HS:preflight] No se pudo verificar la propiedad única:", e?.response?.data ?? e.message);
  }
}

// ---------- envío ----------
async function sendChunk({ token, objectType, idProperty, records, offsetStart, total }) {
  // imprime cada ID de este batch
  printEachRecord(records, offsetStart, total, idProperty);

  const res = await batchUpsertCustomObject({
    token,
    objectType,
    idProperty,
    records
  });

  const sent = res?.sent ?? 0;
  console.log(`[HS:batch] enviados=${sent} de ${records.length} (rango ${offsetStart + 1}-${offsetStart + records.length})`);

  return res;
}

export async function runTigoMessagesSync() {
  console.log("== Sync mensajes Tigo → HubSpot ==");
  try {
    const objectTypeId = await resolveObjectTypeId();
    console.log(`[HS] objectType usado para batch: ${objectTypeId}`);
    await preflight(objectTypeId);

    const days = Number(process.env.TIGO_B2B_DAYS || 7);
    const pageSize = Number(process.env.TIGO_PAGE_SIZE || 100);

    const msgs = await tigoListMessages({ direction: "MT", pageSize, days });
    const arr = Array.isArray(msgs) ? msgs : [];
    console.log(`[TIGO:mensajes] recibidos=${arr.length} (últimos ${days} días)`);

    // 1) Imprime duplicados por ID crudo de Tigo
    printRawIdDuplicates(arr);

    // 2) Mapear y reportar unicidad generada
    const mapped = arr.map(mapTigoMessageToHS).filter(x => x?.[UNIQUE] && x?.[REQUIRED]);

    const freq = new Map();
    for (const r of mapped) freq.set(r[UNIQUE], (freq.get(r[UNIQUE]) || 0) + 1);
    const uniqCount = freq.size;
    const dupGen = Array.from(freq.entries()).filter(([,c]) => c>1);
    console.log(`[GEN] ${UNIQUE} generados: únicos=${uniqCount} clusters_duplicados=${dupGen.length}`);
    if (dupGen.length) {
      console.log(`[GEN] Top 10 ${UNIQUE} duplicados:`);
      dupGen.sort((a,b)=>b[1]-a[1]).slice(0,10).forEach(([k,c],i)=>console.log(`  ${i+1}. ${k} → ${c}`));
    }

    // 3) Dedup opcional antes de enviar
    const toSend = DO_DEDUPE ? dedupeByUnique(mapped) : mapped;
    if (DO_DEDUPE) {
      const dupCount = mapped.length - toSend.length;
      if (dupCount > 0) console.log(`[TIGO→HS:mensajes] duplicados descartados=${dupCount}`);
    } else {
      console.log(`[TIGO→HS:mensajes] DEDUPE OFF → enviando ${toSend.length} registros`);
    }
    if (!toSend.length) {
      console.warn("[TIGO:mensajes] nada para enviar — revisa IDs.");
      return console.log("== End Tigo mensajes ==");
    }

    // 4) Envío por lotes, imprimiendo IDs de cada registro
    let sentTotal = 0;
    for (let i = 0; i < toSend.length; i += BATCH_SIZE) {
      const chunk = toSend.slice(i, i + BATCH_SIZE);
      const res = await sendChunk({
        token: TOKEN,
        objectType: objectTypeId,
        idProperty: UNIQUE,
        records: chunk,
        offsetStart: i,
        total: toSend.length
      });
      sentTotal += res?.sent || 0;
      if (RATE_DELAYMS > 0) await new Promise(r => setTimeout(r, RATE_DELAYMS));
    }

    console.log(`[TIGO→HS:mensajes] enviados=${sentTotal}`);
  } catch (e) {
    const msg = e?.response?.data ?? e.message ?? e;
    console.error("[TIGO:mensajes] error:", msg);
  }
  console.log("== End Tigo mensajes ==");
}

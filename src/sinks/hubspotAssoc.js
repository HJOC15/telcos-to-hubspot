// src/sinks/hubspotAssoc.js
import axios from "axios";
const TOKEN = process.env.HUBSPOT_TOKEN;

function normalizeEnvKey(name) {
  return String(name || "").toUpperCase().replace(/[^A-Z0-9]/g, "_");
}

/** Lee override de tipo: HUBSPOT_<NOMBRE>_OBJECT_TYPEID  (ej: HUBSPOT_P_MENSAJES_OBJECT_TYPEID=2-1234567) */
function readOverrideFor(nameOrId) {
  const key = `HUBSPOT_${normalizeEnvKey(nameOrId)}_OBJECT_TYPEID`;
  return process.env[key] || "";
}

/** Resuelve objectTypeId para un nombre o devuelve el mismo si ya es id */
export async function resolveObjectTypeId(objectNameOrId) {
  // 1) si ya viene como 2-XXXXX
  if (/^\d+-\d+$/.test(objectNameOrId)) return objectNameOrId;

  // 2) override por ENV
  const ov = readOverrideFor(objectNameOrId);
  if (ov) return ov;

  // 3) contactos (varios alias) → 0-1
  const nameLc = String(objectNameOrId).trim().toLowerCase();
  if (nameLc === "contacts" || nameLc === "contact" || nameLc === "contactos" || nameLc === "0-1") {
    return "0-1";
  }

  // 4) buscar en /crm/v3/schemas
  const url = "https://api.hubapi.com/crm/v3/schemas";
  const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
  const list = Array.isArray(data?.results) ? data.results : [];
  const hit = list.find(s =>
    s?.name === objectNameOrId ||
    s?.fullyQualifiedName === objectNameOrId ||
    s?.labels?.singular === objectNameOrId ||
    s?.labels?.plural === objectNameOrId
  );
  if (!hit?.objectTypeId) {
    throw new Error(`[ASSOC] No se pudo resolver objectTypeId para "${objectNameOrId}". ` +
      `Opciones: poner HUBSPOT_${normalizeEnvKey(objectNameOrId)}_OBJECT_TYPEID=2-XXXXX en .env, ` +
      `o revisa Settings → Data Model para copiar su ID.`);
  }
  return hit.objectTypeId;
}

async function fetchLabelsRaw(fromId, toId) {
  const url = `https://api.hubspot.com/crm/v4/associations/${encodeURIComponent(fromId)}/${encodeURIComponent(toId)}/labels`;
  const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
  return Array.isArray(data?.results) ? data.results : [];
}

/** Normaliza estructura de label (soporta associationTypeId | typeId | id) */
function normalizeLabels(rawList = []) {
  return rawList.map(l => {
    const associationTypeId =
      l?.associationTypeId ??
      l?.typeId ??
      l?.id ?? // algunos portales devuelven 'id'
      null;

    return {
      associationTypeId,
      label: l?.label || l?.name || "",
      name: l?.name || l?.label || "",
      raw: l
    };
  }).filter(x => x.associationTypeId != null);
}

/** Intenta A->B; si no hay labels válidos, intenta B->A y marca reversed=true */
export async function getAssociationTypeIdEitherDirection(fromObject, toObject) {
  const prefer = (process.env.HUBSPOT_ASSOC_LABEL_MSG_TO_CONTACT || "").trim().toLowerCase();

  const fromId = await resolveObjectTypeId(fromObject);
  const toId   = await resolveObjectTypeId(toObject);

  // A -> B
  let rawA = await fetchLabelsRaw(fromId, toId);
  let A = normalizeLabels(rawA);

  if (A.length) {
    let chosen = A[0];
    if (prefer) {
      const hit = A.find(l => l.label.trim().toLowerCase() === prefer || l.name.trim().toLowerCase() === prefer);
      if (hit) chosen = hit;
    }
    console.log(`[ASSOC][A->B] labels: ${A.map(l => `${l.associationTypeId}:${l.label||l.name}`).join(", ")}`);
    console.log(`[ASSOC][A->B] elegido: ${chosen.associationTypeId}:${chosen.label || chosen.name}`);
    return { associationTypeId: chosen.associationTypeId, fromId, toId, reversed: false };
  }

  // B -> A (reversa)
  let rawB = await fetchLabelsRaw(toId, fromId);
  let B = normalizeLabels(rawB);

  if (!B.length) {
    console.log("[ASSOC] Respuesta cruda sin normalizar (A->B):", JSON.stringify(rawA, null, 2));
    console.log("[ASSOC] Respuesta cruda sin normalizar (B->A):", JSON.stringify(rawB, null, 2));
    throw new Error(`[ASSOC] No hay labels ni en ${fromId}->${toId} ni en ${toId}->${fromId}. Crea al menos un label en el portal.`);
  }

  let chosen = B[0];
  if (prefer) {
    const hit = B.find(l => l.label.trim().toLowerCase() === prefer || l.name.trim().toLowerCase() === prefer);
    if (hit) chosen = hit;
  }
  console.log(`[ASSOC][B->A] labels: ${B.map(l => `${l.associationTypeId}:${l.label||l.name}`).join(", ")}`);
  console.log(`[ASSOC][B->A] elegido: ${chosen.associationTypeId}:${chosen.label || chosen.name}`);
  return { associationTypeId: chosen.associationTypeId, fromId: toId, toId: fromId, reversed: true };
}

/** Crea asociaciones respetando la dirección indicada */
export async function batchAssociateDirected({ fromId, toId, associationTypeId, pairs }) {
  const url = `https://api.hubspot.com/crm/v4/associations/${encodeURIComponent(fromId)}/${encodeURIComponent(toId)}/batch/create`;
  const headers = { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" };

  const typeVal = typeof associationTypeId === "number" ? associationTypeId : Number(associationTypeId);
  const inputs = pairs.map(([fromRecId, toRecId]) => ({
    from: { id: String(fromRecId) },
    to:   { id: String(toRecId) },
    type: typeVal
  }));

  const { data } = await axios.post(url, { inputs }, { headers });
  return data;
}

// src/sinks/hubspotAssoc.js
import axios from "axios";
const TOKEN = process.env.HUBSPOT_TOKEN;

function normalizeEnvKey(name) {
  return String(name || "").toUpperCase().replace(/[^A-Z0-9]/g, "_");
}

function readOverrideFor(nameOrId) {
  const key = `HUBSPOT_${normalizeEnvKey(nameOrId)}_OBJECT_TYPEID`;
  return process.env[key] || "";
}

export async function resolveObjectTypeId(objectNameOrId) {
  if (/^\d+-\d+$/.test(objectNameOrId)) return objectNameOrId;

  const ov = readOverrideFor(objectNameOrId);
  if (ov) return ov;

  const nameLc = String(objectNameOrId).trim().toLowerCase();
  if (["contacts","contact","contactos","0-1"].includes(nameLc)) return "0-1";

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
      `Pon HUBSPOT_${normalizeEnvKey(objectNameOrId)}_OBJECT_TYPEID=2-XXXXX en .env o revisa Settings → Data Model.`);
  }
  return hit.objectTypeId;
}

async function fetchLabelsRaw(fromId, toId) {
  const url = `https://api.hubspot.com/crm/v4/associations/${encodeURIComponent(fromId)}/${encodeURIComponent(toId)}/labels`;
  const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
  return Array.isArray(data?.results) ? data.results : [];
}

function normalizeLabels(rawList = []) {
  return rawList.map(l => {
    const associationTypeId = l?.associationTypeId ?? l?.typeId ?? l?.id ?? null;
    return {
      associationTypeId,
      label: (l?.label || l?.name || "").trim(),
      name: (l?.name || l?.label || "").trim(),
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
  let A = normalizeLabels(await fetchLabelsRaw(fromId, toId));
  if (A.length) {
    let chosen = pickLabel(A, prefer);
    console.log(`[ASSOC][A->B] labels: ${A.map(l => `${l.associationTypeId}:${l.label||l.name}`).join(", ")}`);
    console.log(`[ASSOC][A->B] elegido: ${chosen.associationTypeId}:${chosen.label || chosen.name}`);
    return { associationTypeId: chosen.associationTypeId, fromId, toId, reversed: false };
  }

  // B -> A (reversa)
  let B = normalizeLabels(await fetchLabelsRaw(toId, fromId));
  if (!B.length) {
    throw new Error(`[ASSOC] No hay labels ni en ${fromId}->${toId} ni en ${toId}->${fromId}. Crea un Association Label.`);
  }
  let chosen = pickLabel(B, prefer);
  console.log(`[ASSOC][B->A] labels: ${B.map(l => `${l.associationTypeId}:${l.label||l.name}`).join(", ")}`);
  console.log(`[ASSOC][B->A] elegido: ${chosen.associationTypeId}:${chosen.label || chosen.name}`);
  return { associationTypeId: chosen.associationTypeId, fromId: toId, toId: fromId, reversed: true };
}

function pickLabel(list, preferLc) {
  // 1) si hay preferencia .env, intenta por label o name
  if (preferLc) {
    const hit = list.find(l => l.label.toLowerCase() === preferLc || l.name.toLowerCase() === preferLc);
    if (hit) return hit;
  }
  // 2) evita labels vacíos si es posible
  const nonEmpty = list.find(l => l.label || l.name);
  return nonEmpty || list[0];
}

/** Crea asociaciones y devuelve conteo real */
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
  // La API puede no devolver “counts” directos; intentamos inferir
  const ok = Array.isArray(data?.results) ? data.results.length : inputs.length;
  const errs = Array.isArray(data?.errors) ? data.errors.length : 0;

  if (errs) {
    console.warn(`[ASSOC][WARN] errores en batch: ${errs} (ok=${ok})`);
    // dump corto de los primeros errores
    console.warn(JSON.stringify((data?.errors || []).slice(0, 3), null, 2));
  }
  return { created: ok, errors: errs, raw: data };
}

export async function getAssociationTypeId(fromObject, toObject) {
  const { associationTypeId, fromId, toId } =
    await getAssociationTypeIdEitherDirection(fromObject, toObject);
  return {
    associationTypeId,
    fromObjectId: fromId,
    toObjectId: toId,
  };
}

// Puente: crea asociaciones usando v4 con el campo "types" (USER_DEFINED).
// pairs: Array<[fromRecordId, toRecordId]>
export async function batchAssociate({
  fromObject,
  toObject,
  associationTypeId,
  pairs,
}) {
  if (!Array.isArray(pairs) || pairs.length === 0) return { created: 0 };

  const fromId = await resolveObjectTypeId(fromObject);
  const toId   = await resolveObjectTypeId(toObject);

  const url = `https://api.hubapi.com/crm/v4/associations/${encodeURIComponent(fromId)}/${encodeURIComponent(toId)}/batch/create`;
  const headers = {
    Authorization: `Bearer ${TOKEN}`,
    "Content-Type": "application/json",
  };

  const inputs = pairs.map(([fromRecId, toRecId]) => ({
    from: { id: String(fromRecId) },
    to:   { id: String(toRecId) },
    types: [
      {
        associationCategory: "USER_DEFINED",
        associationTypeId: Number(associationTypeId),
      },
    ],
  }));

  await axios.post(url, { inputs }, { headers });
  return { created: inputs.length };
}
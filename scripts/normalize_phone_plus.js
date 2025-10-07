// scripts/normalize_phone_plus.js
import "dotenv/config";
import axios from "axios";

const TOKEN = process.env.HUBSPOT_TOKEN;
if (!TOKEN) {
  console.error("Falta HUBSPOT_TOKEN en .env (¿estás en la raíz del proyecto?)");
  process.exit(1);
}

const HS = axios.create({
  baseURL: "https://api.hubapi.com",
  headers: { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" }
});

// === Utils ===
const onlyDigits = (s) => String(s || "").replace(/\D/g, "");
const chunk = (arr, size = 100) =>
  arr.reduce((acc, _, i) => (i % size ? acc[acc.length - 1].push(arr[i]) : acc.push([arr[i]]), acc), []);

// Trae TODOS los contactos con las props que necesitamos
async function fetchAll() {
  const results = [];
  let after = undefined;
  do {
    const { data } = await HS.get("/crm/v3/objects/contacts", {
      params: {
        limit: 100,
        properties: "phone,numero_telefono_id_unico,createdate,lastmodifieddate",
        archived: false,
        after
      }
    });
    for (const r of data.results || []) {
      results.push({
        id: r.id,
        phone: r.properties?.phone || "",
        unico: r.properties?.numero_telefono_id_unico || "",
        createdate: r.properties?.createdate,
        lastmodifieddate: r.properties?.lastmodifieddate
      });
    }
    after = data.paging?.next?.after;
  } while (after);
  return results;
}

// Decide el "canónico" de un grupo si no existe dueño previo
function pickCanonical(records, target) {
  const exact = records.find(r => String((r.unico || "").trim()) === target);
  if (exact) return exact.id;
  const sorted = [...records].sort((a, b) => new Date(a.createdate || 0) - new Date(b.createdate || 0));
  return (sorted[0] || records[0]).id;
}

async function main() {
  console.log("Buscando contactos...");
  const all = await fetchAll();
  console.log(`Total leídos: ${all.length}`);

  // 1) Construimos filas con dígitos y target E.164 (+502...) si aplica
  const rows = all.map(c => {
    const digitsUnico  = onlyDigits(c.unico);
    const digitsPhone  = onlyDigits(c.phone);
    const digits       = digitsUnico || digitsPhone;
    const isGT         = digits && digits.startsWith("502");
    const target       = isGT ? `+${digits}` : null;
    return { ...c, digits, target };
  });

  // 2) Mapa de DUEÑO EXISTENTE en TODO el portal:
  //    si hay alguien que ya tiene numero_telefono_id_unico EXACTAMENTE igual al target,
  //    lo consideramos el dueño oficial.
  const existingOwner = new Map(); // target -> contactId
  for (const r of rows) {
    const val = (r.unico || "").trim();
    if (val && val.startsWith("+")) {
      existingOwner.set(val, r.id);
    }
  }

  // 3) Agrupar los que NECESITAN normalización por target
  const byTarget = new Map();
  for (const r of rows) {
    if (!r.target) continue; // no GT o no hay número
    const needsUnico = !String(r.unico || "").trim().startsWith("+");
    const needsPhone = !String(r.phone || "").trim().startsWith("+");
    if (!needsUnico && !needsPhone) continue; // ya ok
    if (!byTarget.has(r.target)) byTarget.set(r.target, []);
    byTarget.get(r.target).push(r);
  }

  const updatesPhoneOnly = []; // actualizar solo phone
  const updatesBoth = [];      // actualizar phone + numero_telefono_id_unico
  const dupReport = [];

  for (const [target, group] of byTarget.entries()) {
    const ownerId = existingOwner.get(target);

    if (ownerId) {
      // Ya hay dueño global con ese valor único → NO tocar unico de nadie, solo phone
      for (const g of group) {
        updatesPhoneOnly.push({ id: g.id, properties: { phone: target } });
      }
      continue;
    }

    // No hay dueño global → resolvemos dentro del grupo
    if (group.length === 1) {
      const g = group[0];
      updatesBoth.push({ id: g.id, properties: { phone: target, numero_telefono_id_unico: target } });
      // Marcar dueño para prevenir colisiones en otros grupos potenciales del mismo target
      existingOwner.set(target, g.id);
      continue;
    }

    // Duplicados dentro del grupo: elegimos canónico
    const canonicalId = pickCanonical(group, target);
    dupReport.push({
      target, total: group.length, canonicalId,
      others: group.filter(x => x.id !== canonicalId).map(x => x.id)
    });

    for (const g of group) {
      if (g.id === canonicalId) {
        updatesBoth.push({ id: g.id, properties: { phone: target, numero_telefono_id_unico: target } });
        existingOwner.set(target, g.id);
      } else {
        updatesPhoneOnly.push({ id: g.id, properties: { phone: target } });
      }
    }
  }

  console.log(`Plan: phone+unico=${updatesBoth.length}, solo phone=${updatesPhoneOnly.length}`);
  if (dupReport.length) {
    console.log("Duplicados dentro de grupo (resumen):");
    for (const d of dupReport.slice(0, 10)) {
      console.log(`  ${d.target}: total=${d.total} canonical=${d.canonicalId} others=${d.others.join(",")}`);
    }
    if (dupReport.length > 10) console.log(`  ... y ${dupReport.length - 10} grupos más`);
  }

  const doBatchUpdate = async (inputs) => {
    for (const part of chunk(inputs, 100)) {
      await HS.post("/crm/v3/objects/contacts/batch/update", { inputs: part });
      console.log(`Actualizados ${part.length}...`);
    }
  };

  if (updatesBoth.length) {
    console.log("Actualizando phone + numero_telefono_id_unico…");
    await doBatchUpdate(updatesBoth);
  }
  if (updatesPhoneOnly.length) {
    console.log("Actualizando solo phone…");
    await doBatchUpdate(updatesPhoneOnly);
  }

  console.log("Listo. Normalización sin romper la unicidad (revisó dueños globales).");
}

main().catch(err => {
  console.error("Error en normalización:", err?.response?.data ?? err.message);
  process.exit(1);
});

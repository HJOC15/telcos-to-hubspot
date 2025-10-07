// src/sinks/hubspotContacts.js
import axios from "axios";
import fs from "fs";
import path from "path";

const HS_BASE = "https://api.hubapi.com";
const headers = (token) => ({
  Authorization: `Bearer ${token}`,
  "Content-Type": "application/json",
});

// ===== util =====
const chunk = (arr, size = 100) =>
  arr.reduce((acc, _, i) => (i % size ? acc[acc.length - 1].push(arr[i]) : acc.push([arr[i]]), acc), []);

function saveDryRun(obj) {
  const dir = path.join(process.cwd(), "data");
  try { fs.mkdirSync(dir, { recursive: true }); } catch {}
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  fs.writeFileSync(path.join(dir, `contacts-batch-${stamp}.json`), JSON.stringify(obj, null, 2));
}

function isNonUniqueError(e) {
  const msg = e?.response?.data?.message || "";
  const cat = e?.response?.data?.category || "";
  return cat === "VALIDATION_ERROR" && /non-unique/i.test(msg);
}

// ===== fallback 1x1: search → create/update =====
async function upsertOneBySearch({ token, idProperty, record }) {
  const searchUrl = `${HS_BASE}/crm/v3/objects/contacts/search`;
  const searchBody = {
    filterGroups: [
      { filters: [{ propertyName: idProperty, operator: "EQ", value: String(record[idProperty] || "") }] }
    ],
    limit: 1
  };

  const found = await axios
    .post(searchUrl, searchBody, { headers: headers(token) })
    .then(r => r.data?.results?.[0])
    .catch(() => null);

  const props = {
    phone: record.phone ?? "",
    firstname: record.firstname ?? "",
    lastname: record.lastname ?? "",
    // envia siempre las custom (si existen en tu portal)
    numero_telefono_id: record.numero_telefono_id ?? "",
    numero_telefono_id_unico: record.numero_telefono_id_unico ?? "",
    // y la idProperty elegida
    [idProperty]: record[idProperty] ?? "",
  };

  if (found?.id) {
    const url = `${HS_BASE}/crm/v3/objects/contacts/${found.id}`;
    await axios.patch(url, { properties: props }, { headers: headers(token) });
    return { id: found.id, action: "updated" };
  } else {
    const url = `${HS_BASE}/crm/v3/objects/contacts`;
    const { data } = await axios.post(url, { properties: props }, { headers: headers(token) });
    return { id: data.id, action: "created" };
  }
}

// ===== público: intenta batch; si non-unique → fallback 1x1 =====
export async function batchUpsertContacts({ token, idProperty, records }) {
  if (!records?.length) return { sent: 0, dryRun: false };

  const send = String(process.env.SEND_TO_HUBSPOT || "true").toLowerCase() === "true";

  // construye inputs una sola vez
  const inputs = records.map((r) => ({
    id: String(r[idProperty]),
    idProperty,
    properties: {
      phone: r.phone ?? "",
      firstname: r.firstname ?? "",
      lastname: r.lastname ?? "",
      [idProperty]: r[idProperty] ?? "",
      numero_telefono_id: r.numero_telefono_id ?? "",
      numero_telefono_id_unico: r.numero_telefono_id_unico ?? "",
    },
  }));

  if (!send || !token) {
    saveDryRun({ idProperty, inputs });
    console.log("[HS][DRY-RUN] guardado payload en ./data (no se llamó a HubSpot)");
    return { sent: inputs.length, dryRun: true };
  }

  const url = `${HS_BASE}/crm/v3/objects/contacts/batch/upsert`;

  try {
    for (const part of chunk(inputs, 100)) {
      await axios.post(url, { inputs: part }, { headers: headers(token) });
    }
    return { sent: inputs.length, dryRun: false, mode: "batch" };
  } catch (e) {
    if (!isNonUniqueError(e)) throw e;
    console.warn("[HS] idProperty no es única → cambiando a upsert 1x1 por búsqueda…");
  }

  // fallback 1x1
  let ok = 0;
  for (const r of records) {
    try {
      await upsertOneBySearch({ token, idProperty, record: r });
      ok++;
    } catch (e) {
      console.error("[HS][fallback-1x1] error con", r[idProperty], e?.response?.data ?? e.message);
    }
  }
  return { sent: ok, dryRun: false, mode: "fallback-1x1" };
}

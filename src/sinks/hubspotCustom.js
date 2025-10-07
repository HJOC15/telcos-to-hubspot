// src/sinks/hubspotCustom.js
import axios from "axios";
import fs from "fs";
import path from "path";

const HS_BASE = "https://api.hubapi.com";
const headers = (token) => ({
  Authorization: `Bearer ${token}`,
  "Content-Type": "application/json",
});

const chunk = (arr, size = 100) =>
  arr.reduce((acc, x, i) => (i % size ? acc[acc.length - 1].push(x) : acc.push([x]), acc), []);

function saveDryRun(objectType, obj) {
  const dir = path.join(process.cwd(), "data");
  try { fs.mkdirSync(dir, { recursive: true }); } catch {}
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  fs.writeFileSync(path.join(dir, `${objectType}-${stamp}.json`), JSON.stringify(obj, null, 2));
}

function isNonUniqueError(e) {
  const msg = e?.response?.data?.message || "";
  const cat = e?.response?.data?.category || "";
  return cat === "VALIDATION_ERROR" && /non-unique|unique/i.test(msg);
}

async function upsertOneBySearch({ token, objectType, idProperty, properties }) {
  const idValue = String(properties[idProperty] || "");
  const searchUrl = `${HS_BASE}/crm/v3/objects/${objectType}/search`;
  const body = { filterGroups: [{ filters: [{ propertyName: idProperty, operator: "EQ", value: idValue }] }], limit: 1 };

  const found = await axios.post(searchUrl, body, { headers: headers(token) })
    .then(r => r.data?.results?.[0]).catch(() => null);

  if (found?.id) {
    const url = `${HS_BASE}/crm/v3/objects/${objectType}/${found.id}`;
    await axios.patch(url, { properties }, { headers: headers(token) });
    return { id: found.id, action: "updated" };
  } else {
    const url = `${HS_BASE}/crm/v3/objects/${objectType}`;
    const { data } = await axios.post(url, { properties }, { headers: headers(token) });
    return { id: data.id, action: "created" };
  }
}

export async function batchUpsertCustomObject({ token, objectType, idProperty, records }) {
  if (!records?.length) return { sent: 0 };

  const send = String(process.env.SEND_TO_HUBSPOT || "true").toLowerCase() === "true";
  const inputs = records.map((props) => ({
    id: String(props[idProperty]),
    idProperty,
    properties: { ...props, [idProperty]: props[idProperty] ?? "" },
  }));

  if (!send || !token) {
    saveDryRun(objectType, { idProperty, inputs });
    console.log(`[HS][DRY-RUN:${objectType}] guardado payload en ./data`);
    return { sent: inputs.length, dryRun: true };
  }

  const url = `${HS_BASE}/crm/v3/objects/${objectType}/batch/upsert`;

  try {
    for (const part of chunk(inputs, 100)) {
      await axios.post(url, { inputs: part }, { headers: headers(token) });
    }
    return { sent: inputs.length, mode: "batch" };
  } catch (e) {
    if (!isNonUniqueError(e)) throw e;
    console.warn(`[HS:${objectType}] idProperty no única → fallback 1x1`);
  }

  let ok = 0;
  for (const p of records) {
    try {
      await upsertOneBySearch({ token, objectType, idProperty, properties: p });
      ok++;
    } catch (e) {
      console.error(`[HS:${objectType}][fallback] error con`, p[idProperty], e?.response?.data ?? e.message);
    }
  }
  return { sent: ok, mode: "fallback-1x1" };
}

// src/sinks/hubspotSearch.js
import axios from "axios";
const TOKEN = process.env.HUBSPOT_TOKEN;

async function searchBatchByProperty({ objectType, propertyName, values }) {
  const url = `https://api.hubapi.com/crm/v3/objects/${encodeURIComponent(objectType)}/search`;
  const headers = { Authorization: `Bearer ${TOKEN}` };

  const body = {
    filterGroups: [
      { filters: [{ propertyName, operator: "IN", values: values.map(String) }] }
    ],
    properties: [propertyName],
    limit: 100
  };

  const { data } = await axios.post(url, body, { headers });
  const results = Array.isArray(data?.results) ? data.results : [];
  const map = new Map();
  for (const r of results) {
    const v = r?.properties?.[propertyName];
    if (v) map.set(String(v), String(r.id));
  }
  return map;
}

export async function searchManyByProperty({ objectType, propertyName, values = [] }) {
  const out = new Map();
  const chunk = 100;
  for (let i = 0; i < values.length; i += chunk) {
    const slice = values.slice(i, i + chunk);
    const m = await searchBatchByProperty({ objectType, propertyName, values: slice });
    for (const [k, v] of m.entries()) out.set(k, v);
  }
  return out;
}

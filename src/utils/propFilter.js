// src/utils/propFilter.js
export function dropBlacklistedProps(record, blacklist = []) {
  if (!record || typeof record !== "object") return record;
  if (!Array.isArray(blacklist) || blacklist.length === 0) return record;
  const out = { ...record };
  for (const k of blacklist) {
    if (k in out) delete out[k];
  }
  return out;
}

export function parseBlacklistFromEnv(envVal) {
  if (!envVal) return [];
  return String(envVal)
    .split(",")
    .map(s => s.trim())
    .filter(Boolean);
}

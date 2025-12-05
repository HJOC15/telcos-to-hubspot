// src/server.js
import "dotenv/config";
import express from "express";
import cron from "node-cron";

// ===== Runners =====

// CLARO
import { runSync as runClaroContactsSync } from "./jobs/sync.js";
import { runMessagesSync as runClaroMessagesSync } from "./jobs/sync_messages.js";

// TIGO
import { runTigoMessagesSync } from "./jobs/sync_tigo_messages.js";
import { runTigoContactsSync } from "./jobs/sync_tigo_contacts.js";

// Asociaciones
import { associateTigoMessagesToContacts } from "./jobs/associate_tigo_messages_contacts.js";
import { fixOrphanMessagesContacts } from "./jobs/fix_orphan_messages_contacts.js";

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const TIMEZONE = process.env.TIMEZONE || process.env.TZ || "America/Guatemala";

/* =========================
   Helpers de ENV (seguros)
   ========================= */
function envBool(name, def = false) {
  const v = String(process.env[name] ?? "").trim().toLowerCase();
  if (!v) return def;
  return ["1", "true", "yes", "y", "on"].includes(v);
}

function getExtractionMode() {
  const tipo = String(process.env.TIPO_EXTRACCION || "recurrente").trim().toLowerCase();
  const start = String(process.env.FECHA_INICIO || "").trim();
  const end = String(process.env.FECHA_FIN || "").trim();

  const isPuntual = tipo === "puntual" && /^\d{4}-\d{2}-\d{2}$/.test(start) && /^\d{4}-\d{2}-\d{2}$/.test(end);
  if (isPuntual) return { tipo: "puntual", start, end };

  return { tipo: "recurrente" };
}

// Setea el contexto por ENV (retrocompatible)
// (Los providers/jobs luego leerán estas variables)
function applyExtractionWindowToEnv(windowCfg) {
  // Limpia primero para no dejar basura
  delete process.env.EXTRACT_START_DATE;
  delete process.env.EXTRACT_END_DATE;

  delete process.env.CLARO_START_DATE;
  delete process.env.CLARO_END_DATE;

  delete process.env.TIGO_START_DATE;
  delete process.env.TIGO_END_DATE;

  if (windowCfg?.tipo === "puntual") {
    process.env.EXTRACT_START_DATE = windowCfg.start;
    process.env.EXTRACT_END_DATE = windowCfg.end;

    // Por si luego quieres separar por telco
    process.env.CLARO_START_DATE = windowCfg.start;
    process.env.CLARO_END_DATE = windowCfg.end;

    process.env.TIGO_START_DATE = windowCfg.start;
    process.env.TIGO_END_DATE = windowCfg.end;
  }
}

/* Util: envoltorio para no tumbar el proceso si un cron falla */
async function safeRun(label, fn) {
  const t0 = Date.now();
  console.log(`[CRON][${label}] inicio ${new Date().toISOString()}`);

  try {
    await fn();
    const ms = Date.now() - t0;
    console.log(`[CRON][${label}] fin OK (${ms} ms)`);
  } catch (e) {
    console.error(`[CRON][${label}] error:`, e?.response?.data ?? e.message ?? e);
  }
}

/* =========================
   Flags / Modo extracción
   ========================= */
const ENABLE_CLARO = envBool("CLARO", true);
const ENABLE_TIGO  = envBool("TIGO", true);

function logBootConfig() {
  const w = getExtractionMode();
  console.log(`[BOOT] CLARO=${ENABLE_CLARO} TIGO=${ENABLE_TIGO} TZ=${TIMEZONE}`);
  console.log(`[BOOT] TIPO_EXTRACCION=${w.tipo}${w.tipo === "puntual" ? ` (${w.start} → ${w.end})` : ""}`);
}
logBootConfig();

/* =========================
   Healthcheck
   ========================= */
app.get("/health", (_req, res) => {
  const w = getExtractionMode();
  res.json({
    ok: true,
    tz: TIMEZONE,
    claro: ENABLE_CLARO,
    tigo: ENABLE_TIGO,
    tipo_extraccion: w.tipo,
    fecha_inicio: w.start || null,
    fecha_fin: w.end || null,
  });
});

/* =========================
   Endpoints manuales
   ========================= */

// Claro (manual): contactos + mensajes
app.get("/cron/claro/run", async (_req, res) => {
  if (!ENABLE_CLARO) return res.status(200).send("Claro deshabilitado (CLARO=false)");

  const w = getExtractionMode();
  applyExtractionWindowToEnv(w);

  await safeRun("Claro/manual", async () => {
    await runClaroContactsSync();
    await runClaroMessagesSync();
    // Si algún día quieres cubrir huérfanos de Claro aquí:
    // await fixOrphanMessagesContacts();
  });

  res.send(`OK (Claro contactos + mensajes) modo=${w.tipo}`);
});

// Tigo (manual): mensajes + contactos + asociaciones + huérfanos
app.get("/cron/tigo/run", async (_req, res) => {
  if (!ENABLE_TIGO) return res.status(200).send("Tigo deshabilitado (TIGO=false)");

  const w = getExtractionMode();
  applyExtractionWindowToEnv(w);

  await safeRun("Tigo/manual", async () => {
    await runTigoMessagesSync();
    await runTigoContactsSync();
    await associateTigoMessagesToContacts();
    await fixOrphanMessagesContacts();
  });

  res.send(`OK (Tigo mensajes + contactos + asociaciones + huérfanos) modo=${w.tipo}`);
});

/* =========================
   Cron jobs programados
   ========================= */

// Claro a las 08:00 GT
cron.schedule(
  "0 8 * * *",
  () => safeRun("Claro", async () => {
    if (!ENABLE_CLARO) return console.log("[CRON][Claro] saltado (CLARO=false)");
    const w = getExtractionMode();
    applyExtractionWindowToEnv(w);

    await runClaroContactsSync();
    await runClaroMessagesSync();
  }),
  { timezone: TIMEZONE }
);
console.log(`[CRON] Programado Claro (contactos + mensajes) a las 08:00 ${TIMEZONE}`);

// Tigo a las 09:00 GT
cron.schedule(
  "0 9 * * *",
  () => safeRun("Tigo", async () => {
    if (!ENABLE_TIGO) return console.log("[CRON][Tigo] saltado (TIGO=false)");
    const w = getExtractionMode();
    applyExtractionWindowToEnv(w);

    await runTigoMessagesSync();
    await runTigoContactsSync();
    await associateTigoMessagesToContacts();
    await fixOrphanMessagesContacts();
  }),
  { timezone: TIMEZONE }
);
console.log(`[CRON] Programado Tigo (mensajes + contactos + asociaciones + huérfanos) a las 09:00 ${TIMEZONE}`);

/* =========================
   Start server
   ========================= */
app.listen(PORT, () => {
  console.log(`HTTP server listening on :${PORT}`);
  console.log(`Healthcheck: http://localhost:${PORT}/health`);
});

/* =========================
   Manejo de errores globales
   ========================= */
process.on("unhandledRejection", (reason) => {
  console.error("[UNHANDLED REJECTION]", reason);
});
process.on("uncaughtException", (err) => {
  console.error("[UNCAUGHT EXCEPTION]", err);
});

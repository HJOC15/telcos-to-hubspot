// src/server.js
import "dotenv/config";
import express from "express";
import cron from "node-cron";
import { associateTigoMessagesToContacts } from "./jobs/associate_tigo_messages_contacts.js";

// === Runners ===
// CLARO (tu job existente de contactos)
import { runSync as runClaroSync } from "./jobs/sync.js";

// TIGO (ya los tienes listos)
import { runTigoMessagesSync } from "./jobs/sync_tigo_messages.js";
import { runTigoContactsSync } from "./jobs/sync_tigo_contacts.js";

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const TIMEZONE = process.env.TIMEZONE || process.env.TZ || "America/Guatemala";

// ===== Healthcheck =====
app.get("/health", (_req, res) => res.json({ ok: true, tz: TIMEZONE }));

// ===== Endpoints manuales (por si quieres disparar desde Postman) =====
app.get("/cron/claro/run", async (_req, res) => {
  try {
    console.log(`[CRON][Claro][manual] inicio ${new Date().toISOString()}`);
    await runClaroSync();
    res.send("OK (Claro)");
  } catch (e) {
    console.error("[CRON][Claro][manual] error:", e?.response?.data ?? e.message ?? e);
    res.status(500).send("Error");
  }
});

app.get("/cron/tigo/run", async (_req, res) => {
  try {
    console.log(`[CRON][Tigo][manual] inicio ${new Date().toISOString()}`);
    await runTigoMessagesSync();
    await runTigoContactsSync();
    await associateTigoMessagesToContacts();
    res.send("OK (Tigo)");
  } catch (e) {
    console.error("[CRON][Tigo][manual] error:", e?.response?.data ?? e.message ?? e);
    res.status(500).send("Error");
  }
});

// ===== Cron jobs programados =====

// Claro a las 08:00 GT
cron.schedule(
  "0 8 * * *",
  async () => {
    console.log(`[CRON][Claro] inicio ${new Date().toISOString()}`);
    try {
      await runClaroSync();
      console.log("[CRON][Claro] fin OK");
    } catch (e) {
      console.error("[CRON][Claro] error:", e?.response?.data ?? e.message ?? e);
    }
  },
  { timezone: TIMEZONE }
);
console.log(`[CRON] Programado Claro a las 08:00 ${TIMEZONE}`);

// Tigo (mensajes + contactos) a las 09:00 GT
cron.schedule(
  "0 9 * * *",
  async () => {
    console.log(`[CRON][Tigo] inicio ${new Date().toISOString()}`);
    try {
      await runTigoMessagesSync();     // mensajes
      await runTigoContactsSync();     // contactos
      console.log("[CRON][Tigo] fin OK");
    } catch (e) {
      console.error("[CRON][Tigo] error:", e?.response?.data ?? e.message ?? e);
    }
  },
  { timezone: TIMEZONE }
);
console.log(`[CRON] Programado Tigo (mensajes+contactos) a las 09:00 ${TIMEZONE}`);

// ===== Start server =====
app.listen(PORT, () => {
  console.log(`HTTP server listening on :${PORT}`);
  console.log(`Healthcheck: http://localhost:${PORT}/health`);
});

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

// (opcional) Asociaciones Tigo si quieres dispararlas manualmente
import { associateTigoMessagesToContacts } from "./jobs/associate_tigo_messages_contacts.js";

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const TIMEZONE = process.env.TIMEZONE || process.env.TZ || "America/Guatemala";

// ===== Healthcheck =====
app.get("/health", (_req, res) => res.json({ ok: true, tz: TIMEZONE }));

// ===== Endpoints manuales =====

// Claro (manual): contactos + mensajes
app.get("/cron/claro/run", async (_req, res) => {
  try {
    console.log(`[CRON][Claro][manual] inicio ${new Date().toISOString()}`);
    await runClaroContactsSync();   // contactos
    await runClaroMessagesSync();   // mensajes
    console.log("[CRON][Claro][manual] fin OK");
    res.send("OK (Claro contactos + mensajes)");
  } catch (e) {
    console.error("[CRON][Claro][manual] error:", e?.response?.data ?? e.message ?? e);
    res.status(500).send("Error");
  }
});

// Tigo (manual): mensajes + contactos +  asociaciones
app.get("/cron/tigo/run", async (_req, res) => {
  try {
    console.log(`[CRON][Tigo][manual] inicio ${new Date().toISOString()}`);
    await runTigoMessagesSync();            // mensajes
    await runTigoContactsSync();            // contactos
    await associateTigoMessagesToContacts();
    console.log("[CRON][Tigo][manual] fin OK");
    res.send("OK (Tigo mensajes + contactos + asociaciones)");
  } catch (e) {
    console.error("[CRON][Tigo][manual] error:", e?.response?.data ?? e.message ?? e);
    res.status(500).send("Error");
  }
});

// ===== Cron jobs programados =====

// Claro a las 08:00 GT (contactos + mensajes)
cron.schedule(
  "0 8 * * *",
  async () => {
    console.log(`[CRON][Claro] inicio ${new Date().toISOString()}`);
    try {
      await runClaroContactsSync();   // contactos
      await runClaroMessagesSync();   // mensajes
      console.log("[CRON][Claro] fin OK");
    } catch (e) {
      console.error("[CRON][Claro] error:", e?.response?.data ?? e.message ?? e);
    }
  },
  { timezone: TIMEZONE }
);
console.log(`[CRON] Programado Claro (contactos + mensajes) a las 08:00 ${TIMEZONE}`);

// Tigo a las 09:00 GT (mensajes + contactos)
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
console.log(`[CRON] Programado Tigo (mensajes + contactos) a las 09:00 ${TIMEZONE}`);

// ===== Start server =====
app.listen(PORT, () => {
  console.log(`HTTP server listening on :${PORT}`);
  console.log(`Healthcheck: http://localhost:${PORT}/health`);
});

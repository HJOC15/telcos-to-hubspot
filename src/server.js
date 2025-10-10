// src/server.js
import "dotenv/config";
import express from "express";
import cron from "node-cron";

// ===== Runners =====

// CLARO
import { runSync as runClaroContactsSync } from "./jobs/sync.js";                 // contactos Claro
import { runMessagesSync as runClaroMessagesSync } from "./jobs/sync_messages.js"; // mensajes Claro

// TIGO
import { runTigoMessagesSync } from "./jobs/sync_tigo_messages.js";
import { runTigoContactsSync } from "./jobs/sync_tigo_contacts.js";

// Asociaciones (Tigo y genéricas msg→contact)
import { associateTigoMessagesToContacts } from "./jobs/associate_tigo_messages_contacts.js";
import { fixOrphanMessagesContacts } from "./jobs/fix_orphan_messages_contacts.js"; // crea contacto faltante y asocia

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const TIMEZONE = process.env.TIMEZONE || process.env.TZ || "America/Guatemala";

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

// ===== Healthcheck =====
app.get("/health", (_req, res) => res.json({ ok: true, tz: TIMEZONE }));

// ===== Endpoints manuales =====

// Claro (manual): contactos + mensajes
app.get("/cron/claro/run", async (_req, res) => {
  await safeRun("Claro/manual", async () => {
    await runClaroContactsSync();   // contactos
    await runClaroMessagesSync();   // mensajes
    // (opcional) también podrías llamar fixOrphanMessagesContacts() aquí si quieres cubrir huérfanos de Claro inmediatamente
  });
  res.send("OK (Claro contactos + mensajes)");
});

// Tigo (manual): mensajes + contactos + asociaciones + huérfanos
app.get("/cron/tigo/run", async (_req, res) => {
  await safeRun("Tigo/manual", async () => {
    await runTigoMessagesSync();            // mensajes
    await runTigoContactsSync();            // contactos
    await associateTigoMessagesToContacts();// asocia por número
    await fixOrphanMessagesContacts();      // crea contactos que falten y asocia
  });
  res.send("OK (Tigo mensajes + contactos + asociaciones + huérfanos)");
});

// ===== Cron jobs programados =====

// Claro a las 08:00 GT (contactos + mensajes)
cron.schedule(
  "0 8 * * *",
  () => safeRun("Claro", async () => {
    await runClaroContactsSync();
    await runClaroMessagesSync();
    // si quieres también cubrir huérfanos de Claro diario, descomenta:
    // await fixOrphanMessagesContacts();
  }),
  { timezone: TIMEZONE }
);
console.log(`[CRON] Programado Claro (contactos + mensajes) a las 08:00 ${TIMEZONE}`);

// Tigo a las 09:00 GT (mensajes + contactos + asociaciones + huérfanos)
cron.schedule(
  "0 9 * * *",
  () => safeRun("Tigo", async () => {
    await runTigoMessagesSync();       // mensajes
    await runTigoContactsSync();       // contactos
    await associateTigoMessagesToContacts(); // asocia por número
    await fixOrphanMessagesContacts(); // crea contactos que falten y asocia
  }),
  { timezone: TIMEZONE }
);
console.log(`[CRON] Programado Tigo (mensajes + contactos + asociaciones + huérfanos) a las 09:00 ${TIMEZONE}`);

// ===== Start server =====
app.listen(PORT, () => {
  console.log(`HTTP server listening on :${PORT}`);
  console.log(`Healthcheck: http://localhost:${PORT}/health`);
});

// ===== Manejo de errores globales (para que no se caiga el proceso) =====
process.on("unhandledRejection", (reason) => {
  console.error("[UNHANDLED REJECTION]", reason);
});
process.on("uncaughtException", (err) => {
  console.error("[UNCAUGHT EXCEPTION]", err);
});

// src/server.js
import "dotenv/config";
import express from "express";
import cron from "node-cron";

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const CRON_ENABLE = String(process.env.CRON_ENABLE || "true").toLowerCase() === "true";
const CRON_EXPRESSION = process.env.CRON_EXPRESSION || "0 8 * * *";
const TIMEZONE = process.env.TIMEZONE || "America/Guatemala";

// ---- Helpers para cargar tus jobs sin pelear con nombres de archivos ----
async function getMessagesRunner() {
  // tu repo ya tiene: src/jobs/sync_messages.js -> runMessagesSync
  const mod = await import("./jobs/sync_messages.js");
  if (typeof mod.runMessagesSync !== "function") {
    throw new Error("No encontré runMessagesSync en ./jobs/sync_messages.js");
  }
  return mod.runMessagesSync;
}

async function getContactsRunner() {
  // algunos repos lo tienen en sync_contacts.js; otros en sync.js
  try {
    const mod = await import("./jobs/sync_contacts.js");
    if (typeof mod.runContactsSync === "function") return mod.runContactsSync;
  } catch {}
  const fallback = await import("./jobs/sync.js");
  if (typeof fallback.runContactsSync === "function") return fallback.runContactsSync;
  if (typeof fallback.runSync === "function") return fallback.runSync; // por si se llama así
  throw new Error("No encontré el runner de contactos (runContactsSync/runSync).");
}

// ---- Pequeño lock para evitar solapes de ejecuciones ----
const running = new Map(); // key: "claro:contacts" | "claro:messages" -> boolean

async function guardedRun(key, fn) {
  if (running.get(key)) {
    return { ok: false, message: `La tarea ${key} ya está corriendo` };
  }
  running.set(key, true);
  const startedAt = new Date().toISOString();
  try {
    await fn();
    return { ok: true, startedAt, finishedAt: new Date().toISOString() };
  } catch (err) {
    return { ok: false, startedAt, error: err?.response?.data ?? err?.message ?? String(err) };
  } finally {
    running.set(key, false);
  }
}

// ---- Rutas HTTP ----
app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    timezone: TIMEZONE,
    cron: { enabled: CRON_ENABLE, expression: CRON_EXPRESSION },
    running: Array.from(running.entries()),
    now: new Date().toISOString(),
  });
});

// Disparo manual: CONTACTOS de Claro -> HubSpot
app.post("/run/claro/contacts", async (_req, res) => {
  try {
    const runner = await getContactsRunner();
    const result = await guardedRun("claro:contacts", runner);
    res.status(result.ok ? 200 : 409).json({ task: "claro:contacts", ...result });
  } catch (e) {
    res.status(500).json({ task: "claro:contacts", ok: false, error: e?.message ?? String(e) });
  }
});

// Disparo manual: MENSAJES de Claro -> HubSpot
app.post("/run/claro/mensajes", async (_req, res) => {
  try {
    const runner = await getMessagesRunner();
    const result = await guardedRun("claro:messages", runner);
    res.status(result.ok ? 200 : 409).json({ task: "claro:messages", ...result });
  } catch (e) {
    res.status(500).json({ task: "claro:messages", ok: false, error: e?.message ?? String(e) });
  }
});

// ---- Cron programado a las 8:00 AM GT ----
if (CRON_ENABLE) {
  try {
    // CONTACTOS
    cron.schedule(
      CRON_EXPRESSION,
      async () => {
        const runner = await getContactsRunner();
        await guardedRun("claro:contacts", runner);
      },
      { timezone: TIMEZONE }
    );
    // MENSAJES
    cron.schedule(
      CRON_EXPRESSION,
      async () => {
        const runner = await getMessagesRunner();
        await guardedRun("claro:messages", runner);
      },
      { timezone: TIMEZONE }
    );
    console.log(`[CRON] Programado "${CRON_EXPRESSION}" TZ=${TIMEZONE} (contacts & mensajes)`);
  } catch (e) {
    console.error("[CRON] Error al programar tareas:", e?.message ?? e);
  }
}

// ---- Start server ----
app.listen(PORT, () => {
  console.log(`HTTP server listening on :${PORT}`);
  console.log(`Healthcheck: http://localhost:${PORT}/health`);
});

// src/cron/index.js
import cron from "node-cron";
import { runTigoMessagesSync } from "../jobs/sync_tigo_messages.js";
import { runTigoContactsSync } from "../jobs/sync_tigo_contacts.js";

const TZ = process.env.TZ || "America/Guatemala";

let isRunningTigo = false;

async function runTigoAll() {
  if (isRunningTigo) {
    console.log("[CRON][Tigo] ya hay una corrida en curso, se salta esta ejecución");
    return;
  }
  isRunningTigo = true;
  console.log(`[CRON][Tigo] inicio ${new Date().toISOString()}`);

  try {
    // 1) Mensajes
    await runTigoMessagesSync();
    // 2) Contactos
    await runTigoContactsSync();
  } catch (e) {
    const msg = e?.response?.data ?? e.message ?? e;
    console.error("[CRON][Tigo] error:", msg);
  } finally {
    isRunningTigo = false;
    console.log(`[CRON][Tigo] fin ${new Date().toISOString()}`);
  }
}

export function scheduleCrons(app) {
  // Corre todos los días a las 09:00 hora local GT
  cron.schedule("0 9 * * *", runTigoAll, { timezone: TZ });
  console.log(`[CRON] Programado Tigo (mensajes+contactos) a las 09:00 ${TZ}`);

  // Endpoint opcional para dispararlo manualmente desde el navegador/Postman
  if (app?.get) {
    app.get("/cron/tigo/run", async (_req, res) => {
      runTigoAll()
        .then(() => res.send("OK"))
        .catch(err => res.status(500).send(String(err)));
    });
  }
}

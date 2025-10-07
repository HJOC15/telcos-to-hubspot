import "dotenv/config";
import cron from "node-cron";
import { runSync } from "./sync.js";

const CRON = process.env.CRON_EXPRESSION || "0 8 * * *";
const TIMEZONE = process.env.TIMEZONE || "America/Guatemala";

console.log(`[CRON] programado: "${CRON}" tz=${TIMEZONE}`);
cron.schedule(CRON, async () => {
  console.log("[CRON] disparo de sincronización", new Date().toISOString());
  await runSync();
}, { timezone: TIMEZONE });

console.log("[CRON] servicio iniciado. Ctrl+C para salir.");

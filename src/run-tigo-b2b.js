// src/run-tigo-b2b.js
import { tigoListMessagesB2B } from "./providers/tigo.js";

(async () => {
  console.log("== Tigo B2B (Comcorp) test ==");
  try {
    // últimos 7 días
    const end = new Date();
    const start = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

    const data = await tigoListMessagesB2B({
      limit: 5,
      startDate: start,
      endDate: end,
      direction: "MT", // quítalo si no lo acepta
    });

    console.log("[TIGO B2B] mensajes:", Array.isArray(data) ? data.length : typeof data);
    if (Array.isArray(data) && data[0]) console.log("[sample]", data[0]);
  } catch (e) {
    console.error("[TIGO B2B] error:", e?.response?.data ?? e?.data ?? e?.message ?? e);
  }
  console.log("== End ==");
})();

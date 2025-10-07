// src/jobs/associate_messages_contacts.js
import "dotenv/config";
import { associateMessagesToContactsByPhone } from "../sinks/hubspotAssociations.js";

/**
 * Este job espera un array de mensajes ya “mapeados” (o una lectura simple)
 * Para mantenerlo simple, aquí te muestro un ejemplo con data mínima.
 * En tu flujo real, puedes reutilizar lo que ya mapeas en sync_messages.
 */

export async function runAssociateMessagesToContacts({ rows } = {}) {
  console.log("== Asociar Mensajes → Contactos ==");
  try {
    // rows debe ser: [{ mensajeIdValue: <id único del mensaje>, numero: <tel> }, ...]
    // Si no te los pasan, aquí podrías consultar a HubSpot los últimos N mensajes y construir “rows”.
    if (!Array.isArray(rows) || rows.length === 0) {
      console.warn("[ASSOC] No se recibieron filas. Pasa rows con {mensajeIdValue, numero}.");
      return;
    }

    const res = await associateMessagesToContactsByPhone(rows);
    console.log(`[ASSOC] created=${res.created} skipped=${res.skipped}`);
  } catch (e) {
    console.error("[ASSOC] error:", e?.response?.data ?? e?.message ?? e);
  }
  console.log("== End Asociaciones ==");
}

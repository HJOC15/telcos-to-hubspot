// src/run-assoc.js
import { runAssociateMessagesToContacts } from "./jobs/associate_messages_contacts.js";

/**
 * Demo: 2 filas de ejemplo.
 * - mensajeIdValue: el valor de tu propiedad única en el objeto de mensajes (p. ej. id_mensaje_unico)
 * - numero: el número del mensaje (puede venir sin +, lo normalizamos)
 */
const demoRows = [
  { mensajeIdValue: "sms-000001", numero: "50259515736" },
  { mensajeIdValue: "sms-000002", numero: "+50242183669" },
];

runAssociateMessagesToContacts({ rows: demoRows });

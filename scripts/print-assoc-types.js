// scripts/print-assoc-types.js
import "dotenv/config";
import axios from "axios";

const TOKEN = process.env.HUBSPOT_TOKEN;
const FROM = process.env.HUBSPOT_MESSAGES_OBJECT || "p_mensajes";
const TO = process.env.HUBSPOT_CONTACTS_OBJECT || "contacts";

if (!TOKEN) {
  console.error("Falta HUBSPOT_TOKEN (no se cargó .env).");
  process.exit(1);
}

(async () => {
  try {
    const url = `https://api.hubapi.com/crm/v3/associations/${encodeURIComponent(FROM)}/${encodeURIComponent(TO)}/types`;
    const { data } = await axios.get(url, {
      headers: { Authorization: `Bearer ${TOKEN}` }
    });
    console.log(JSON.stringify(data, null, 2));
  } catch (e) {
    console.error(e?.response?.data ?? e.message ?? e);
    process.exit(1);
  }
})();

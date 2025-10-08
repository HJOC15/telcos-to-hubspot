import "dotenv/config";
import axios from "axios";

const TOKEN = process.env.HUBSPOT_TOKEN;

async function main() {
  const url = "https://api.hubapi.com/crm/v3/schemas";
  const { data } = await axios.get(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
  const rows = (data?.results || []).map(s => ({
    objectTypeId: s.objectTypeId,
    name: s.name,
    fqName: s.fullyQualifiedName,
    labelSingular: s.labels?.singular,
    labelPlural: s.labels?.plural
  }));
  console.table(rows.slice(0, 50));
  const p = rows.find(r =>
    String(r.name).toLowerCase() === "p_mensajes" ||
    String(r.fqName).toLowerCase() === "p_mensajes" ||
    String(r.labelPlural).toLowerCase() === "p_mensajes" ||
    String(r.labelSingular).toLowerCase() === "p_mensajes"
  );
  if (p) {
    console.log("\n[HINT] Para .env:");
    console.log(`HUBSPOT_P_MENSAJES_OBJECT_TYPEID=${p.objectTypeId}`);
  }
}
main().catch(e => console.error(e?.response?.data ?? e.message ?? e));

// src/run-orphans.js
import "dotenv/config";
import { fixOrphanMessagesContacts } from "./jobs/fix_orphan_messages_contacts.js";

await fixOrphanMessagesContacts();

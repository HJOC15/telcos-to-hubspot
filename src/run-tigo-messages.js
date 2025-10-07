// src/run-tigo-messages.js
import "dotenv/config";
import { runTigoMessagesSync } from "./jobs/sync_tigo_messages.js";

await runTigoMessagesSync();

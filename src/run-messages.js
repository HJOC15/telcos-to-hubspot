// src/run-messages.js
import "dotenv/config";
import { runMessagesSync } from "./jobs/sync_messages.js";

runMessagesSync();

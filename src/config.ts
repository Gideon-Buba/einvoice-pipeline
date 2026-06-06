import "dotenv/config";
import * as sql from "mssql";

export const SOURCE_CONFIG = {
  host: process.env["SOURCE_HOST"]!,
  port: parseInt(process.env["SOURCE_PORT"] ?? "5432"),
  database: process.env["SOURCE_DB"]!,
  user: process.env["SOURCE_USER"]!,
  password: process.env["SOURCE_PASSWORD"]!,
  connectionTimeoutMillis: 30000,
  query_timeout: 1200000,
  ssl: { rejectUnauthorized: false },
};

export const DEST_CONFIG: sql.config = {
  server: process.env["DEST_SERVER"]!,
  port: parseInt(process.env["DEST_PORT"] ?? "1433"),
  database: process.env["DEST_DB"]!,
  domain: process.env["DEST_DOMAIN"]!,
  user: process.env["DEST_USER"]!,
  password: process.env["DEST_PASSWORD"]!,
  options: {
    encrypt: false,
    trustServerCertificate: true,
    useUTC: false,
  },
  driver: "tedious",
  connectionTimeout: 30000,
  requestTimeout: 600000,
};

export const TELEGRAM_BOT_TOKEN = process.env["TELEGRAM_BOT_TOKEN"]!;
export const TELEGRAM_CHAT_ID = process.env["TELEGRAM_CHAT_ID"]!;

export const LOGS_DIR =
  process.env["LOGS_DIR"] ?? "/Users/user/Documents/pipeline-logs";
export const BATCH_SIZE = parseInt(process.env["BATCH_SIZE"] ?? "250000");
export const INSERT_CHUNK = parseInt(process.env["INSERT_CHUNK"] ?? "5000");
export const MAX_RETRIES = 3;
export const RETRY_DELAY_MS = 10_000;
export const TABLE = "dbo.factInvoice";
export const MIN_ID = 5;

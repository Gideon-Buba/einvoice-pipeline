import { appendFileSync, existsSync, mkdirSync } from "fs";
import { join } from "path";
import { LOGS_DIR } from "./config";

export function log(message: string): void {
  const line = `[${new Date().toISOString()}] ${message}`;
  console.log(line);
  if (!existsSync(LOGS_DIR)) mkdirSync(LOGS_DIR, { recursive: true });
  appendFileSync(
    join(LOGS_DIR, `pipeline_${new Date().toISOString().split("T")[0]}.log`),
    line + "\n",
  );
}

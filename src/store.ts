import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { LOGS_DIR } from "./config";
import { Progress, FailedRange } from "./types";

const PROGRESS_FILE = join(LOGS_DIR, "progress.json");
const FAILED_FILE = join(LOGS_DIR, "failed_ranges.json");

export function ensureDirs(): void {
  if (!existsSync(LOGS_DIR)) mkdirSync(LOGS_DIR, { recursive: true });
}

export function loadProgress(): Progress | null {
  if (!existsSync(PROGRESS_FILE)) return null;
  return JSON.parse(readFileSync(PROGRESS_FILE, "utf-8")) as Progress;
}

export function saveProgress(p: Progress): void {
  writeFileSync(PROGRESS_FILE, JSON.stringify(p, null, 2));
}

export function loadFailed(): FailedRange[] {
  if (!existsSync(FAILED_FILE)) return [];
  return JSON.parse(readFileSync(FAILED_FILE, "utf-8")) as FailedRange[];
}

export function appendFailed(range: FailedRange): void {
  const failed = loadFailed();
  const idx = failed.findIndex((f) => f.batchStart === range.batchStart);
  if (idx >= 0) failed[idx] = range;
  else failed.push(range);
  writeFileSync(FAILED_FILE, JSON.stringify(failed, null, 2));
}

export function removeFailed(batchStart: number): void {
  const failed = loadFailed().filter((f) => f.batchStart !== batchStart);
  writeFileSync(FAILED_FILE, JSON.stringify(failed, null, 2));
}

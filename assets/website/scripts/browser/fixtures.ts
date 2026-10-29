import { mkdtemp, mkdir } from "node:fs/promises";
import { execFile } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import type { Page } from "@playwright/test";

/** Use editor keyboard input instead of replacing CodeMirror-owned DOM nodes. */
export async function replaceQuery(page: Page, query: string) {
  const editor = page.locator(".cm-content:visible");
  await editor.click();
  await editor.press("ControlOrMeta+A");
  await page.keyboard.insertText(query);
}

/** Genuine Parquet fixture shared by browser suites; never simulates engine responses. */
export async function addParquetTable(directory: string, table: string, projection: string) {
  await mkdir(`${directory}/${table}`);
  const path = `${directory}/${table}/${table}.parquet`.replaceAll("'", "''");
  await promisify(execFile)("duckdb", ["-c", `COPY (${projection}) TO '${path}' (FORMAT PARQUET)`]);
}

export async function createDataDirectory(
  projection = "SELECT i::BIGINT AS id, (i * 10)::BIGINT AS amount FROM range(1, 10001) t(i)",
  table = "smoke",
) {
  const directory = await mkdtemp(join(tmpdir(), "vein-workspace-"));
  await addParquetTable(directory, table, projection);
  return directory;
}

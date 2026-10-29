import { expect, test } from "@playwright/test";
import { rm } from "node:fs/promises";
import { createDataDirectory, replaceQuery } from "./fixtures";

let directory: string;
test.beforeAll(async () => { directory = await createDataDirectory(); });
test.afterAll(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });

test("released results can rerun their exact statement and failed reruns label older rows", async ({ page }) => {
  await page.goto("/");
  const results = page.getByRole("region", { name: "Query results", exact: true });
  await expect(results).toHaveAttribute("data-state", "not-run");
  await page.locator('input[type="file"]').setInputFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  await replaceQuery(page, "FROM smoke |> WHERE id <= 3 |> SELECT id");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(results).toHaveAttribute("data-state", "completed");
  await expect(page.getByRole("grid")).toHaveAttribute("aria-rowcount", "4");
  await page.getByRole("button", { name: "Commands", exact: true }).click();
  await page.getByRole("button", { name: "Release current result", exact: true }).click();
  await expect(results).toHaveAttribute("data-state", "released");
  await expect(results.locator(".ide-empty > strong")).toHaveText("Result released");
  await expect(page.getByRole("grid")).toHaveCount(0);
  await replaceQuery(page, "FROM smoke |> WHERE id <= 1 |> SELECT id");
  await page.getByRole("button", { name: "Rerun released query", exact: true }).click();
  await expect(results).toHaveAttribute("data-state", "completed");
  await expect(page.getByRole("grid")).toHaveAttribute("aria-rowcount", "4");
  await expect(page.locator(".cm-content:visible")).toContainText("id <= 1");
  await replaceQuery(page, "FROM smoke |> SELECT missing_column");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(results).toHaveAttribute("data-state", "failed");
  await expect(page.getByRole("status")).toHaveText("Failed · previous result retained");
  await expect(page.getByRole("grid")).toHaveAttribute("aria-rowcount", "4");
  await page.getByRole("tab", { name: "Messages", exact: true }).click();
  await expect(page.getByRole("region", { name: "Execution messages", exact: true })).toContainText("Failed");
});

test("zero-row completion stays distinct from failure and release", async ({ page }) => {
  await page.goto("/");
  await page.locator('input[type="file"]').setInputFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  await replaceQuery(page, "FROM smoke |> WHERE id < 0 |> SELECT id");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  const results = page.getByRole("region", { name: "Query results", exact: true });
  await expect(results).toHaveAttribute("data-state", "completed");
  await expect(results).toContainText("No rows matched this query.");
  await expect(results.getByRole("button", { name: "Rerun released query" })).toHaveCount(0);
});

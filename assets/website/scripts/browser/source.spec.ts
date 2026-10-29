import { expect, test } from "@playwright/test";
import { mkdir, rename, rm } from "node:fs/promises";
import { createDataDirectory, replaceQuery } from "./fixtures";

const roots: string[] = [];
test.beforeAll(async () => {
  for (let index = 0; index < 2; index++) {
    const root = await createDataDirectory(); roots.push(root);
    await mkdir(`${root}/warehouse`);
    await rename(`${root}/smoke`, `${root}/warehouse/smoke`);
  }
});
test.afterAll(async () => { for (const root of roots) await rm(root, { recursive: true, force: true }); });

test("results omit the source banner while Details retains source provenance", async ({ page }) => {
  await page.goto("/");
  const input = page.locator('input[type="file"]');
  await input.setInputFiles(`${roots[0]}/warehouse`);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  await replaceQuery(page, "FROM smoke |> AGGREGATE count(*) AS n");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("region", { name: "Query results", exact: true })).toHaveAttribute("data-state", "completed");
  await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
  const source = page.getByRole("note", { name: "Result data source", exact: true });
  await expect(source).toHaveCount(0);
  await input.setInputFiles(`${roots[1]}/warehouse`);
  await expect(page.getByRole("status")).toHaveAttribute("data-phase", "ready");
  await expect(source).toHaveCount(0);
  await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
  await page.getByRole("tab", { name: "Details", exact: true }).click();
  await expect(page.getByRole("region", { name: "Execution details", exact: true })).toContainText("Previous data selection");
  await page.getByRole("tab", { name: "Results", exact: true }).click();
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("region", { name: "Query results", exact: true })).toHaveAttribute("data-state", "completed");
  await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
  await expect(source).toHaveCount(0);
  await page.getByRole("tab", { name: "Details", exact: true }).click();
  await expect(page.getByRole("region", { name: "Execution details", exact: true })).toContainText("Current data selection");
});

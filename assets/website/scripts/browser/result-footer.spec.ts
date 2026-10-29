import { expect, test } from "@playwright/test";
import { rm } from "node:fs/promises";
import { createDataDirectory, replaceQuery } from "./fixtures";

let directory: string;
test.beforeAll(async () => { directory = await createDataDirectory(); });
test.afterAll(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });

for (const width of [1280, 390]) {
  test(`results use one summary footer at ${width}px with working Arrow export`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("/");
    await page.locator('input[type="file"]').setInputFiles(directory);
    await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
    await replaceQuery(page, "FROM smoke |> WHERE id <= 4 |> SELECT id, amount");
    await page.getByRole("button", { name: "Run", exact: true }).click();
    const grid = page.getByRole("grid");
    await expect(grid).toHaveAttribute("aria-rowcount", "5", { timeout: 30_000 });
    const summary = page.getByRole("group", { name: "Result summary", exact: true });
    await expect(summary).toHaveCount(1);
    await expect(page.locator(".vein-ide > footer")).toHaveCount(1);
    await expect(page.locator(".ide-result-footer, .ide-explorer-footer")).toHaveCount(0);
    await expect(summary).toContainText("4 rows");
    await expect(summary).toContainText("Completed");
    await expect(summary.getByText("4 rows", { exact: true })).toHaveCount(1);
    await expect(summary.getByText("Completed", { exact: true })).toHaveCount(1);
    await expect(summary.getByText("Ready", { exact: true })).toHaveCount(0);
    await expect(summary).toContainText("1 file");
    await expect(summary).toContainText(/\d+\.\d ms/);
    await expect(summary).toContainText(/result.*read.*ranges/);
    await expect(page.locator(".ide-result-toolbar, .ide-result-origin, .ide-result-source")).toHaveCount(0);
    const gridBox = await grid.boundingBox();
    const footerBox = await summary.boundingBox();
    expect(footerBox!.y).toBeGreaterThanOrEqual(gridBox!.y + gridBox!.height - 1);
    const sourceBox = await summary.locator(".ide-status-source").boundingBox();
    const detailsBox = await summary.locator(".ide-status-details").boundingBox();
    expect(sourceBox!.x).toBe(footerBox!.x + 12);
    expect(detailsBox!.x + detailsBox!.width).toBeCloseTo(footerBox!.x + footerBox!.width - 12, 0);
    await expect(summary.locator(".ide-status-details")).toHaveCSS("justify-content", "flex-end");
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
    const download = page.waitForEvent("download");
    await summary.getByRole("button", { name: "Arrow", exact: true }).click();
    expect((await download).suggestedFilename()).toMatch(/\.arrow$/);
    await page.getByRole("tab", { name: "Messages", exact: true }).click();
    await expect(summary).toBeVisible();
    await expect(summary.getByRole("button", { name: "Arrow", exact: true })).toBeVisible();
    await page.screenshot({ path: test.info().outputPath(`vein-footer-${width}-${test.info().project.name}.png`) });
  });
}

import { test, expect } from "@playwright/test";
import { rm } from "node:fs/promises";
import { createDataDirectory, replaceQuery } from "./fixtures";

let directory: string;
test.beforeAll(async () => { directory = await createDataDirectory(); });
test.afterAll(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });

test("real WASM supplies table, alias, derived-column and registered-function completions", async ({ page }) => {
  await page.goto("/");
  await page.locator('input[type="file"]').setInputFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  const editor = page.locator(".cm-content:visible");
  const cases = [
    ["FROM sm", "o", /^smoke/],
    ["FROM smoke AS s |> WHERE s.am", "o", /^amount/],
    ["FROM smoke |> EXTEND amount + 1 AS next_amount |> SELECT ne", "x", /^next_amount/],
    ["FROM smoke |> AGGREGATE su", "m", /^sum/i],
  ] as const;
  for (const [query, character, name] of cases) {
    await editor.fill(query);
    await editor.press("ControlOrMeta+End");
    await page.keyboard.type(character);
    const suggestion = page.getByRole("option", { name }).first();
    await expect(suggestion).toBeVisible({ timeout: 15_000 });
    await suggestion.click();
  }
  await expect(editor).toContainText(/sum\(/i);
  await expect(page.getByRole("grid")).toHaveCount(0);
  await expect(page.getByRole("status")).toContainText("Ready");
});

test("real parser and binder diagnostics clear on correction without executing queries", async ({ page }) => {
  await page.goto("/");
  await page.locator('input[type="file"]').setInputFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  const editor = page.locator(".cm-content:visible");
  const problems = page.getByRole("button", { name: /Problems/ });
  const editorHeight = (await page.locator(".ide-editor:visible").boundingBox())!.height;
  await replaceQuery(page, "FROM smoke |> WHERE )");
  await editor.press("ControlOrMeta+End");
  await expect(problems).toContainText("1", { timeout: 15_000 });
  await expect(problems).toHaveAttribute("aria-expanded", "false");
  expect((await page.locator(".ide-editor:visible").boundingBox())!.height).toBe(editorHeight);
  await expect(page.locator(".cm-panel-lint")).toHaveCount(0);
  await expect(page.locator(".cm-lintRange-error")).not.toHaveCount(0);
  await problems.click();
  const diagnostics = page.getByLabel("Query diagnostics");
  await expect(diagnostics).toContainText("Vein syntax");
  const expandedHeight = (await page.locator(".ide-editor:visible").boundingBox())!.height;
  await diagnostics.getByRole("button").first().click();
  await expect(editor).toBeFocused();
  await replaceQuery(page, "FROM smoke |> SELECT missing_column");
  await editor.press("ControlOrMeta+End");
  await expect(page.getByLabel("Query diagnostics")).toContainText("missing_column");
  await expect(page.getByLabel("Query diagnostics")).toContainText("location unavailable");
  await expect(page.locator(".cm-lintRange-error")).toHaveCount(0);
  expect((await page.locator(".ide-editor:visible").boundingBox())!.height).toBe(expandedHeight);
  await replaceQuery(page, "FROM smoke |> SELECT id");
  await editor.press("ControlOrMeta+End");
  await expect(page.locator(".cm-panel-lint")).toHaveCount(0);
  await expect(diagnostics).toContainText("No problems reported");
  expect((await page.locator(".ide-editor:visible").boundingBox())!.height).toBe(expandedHeight);
  await problems.click();
  await expect(diagnostics).toBeHidden();
  expect((await page.locator(".ide-editor:visible").boundingBox())!.height).toBe(editorHeight);
  await expect(page.getByRole("grid")).toHaveCount(0);
});

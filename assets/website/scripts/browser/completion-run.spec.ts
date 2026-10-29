import { expect, test } from "@playwright/test";
import { rm } from "node:fs/promises";
import { createDataDirectory, replaceQuery } from "./fixtures";

let directory: string;
test.beforeAll(async () => { directory = await createDataDirectory(); });
test.afterAll(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });

for (const action of ["button", "shortcut"] as const) {
  test(`running by ${action} dismisses completion without changing the statement`, async ({ page }) => {
    await page.goto("/");
    await page.locator('input[type="file"]').setInputFiles(directory);
    await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
    const query = "FROM smoke |> SELECT amount";
    await replaceQuery(page, query);
    const editor = page.locator(".cm-content:visible");
    await editor.press("ControlOrMeta+End");
    await editor.press("Control+Space");
    await expect(page.getByRole("option", { name: /^amount/ }).first()).toBeVisible();
    if (action === "button") await page.getByRole("button", { name: "Run", exact: true }).click();
    else await editor.press("ControlOrMeta+Enter");
    await expect(page.getByRole("grid")).toBeVisible({ timeout: 30_000 });
    await expect(page.locator(".cm-tooltip-autocomplete")).toHaveCount(0);
    await expect(editor).toHaveText(query);
    await expect(page.getByRole("columnheader", { name: /amount/ })).toBeVisible();
    // Completion remains usable after execution; dismissal does not disable it.
    await editor.click();
    await editor.press("ControlOrMeta+End");
    await editor.press("Control+Space");
    await expect(page.getByRole("option", { name: /^amount/ }).first()).toBeVisible();
  });
}

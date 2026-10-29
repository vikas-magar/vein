import { expect, test } from "@playwright/test";
import { rm } from "node:fs/promises";
import { createDataDirectory } from "./fixtures";

let directory: string;
test.beforeAll(async () => { directory = await createDataDirectory(); });
test.afterAll(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });
test.beforeEach(async ({ page }) => {
  // Fail only CodeMirror initialization. The browser engine and query results remain genuine.
  await page.addInitScript(() => {
    const original = Element.prototype.setAttribute;
    let failed = false;
    Element.prototype.setAttribute = function (name, value) {
      if (!failed && name === "class" && value.split(/\s+/).includes("cm-editor")) {
        failed = true;
        throw new Error("Intentional editor initialization failure");
      }
      return original.call(this, name, value);
    };
  });
  await page.goto("/");
});

test("fallback retains the draft and runs only the statement at the cursor", async ({ page }) => {
  let engineLoads = 0;
  page.on("request", request => { if (request.url().endsWith(".wasm")) engineLoads++; });
  const editor = page.getByRole("textbox", { name: "Vein Pipe query", exact: true });
  await expect(editor).toBeVisible();
  await expect(editor).toHaveCSS("font-variant-ligatures", "none");
  await expect(editor).toContainText("FROM lineitem");
  await editor.fill("FROM smoke |> WHERE id <= 3 |> AGGREGATE sum(amount) AS total;\nFROM smoke |> AGGREGATE count(*) AS n");
  await page.locator('input[type="file"]').setInputFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  await editor.press("ControlOrMeta+End");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
  await editor.press("ControlOrMeta+A");
  await editor.press("ArrowLeft");
  expect(await editor.evaluate(element => (element as HTMLTextAreaElement).selectionStart)).toBe(0);
  await editor.press("ControlOrMeta+Enter");
  await expect(page.getByRole("gridcell", { name: "60", exact: true })).toBeVisible();
  await page.screenshot({ path: test.info().outputPath(`vein-fallback-${test.info().project.name}.png`) });
  await editor.fill("");
  await expect(page.getByRole("button", { name: "Run", exact: true })).toBeDisabled();
  await page.getByRole("button", { name: "New query", exact: true }).click();
  await expect(page.locator(".cm-content:visible")).toBeVisible();
  expect(engineLoads).toBe(1);
});

test("fallback stop controls cancel initialization and remain usable for reconnect", async ({ page }) => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  await page.route("**/*.wasm", async route => { await gate; await route.continue(); });
  try {
    const editor = page.getByRole("textbox", { name: "Vein Pipe query", exact: true });
    await expect(editor).toBeVisible();
    await editor.fill("FROM smoke |> AGGREGATE count(*) AS n");
    await page.locator('input[type="file"]').setInputFiles(directory);
    await expect(page.getByRole("progressbar")).toBeVisible();
    await page.getByRole("button", { name: "Stop execution", exact: true }).click();
    await expect(page.getByRole("progressbar")).toHaveCount(0);
    release();
    await page.getByRole("button", { name: "Run", exact: true }).click();
    await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible({ timeout: 30_000 });
  } finally { release(); }
});

import { test, expect } from "@playwright/test";
import { rm } from "node:fs/promises";
import { createDataDirectory, replaceQuery } from "./fixtures";

let directory: string;
test.beforeAll(async () => { directory = await createDataDirectory(); });
test.afterAll(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });

test("real WASM queries, independent drafts, virtualized results and retained sessions", async ({ page }) => {
  const errors: string[] = [];
  const engineRequests: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  page.on("request", request => { if (request.url().endsWith(".wasm")) engineRequests.push(request.url()); });
  await page.goto("/");
  const editor = page.locator(".cm-content").first();
  await expect(editor).toBeVisible();
  expect(engineRequests).toEqual([]);
  const chooserReady = page.waitForEvent("filechooser");
  await expect(page.locator(".ide-header .ide-source-button")).toHaveCount(0);
  await page.getByRole("button", { name: "Open data directory", exact: true }).first().click();
  await (await chooserReady).setFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  await editor.fill("FROM smoke |> WHERE id <= 3 |> AGGREGATE sum(amount) AS total, count(*) AS n");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("gridcell", { name: "60", exact: true })).toBeVisible({ timeout: 30_000 });
  await expect(page.getByLabel("Result identity")).toHaveCount(0);
  const separator = page.getByRole("separator", { name: "Resize total column", exact: true });
  await separator.focus(); await separator.press("ArrowRight");
  await expect(separator).toHaveAttribute("aria-valuenow", "206");
  await expect(page.getByRole("status")).toHaveAttribute("data-phase", "ready");
  await page.getByRole("button", { name: "New query", exact: true }).click();
  await page.locator(".cm-content:visible").fill("FROM smoke |> SELECT id, amount");
  await page.getByRole("button", { name: "Run", exact: true }).last().click();
  await expect(page.getByRole("status")).toHaveAttribute("data-phase", "ready");
  await expect(page.getByRole("grid")).toHaveAttribute("aria-rowcount", "10001");
  expect(await page.getByRole("gridcell").count()).toBeLessThan(150);
  console.log(JSON.stringify({ browser: test.info().project.name, logicalRows: 10000,
    mountedCells: await page.getByRole("gridcell").count(), engineModulesRequested: engineRequests.length }));
  await page.getByRole("grid").evaluate(element => { element.scrollTop = 150000; });
  await expect(page.getByRole("rowheader", { name: "1", exact: true })).toHaveCount(0);
  await page.screenshot({ path: `/private/tmp/vein-ide-${test.info().project.name}.png` });
  await page.getByRole("tab", { name: /Welcome.vein/ }).click();
  await expect(page.locator(".cm-content:visible")).toContainText("WHERE id <= 3");
  await expect(page.getByRole("gridcell", { name: "60", exact: true })).toBeVisible();
  await page.getByRole("link", { name: /Documentation/ }).click();
  await page.waitForURL("**/docs/");
  await page.getByRole("button", { name: "Edit & run" }).click();
  await page.locator(".cm-content:visible").fill("FROM smoke |> AGGREGATE count(*) AS n");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
  await page.goBack();
  await expect(page.locator(".cm-content:visible")).toBeVisible();
  await expect(page.getByRole("status")).toHaveAttribute("data-phase", "ready");
  expect(errors).toEqual([]);
  await page.locator(".cm-content:visible").fill("FROM smoke |> AGGREGATE count(*) AS n");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
});

test("compact layout keeps the editor usable without horizontal page overflow", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page.locator(".cm-content:visible")).toBeVisible();
  const editor = await page.locator(".ide-query").boundingBox();
  const results = await page.locator(".ide-results").boundingBox();
  expect(editor!.width).toBeGreaterThan(300);
  expect(results!.y).toBeGreaterThan(editor!.y);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  await page.screenshot({ path: `/private/tmp/vein-ide-compact-${test.info().project.name}.png` });
});

test("query errors keep the engine usable and drafts survive refresh", async ({ page }) => {
  await page.goto("/");
  await page.locator('input[type="file"]').setInputFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready");
  await page.locator(".cm-content:visible").fill("FROM missing_table");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.locator(".ide-results").getByRole("alert")).toBeVisible();
  await page.locator(".cm-content:visible").fill("FROM smoke |> AGGREGATE count(*) AS n");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
  await page.waitForTimeout(500);
  await page.reload();
  await expect(page.locator(".cm-content:visible")).toContainText("count(*) AS n");
});

test("closing modified examples requires confirmation and releases the old editor state", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("tab", { name: "Examples", exact: true }).click();
  await page.getByRole("button", { name: /^Q6\b/ }).click();
  await page.locator(".cm-content:visible").fill("FROM modified_draft");
  const close = page.getByRole("tab", { name: /Q6.vein/ }).locator(".flexlayout__tab_button_trailing");
  await close.click();
  await expect(page.getByRole("alertdialog")).toBeVisible();
  await page.getByRole("button", { name: "Keep editing", exact: true }).click();
  await expect(page.locator(".cm-content:visible")).toContainText("modified_draft");
  await close.click();
  await page.getByRole("button", { name: "Close query", exact: true }).click();
  await page.getByRole("button", { name: /^Q6\b/ }).click();
  await expect(page.locator(".cm-content:visible")).not.toContainText("modified_draft");
  await expect(page.locator(".cm-content:visible")).toContainText("FROM lineitem");
});

test("editor preferences apply without restarting the workspace and persist locally", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Settings", exact: true }).click();
  const size = page.getByRole("textbox", { name: "Editor font size", exact: true });
  await size.fill("16"); await size.press("Tab");
  await page.getByRole("button", { name: "Done", exact: true }).click();
  await expect(page.locator(".cm-editor:visible")).toHaveCSS("font-size", "16px");
  await page.reload();
  await expect(page.locator(".cm-editor:visible")).toHaveCSS("font-size", "16px");
});

test("installer command is available and the development indicator is disabled", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Install CLI" }).click();
  const command = page.locator(".ide-install-command");
  await expect(command).toContainText("curl -fsSL http://127.0.0.1:3000/install.sh | sh");
  await expect(page.getByRole("dialog")).toContainText("Requires a published public GitHub release");
  const response = await page.request.get("/install.sh");
  expect(response.status()).toBe(200);
  expect(await response.text()).toContain("Checksum mismatch");
  const indicator = await page.evaluate(() => {
    const portal = document.querySelector("nextjs-portal");
    return !!portal?.shadowRoot?.querySelector('[data-nextjs-dev-tools-button]');
  });
  expect(indicator).toBe(false);
});

test("engine startup shows indeterminate progress and clears it on completion", async ({ page }) => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  await page.route("**/*.wasm", async route => { await gate; await route.continue(); });
  try {
    await page.goto("/");
    await replaceQuery(page, "FROM smoke |> AGGREGATE count(*) AS n");
    const chooserReady = page.waitForEvent("filechooser");
    await page.getByRole("button", { name: "Run", exact: true }).click();
    await (await chooserReady).setFiles(directory);
    const progress = page.getByRole("progressbar");
    await expect(progress).toBeVisible();
    await expect(page.locator(".ide-query-actions").getByRole("progressbar")).toBeVisible();
    await expect(page.getByRole("button", { name: "Run", exact: true })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Stop execution", exact: true })).toBeEnabled();
    expect((await page.getByRole("button", { name: "Run", exact: true }).boundingBox())?.height).toBe(32);
    await expect(progress).not.toHaveAttribute("aria-valuenow");
    await expect(progress.locator(".ide-progress-pulse")).toBeVisible();
    await expect(progress.locator(".ide-progress-label")).not.toBeEmpty();
    await expect(progress.locator(".ide-pulse-flow")).toHaveCSS("animation-name", "ide-heartbeat-flow");
    await page.emulateMedia({ reducedMotion: "reduce" });
    await expect(progress.locator(".ide-pulse-flow")).toHaveCSS("animation-name", "none");
    await page.getByRole("tab", { name: "Messages", exact: true }).click();
    release();
    await expect(page.getByRole("status")).toHaveAttribute("data-phase", "ready", { timeout: 30_000 });
    await expect(progress).toHaveCount(0);
    await expect(page.getByRole("tab", { name: "Results", exact: true })).toHaveAttribute("aria-selected", "true");
    await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
  } finally { release(); }
});

test("controls follow query endings and reveal their result when another output tab is selected", async ({ page }) => {
  await page.goto("/");
  await page.locator('input[type="file"]').setInputFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  const editor = page.locator(".cm-content:visible");
  await editor.fill("FROM smoke |> AGGREGATE count(*) AS n;\nFROM smoke\n|> WHERE id <= 3\n|> AGGREGATE sum(amount) AS total");
  const groups = page.getByRole("group", { name: "Query execution", exact: true });
  await expect(groups).toHaveCount(2);
  const lastLine = await editor.locator(".cm-line").last().boundingBox();
  const controls = await groups.last().boundingBox();
  if (!lastLine || !controls) throw new Error("Query text and controls must be visible");
  expect(controls.y).toBeGreaterThanOrEqual(lastLine.y + lastLine.height - 1);
  expect(controls.y - lastLine.y - lastLine.height).toBeLessThan(10);
  await page.getByRole("tab", { name: "Messages", exact: true }).click();
  await groups.last().getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("gridcell", { name: "60", exact: true })).toBeVisible({ timeout: 30_000 });
  const results = page.getByRole("region", { name: "Query results", exact: true });
  await expect(page.locator(".ide-result-origin")).toHaveCount(0);
  await expect(page.getByText("View executed text", { exact: true })).toHaveCount(0);
  await expect(results).not.toContainText("FROM smoke");
  await expect(results).not.toContainText("Statement 2");
  await groups.first().getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
  await editor.fill("FROM smoke |> LIMIT 1");
  await expect(results).not.toContainText("Query edited since execution");
  await expect(page.getByRole("gridcell", { name: "10000", exact: true })).toBeVisible();
});

import { expect, test } from "@playwright/test";
import { rm } from "node:fs/promises";
import { addParquetTable, createDataDirectory } from "./fixtures";

let first: string, second: string;
test.beforeAll(async () => {
  first = await createDataDirectory();
  second = await createDataDirectory("SELECT 1::BIGINT AS replacement_column");
  await addParquetTable(first, "order", "SELECT 1::BIGINT AS subtotal");
  await addParquetTable(first, "wide", `SELECT ${Array.from({ length: 500 }, (_, i) => `${i}::BIGINT AS c_${i}`).join(", ")}`);
});
test.afterAll(async () => { for (const root of [first, second]) if (root) await rm(root, { recursive: true, force: true }); });

test("schemas load on expansion, retain metadata on reopen and reset for a changed source", async ({ page }) => {
  await page.addInitScript(() => {
    const requests: string[] = [];
    Object.assign(window, { schemaRequests: requests });
    const original = Worker.prototype.postMessage;
    Worker.prototype.postMessage = function (message: unknown, ...options: [options?: Transferable[] | StructuredSerializeOptions]) {
      const request = message as { type?: string; query?: string };
      if (request.type === "inspect" && request.query) requests.push(request.query);
      return Reflect.apply(original, this, [message, ...options]);
    };
  });
  await page.goto("/");
  const input = page.locator('input[type="file"]');
  await input.setInputFiles(first);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  const count = () => page.evaluate(() => (window as unknown as { schemaRequests: string[] }).schemaRequests
    .filter(query => query === 'FROM "smoke"').length);
  expect(await count()).toBe(0);
  const columns = page.getByRole("list", { name: "Columns of smoke", exact: true });
  await expect(columns).toHaveCount(0);
  await page.getByRole("button", { name: "Expand table smoke", exact: true }).click();
  await expect(columns).toContainText("amount");
  await expect(columns).toContainText("Int64");
  expect(await count()).toBe(1);
  await page.getByRole("button", { name: "Collapse table smoke", exact: true }).click();
  await page.getByRole("button", { name: "Expand table smoke", exact: true }).click();
  await expect(columns).toContainText("amount");
  expect(await count()).toBe(1);
  await expect(page.getByRole("grid")).toHaveCount(0);
  await input.setInputFiles(second);
  await expect(page.getByRole("status")).toContainText("Ready");
  await expect(columns).toHaveCount(0);
  await page.getByRole("button", { name: "Expand table smoke", exact: true }).click();
  await expect(columns).toContainText("replacement_column");
  await expect(columns).not.toContainText("amount");
});

test("keyword table names work and wide schema lists remain virtualized", async ({ page }) => {
  await page.goto("/");
  await page.locator('input[type="file"]').setInputFiles(first);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  await page.getByRole("button", { name: "Expand table order", exact: true }).click();
  await expect(page.getByRole("list", { name: "Columns of order", exact: true })).toContainText("subtotal");
  await page.getByRole("button", { name: "Expand table wide", exact: true }).click();
  const columns = page.getByRole("list", { name: "Columns of wide", exact: true });
  await expect(columns).toContainText("c_0");
  expect(await columns.getByRole("listitem").count()).toBeLessThan(25);
  await columns.evaluate(element => { element.scrollTop = 14_000; });
  await expect(columns).toContainText("c_499");
  await page.getByRole("button", { name: "Open query for order", exact: true }).click();
  await expect(page.locator(".cm-content:visible")).toContainText('FROM "order"');
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("gridcell", { name: "1", exact: true })).toBeVisible();
});

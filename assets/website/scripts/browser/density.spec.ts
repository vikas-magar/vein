import { expect, test } from "@playwright/test";

test("compact tab strips preserve panel navigation", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator(".cm-content:visible")).toHaveCount(1);
  const tabs = page.locator(".flexlayout__tab_button");
  expect(await tabs.count()).toBeGreaterThan(3);
  for (const tab of await tabs.all()) {
    const bounds = await tab.boundingBox();
    if (bounds) expect(bounds.height).toBeLessThanOrEqual(28);
  }
  await page.getByRole("tab", { name: "Examples", exact: true }).click();
  await expect(page.getByRole("button", { name: /^Q6\b/ })).toBeVisible();
  await page.getByRole("button", { name: "New query", exact: true }).click();
  await expect(page.locator(".cm-content:visible")).toHaveCount(1);
});

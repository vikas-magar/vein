import { expect, test } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page.locator(".cm-content:visible")).toBeVisible();
});

test("mobile menu supports keyboard dismissal and the shared command dialog", async ({ page }) => {
  const trigger = page.getByRole("button", { name: "Workspace menu", exact: true });
  await trigger.click();
  const menu = page.getByRole("menu", { name: "Workspace menu", exact: true });
  await expect(menu).toBeVisible();
  await expect(menu.getByRole("menuitem")).toHaveCount(3);
  await page.screenshot({ path: `/private/tmp/vein-mobile-menu-${test.info().project.name}.png` });
  await page.keyboard.press("Escape");
  await expect(menu).toHaveCount(0);
  await expect(trigger).toBeFocused();
  await trigger.click();
  await menu.getByRole("menuitem", { name: "Commands", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Workspace commands", exact: true });
  await expect(dialog).toBeVisible();
  await expect(menu).toHaveCount(0);
  await expect(dialog.getByRole("searchbox", { name: "Find a command" })).toBeFocused();
  await dialog.getByRole("button", { name: "Close", exact: true }).click();
  await expect(trigger).toBeFocused();
  await trigger.click();
  await menu.getByRole("menuitem", { name: "Commands", exact: true }).click();
  await dialog.getByRole("button", { name: "New query", exact: true }).click();
  await expect(page.getByRole("tab", { name: /Untitled.vein/ })).toBeVisible();
});

test("mobile settings reuse preferences and documentation uses navigation", async ({ page }) => {
  const trigger = page.getByRole("button", { name: "Workspace menu", exact: true });
  await trigger.click();
  const settings = page.getByRole("menuitem", { name: "Settings", exact: true });
  await settings.focus(); await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog", { name: "Editor settings", exact: true });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("textbox", { name: "Editor font size", exact: true }).fill("16");
  await page.keyboard.press("Tab");
  await dialog.getByRole("button", { name: "Done", exact: true }).click();
  await expect(trigger).toBeFocused();
  await expect(page.locator(".cm-editor:visible")).toHaveCSS("font-size", "16px");
  await trigger.click();
  await page.getByRole("menuitem", { name: "Documentation", exact: true }).click();
  await page.waitForURL("**/docs/");
});

test("widening the viewport closes the mobile menu and exposes desktop controls", async ({ page }) => {
  await page.getByRole("button", { name: "Workspace menu", exact: true }).click();
  await page.setViewportSize({ width: 1280, height: 900 });
  await expect(page.getByRole("menu")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Commands", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Settings", exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(1280);
});

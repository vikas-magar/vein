import { expect, test } from "@playwright/test";
import { replaceQuery } from "./fixtures";

test("pipeline completion inserts stages and preserves literal input", async ({ page }) => {
  await page.goto("/");
  const editor = page.locator(".cm-content:visible");
  await expect(editor).toHaveCSS("font-variant-ligatures", "none");
  await replaceQuery(page, "FROM smoke");
  await editor.press("ControlOrMeta+End");
  await page.keyboard.press("Enter");
  await page.keyboard.type("whe");
  await expect(page.getByRole("option", { name: /WHERE/ })).toBeVisible();
  await page.getByRole("option", { name: /WHERE/ }).click();
  await expect(editor).toHaveText(/FROM smoke\s*\|> WHERE/);
  await replaceQuery(page, "FROM smoke");
  await editor.press("ControlOrMeta+End");
  await page.keyboard.press("Enter");
  await page.keyboard.type("|");
  await expect(editor).toHaveText(/\|>/);
  await replaceQuery(page, "FROM smoke |> WHERE name = 'abc");
  await editor.press("ControlOrMeta+End");
  await page.keyboard.type("|");
  await expect(editor).toHaveText(/'abc\|$/);
  await replaceQuery(page, "FROM smoke -- comment");
  await editor.press("ControlOrMeta+End");
  await page.keyboard.type("|");
  await expect(editor).toHaveText(/-- comment\|$/);
});

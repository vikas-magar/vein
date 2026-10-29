import { expect, test } from "@playwright/test";
import { replaceQuery } from "./fixtures";

test("typing reuses Run control nodes and updates statement boundaries", async ({ page }) => {
  await page.goto("/");
  await replaceQuery(page, "FROM a;\nFROM b;\nFROM c");
  const controls = page.getByRole("group", { name: "Query execution", exact: true });
  await expect(controls).toHaveCount(3);
  const original = await controls.evaluateAll(elements => {
    const registry = new WeakMap();
    elements.forEach((element, index) => registry.set(element, index));
    Object.assign(window, { controlRegistry: registry });
    return elements.length;
  });
  expect(original).toBe(3);
  const editor = page.locator(".cm-content:visible");
  await editor.press("ControlOrMeta+End");
  await page.keyboard.type(" |> LIMIT 1");
  expect(await controls.evaluateAll(elements => elements.map(element =>
    (window as unknown as { controlRegistry: WeakMap<Element, number> }).controlRegistry.get(element)))).toEqual([0, 1, 2]);
  await page.keyboard.type(";\nFROM d");
  await expect(controls).toHaveCount(4);
  await editor.press("ControlOrMeta+z");
  await expect(editor).toBeVisible();
});

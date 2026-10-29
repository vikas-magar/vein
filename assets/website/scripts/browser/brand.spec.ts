import { expect, test } from "@playwright/test";

test("wordmark accents only the i dot and retains its accessible name", async ({ page }) => {
  await page.goto("/");
  const brand = page.locator(".ide-brand");
  await expect(brand.getByRole("img", { name: "vein", exact: true })).toBeVisible();
  await expect(brand).not.toContainText(".");
  const dot = await brand.locator(".vein-wordmark-i").evaluate(element => {
    const style = getComputedStyle(element, "::before");
    return { color: style.backgroundColor, width: parseFloat(style.width) };
  });
  expect(dot.color).toBe("rgb(255, 73, 105)");
  expect(dot.width).toBeGreaterThan(0);
  await brand.screenshot({ path: test.info().outputPath(`vein-wordmark-${test.info().project.name}.png`) });
});

import { defineConfig, devices } from "@playwright/test";
import { fileURLToPath } from "node:url";

export default defineConfig({
  testDir: "./scripts/browser", timeout: 60_000, fullyParallel: false,
  reporter: "list", outputDir: "/private/tmp/vein-browser-test-results",
  use: { baseURL: process.env.VEIN_WEBSITE_URL ?? "http://127.0.0.1:3000", trace: "retain-on-failure" },
  webServer: process.env.VEIN_WEBSITE_URL ? undefined : {
    command: "npm run dev -- --hostname 127.0.0.1 --port 3000",
    cwd: fileURLToPath(new URL(".", import.meta.url)),
    url: "http://127.0.0.1:3000", reuseExistingServer: !process.env.CI, timeout: 60_000,
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } } },
    { name: "webkit", use: { ...devices["Desktop Safari"], viewport: { width: 1440, height: 900 } } },
  ],
});

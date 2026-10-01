import { defineConfig, devices } from "@playwright/test";

// Runs against an already-started stack (docker compose up) with fault injection enabled:
//   RELAYFLOW_ENABLE_FAULT_INJECTION=1 docker compose up -d --build
export default defineConfig({
  testDir: "./e2e",
  timeout: 90_000,
  expect: { timeout: 30_000 },
  retries: 0,
  workers: 1,
  reporter: [["list"], ["html", { open: "never" }]],
  use: {
    baseURL: process.env.RELAYFLOW_DASHBOARD_URL ?? "http://127.0.0.1:8080",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});

// Test helper for tests/stack/test_api_restart.py (not a Playwright spec).
//
//   node e2e/connection-watch.mjs <dashboard-url> <run-id>
//
// Opens ONE browser page on the run and never reloads it. Prints one line per milestone
// so the Python test can coordinate deterministically:
//   READY        page shows "Connected" and the run
//   DOWN         header indicator switched to "API unreachable" (API container stopped)
//   UP <status>  indicator back to "Connected" and the run status shown
//   SAME_PAGE    the page was never reloaded (a marker set at READY is still present)
// Any failure prints "ERROR <message>" and exits 1. Every wait is bounded.
import { chromium } from "@playwright/test";

const [base, runId] = process.argv.slice(2);
const say = (line) => process.stdout.write(`${line}\n`);
const browser = await chromium.launch();
try {
  const page = await browser.newPage();
  await page.goto(`${base}/runs/${runId}`);
  const indicator = page.getByTestId("connection");
  await indicator.filter({ hasText: "Connected" }).waitFor({ timeout: 30_000 });
  await page.getByTestId("run-title").waitFor({ timeout: 30_000 });
  await page.evaluate(() => {
    window.__relayflowSamePage = true;
  });
  say("READY");
  await indicator.filter({ hasText: "API unreachable" }).waitFor({ timeout: 60_000 });
  say("DOWN");
  await indicator.filter({ hasText: "Connected" }).waitFor({ timeout: 120_000 });
  const badge = page.locator(".controls").getByTestId("status-badge");
  await badge.filter({ hasText: "succeeded" }).waitFor({ timeout: 60_000 });
  say(`UP ${(await badge.textContent())?.trim()}`);
  const same = await page.evaluate(() => window.__relayflowSamePage === true);
  say(same ? "SAME_PAGE" : "RELOADED");
} catch (error) {
  say(`ERROR ${String(error).split("\n")[0]}`);
  process.exitCode = 1;
} finally {
  await browser.close();
}

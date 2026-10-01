// Captures README screenshots from a running stack: node e2e/screenshots.mjs <run-id>
import { chromium } from "@playwright/test";

const base = process.env.RELAYFLOW_DASHBOARD_URL ?? "http://127.0.0.1:8080";
const runId = process.argv[2];
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1280, height: 900 }, colorScheme: "light" });
await page.goto(`${base}/`);
await page.waitForSelector('[data-testid="workers-table"]');
await page.screenshot({ path: "../docs/images/overview.png", fullPage: true });
if (runId) {
  await page.goto(`${base}/runs/${runId}`);
  await page.waitForSelector('[data-testid="recovery-panel"]');
  await page.getByTestId("dag-node-keywords").click();
  await page.waitForTimeout(500);
  await page.screenshot({ path: "../docs/images/run-recovered.png", fullPage: true });
}
await page.goto(`${base}/runs`);
await page.waitForSelector('[data-testid="runs-table"]');
await page.screenshot({ path: "../docs/images/runs.png" });
await page.goto(`${base}/submit`);
await page.screenshot({ path: "../docs/images/submit.png", fullPage: true });
await browser.close();

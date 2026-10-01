import { expect, test, type Page } from "@playwright/test";

// Browser end-to-end tests for the main dashboard flows. They drive the real stack
// (API + workers + scheduler + PostgreSQL + mock notification service), so every
// assertion below reflects persisted backend state, not mocked responses.

async function submitDocument(page: Page, options: { title: string; delayTask?: string; delay?: number; keywordFailures?: string }) {
  await page.goto("/submit");
  await page.locator('input[name="title"]').fill(options.title);
  if (options.delayTask) {
    await page.locator('select[name="delay-task"]').selectOption(options.delayTask);
    await page.locator('input[name="delay"]').fill(String(options.delay ?? 10));
  }
  if (options.keywordFailures) {
    await page.locator('select[name="keyword-failures"]').selectOption(options.keywordFailures);
  }
  await page.getByTestId("submit-button").click();
  await expect(page).toHaveURL(/\/runs\/[0-9a-f-]{36}$/);
  await expect(page.getByTestId("run-title")).toHaveText(options.title);
}

function runStatus(page: Page) {
  return page.locator(".controls").getByTestId("status-badge");
}

test("overview shows healthy workers", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("connection")).toContainText("Connected");
  const workers = page.getByTestId("workers-table");
  await expect(workers).toBeVisible();
  await expect(workers.getByText("healthy").first()).toBeVisible();
});

test("submit a document and watch it complete", async ({ page }) => {
  const title = `E2E success ${Date.now()}`;
  await submitDocument(page, { title });
  await expect(runStatus(page)).toHaveText("succeeded");
  for (const key of ["validate", "word_count", "keywords", "report", "notify"]) {
    await expect(page.getByTestId(`dag-node-${key}`)).toHaveAttribute("data-status", "succeeded");
  }
  await page.getByTestId("dag-node-notify").click();
  const panel = page.getByTestId("task-panel");
  await expect(panel).toContainText("notification_id");
  await expect(panel).toContainText(`relayflow:`);
  await expect(page.getByTestId("events")).toContainText("run succeeded");

  await page.goto("/runs?status=succeeded");
  await expect(page.getByTestId("runs-table")).toContainText(title);
});

test("progress is visible while a task runs, and cancel stops the run", async ({ page }) => {
  await submitDocument(page, { title: `E2E cancel ${Date.now()}`, delayTask: "keywords", delay: 25 });
  await expect(page.getByTestId("dag-node-keywords")).toHaveAttribute("data-status", "running");
  await expect(page.getByTestId("retry-button")).toBeDisabled();
  page.once("dialog", (dialog) => void dialog.accept());
  await page.getByTestId("cancel-button").click();
  await expect(runStatus(page)).toHaveText("cancelled", { timeout: 15_000 });
  await expect(page.getByTestId("dag-node-report")).toHaveAttribute("data-status", "cancelled");
  await expect(page.getByTestId("dag-node-validate")).toHaveAttribute("data-status", "succeeded");
  await expect(page.getByTestId("cancel-button")).toBeDisabled();
});

test("terminal failure, then manual retry completes and keeps earlier outputs", async ({ page }) => {
  await submitDocument(page, { title: `E2E retry ${Date.now()}`, keywordFailures: "exhaust" });
  await expect(runStatus(page)).toHaveText("failed");
  await expect(page.getByTestId("run-error")).toContainText("keywords");
  await expect(page.getByTestId("dag-node-keywords")).toHaveAttribute("data-status", "failed");
  await expect(page.getByTestId("dag-node-report")).toHaveAttribute("data-status", "blocked");
  await expect(page.getByTestId("dag-node-word_count")).toHaveAttribute("data-status", "succeeded");
  await page.getByTestId("dag-node-keywords").click();
  await expect(page.getByTestId("attempts-table").locator("tbody tr")).toHaveCount(3);

  page.once("dialog", (dialog) => void dialog.accept());
  await page.getByTestId("retry-button").click();
  await expect(runStatus(page)).toHaveText("succeeded");
  await expect(page.getByTestId("attempts-table").locator("tbody tr")).toHaveCount(4);
  await page.getByTestId("dag-node-word_count").click();
  await expect(page.getByTestId("attempts-table").locator("tbody tr")).toHaveCount(1); // not re-run
  await expect(page.locator(".page-head")).toContainText("manual retries: 1");
});

test("status filter only lists matching runs", async ({ page }) => {
  await page.goto("/runs");
  await page.getByTestId("filter-cancelled").click();
  await expect(page).toHaveURL(/status=cancelled/);
  const badges = page.getByTestId("runs-table").getByTestId("status-badge");
  await expect(badges.first()).toBeVisible();
  for (const text of await badges.allTextContents()) expect(text).toBe("cancelled");
});

// Typed client for the RelayFlow REST API (docs/architecture.md section 10).
// Everything the dashboard shows comes from these endpoints, i.e. persisted PostgreSQL state.

export type RunStatus = "running" | "cancelling" | "succeeded" | "failed" | "cancelled";
export type TaskStatus =
  | "pending"
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "blocked"
  | "cancelled";
export type AttemptStatus =
  | "running"
  | "succeeded"
  | "failed"
  | "timed_out"
  | "lease_expired"
  | "cancelled"
  | "released";

export const RUN_STATUSES: RunStatus[] = ["running", "cancelling", "succeeded", "failed", "cancelled"];

export interface Attempt {
  id: string;
  attempt_number: number;
  worker_id: string;
  status: AttemptStatus;
  started_at: string;
  heartbeat_at: string;
  lease_expires_at: string;
  finished_at: string | null;
  error: string | null;
  lease_lapsed: boolean;
}

export interface Task {
  id: string;
  task_key: string;
  task_type: string;
  depends_on: string[];
  status: TaskStatus;
  input: unknown;
  output: unknown;
  max_attempts: number;
  timeout_seconds: number;
  attempt_count: number;
  failure_count: number;
  available_at: string;
  queued_at: string | null;
  finished_at: string | null;
  last_error: string | null;
  attempts: Attempt[];
}

export interface RunSummary {
  id: string;
  workflow_name: string;
  workflow_version: number;
  status: RunStatus;
  title: string | null;
  idempotency_key: string | null;
  manual_retry_count: number;
  cancel_requested_at: string | null;
  error: string | null;
  created_at: string;
  updated_at: string;
  finished_at: string | null;
  task_total: number;
  task_succeeded: number;
  attempt_total: number;
}

export interface RunDetail extends Omit<RunSummary, "task_total" | "task_succeeded" | "attempt_total"> {
  input: Record<string, unknown>;
  definition_snapshot: { name: string; version: number; description: string };
  db_now: string;
  tasks: Task[];
}

export interface RunEvent {
  id: number;
  task_key: string | null;
  attempt_id: string | null;
  worker_id: string | null;
  kind: string;
  message: string;
  data: Record<string, unknown>;
  created_at: string;
}

export interface Worker {
  id: string;
  name: string;
  hostname: string;
  pid: number;
  concurrency: number;
  status: "active" | "stopping" | "stopped";
  in_flight: number;
  started_at: string;
  last_heartbeat_at: string;
  heartbeat_age_seconds: number;
  healthy: boolean;
  attempts_total: number;
  attempts_running: number;
}

export interface Overview {
  runs: Partial<Record<RunStatus, number>>;
  tasks: Partial<Record<TaskStatus, number>>;
  lease_expirations: number;
  workers: Worker[];
  db_now: string;
  settings: {
    lease_seconds: number;
    heartbeat_seconds: number;
    poll_interval_seconds: number;
    fault_injection: boolean;
  };
}

export interface SubmitBody {
  workflow_name: string;
  input: Record<string, unknown>;
  idempotency_key?: string;
}

export interface SubmitResult {
  run_id: string;
  created: boolean;
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly errors: string[] = [],
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api${path}`, {
      ...init,
      headers: { "Content-Type": "application/json", ...init?.headers },
    });
  } catch {
    throw new ApiError(0, "Cannot reach the RelayFlow API");
  }
  const body: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = (body as { detail?: { message?: string; errors?: string[] } | string } | null)?.detail;
    const message =
      typeof detail === "string" ? detail : (detail?.message ?? `Request failed (${response.status})`);
    throw new ApiError(response.status, message, typeof detail === "object" ? (detail?.errors ?? []) : []);
  }
  return body as T;
}

export const api = {
  overview: () => request<Overview>("/overview"),
  runs: (status: RunStatus | null, limit = 50, offset = 0) => {
    const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    if (status) params.set("status", status);
    return request<{ items: RunSummary[]; total: number }>(`/runs?${params.toString()}`);
  },
  run: (id: string) => request<RunDetail>(`/runs/${id}`),
  events: (id: string) => request<RunEvent[]>(`/runs/${id}/events`),
  submit: (body: SubmitBody) =>
    request<SubmitResult>("/runs", { method: "POST", body: JSON.stringify(body) }),
  cancel: (id: string) => request<{ status: RunStatus }>(`/runs/${id}/cancel`, { method: "POST" }),
  retry: (id: string) => request<{ status: RunStatus }>(`/runs/${id}/retry`, { method: "POST" }),
};

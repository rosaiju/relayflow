import { useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";

import { api, ApiError, type RunDetail, type Task } from "../api";
import { Dag } from "../components/Dag";
import { ErrorBox, JsonView, Loading, StaleBanner, StatusBadge } from "../components/common";
import { ago, clock, dateTime, duration, seconds, shortId, workerLabel } from "../format";
import { usePolling } from "../usePolling";

interface Recovery {
  task: string;
  expired: Task["attempts"][number];
  next: Task["attempts"][number] | undefined;
}

function recoveries(run: RunDetail): Recovery[] {
  return run.tasks.flatMap((task) =>
    task.attempts
      .filter((a) => a.status === "lease_expired")
      .map((expired) => ({
        task: task.task_key,
        expired,
        next: task.attempts.find((a) => a.attempt_number === expired.attempt_number + 1),
      })),
  );
}

function RecoveryPanel({ run }: { run: RunDetail }) {
  const items = recoveries(run);
  if (items.length === 0) return null;
  return (
    <section className="card recovery" data-testid="recovery-panel">
      <h2>Recovered after worker failure</h2>
      <ul>
        {items.map(({ task, expired, next }) => (
          <li key={expired.id}>
            <strong>{task}</strong>: attempt {expired.attempt_number} on{" "}
            <span className="mono">{workerLabel(expired.worker_id)}</span> last heartbeat {clock(expired.heartbeat_at)};
            lease expired at {clock(expired.lease_expires_at)} and the scheduler recorded it at{" "}
            {clock(expired.finished_at)}.{" "}
            {next ? (
              <>
                Attempt {next.attempt_number} was claimed by <span className="mono">{workerLabel(next.worker_id)}</span>{" "}
                at {clock(next.started_at)} ({seconds(ago(expired.heartbeat_at, next.started_at))} after the last
                heartbeat) and is <StatusBadge status={next.status} />.
              </>
            ) : (
              <>Waiting for another worker to claim it.</>
            )}
          </li>
        ))}
      </ul>
      <p className="hint">
        Times are PostgreSQL timestamps. Completed tasks were not re-run; only the task whose lease lapsed got a new
        attempt. Execution is at-least-once: the crashed attempt may already have produced external effects.
      </p>
    </section>
  );
}

function nextAttemptNote(task: Task, dbNow: string): string | null {
  if (task.status !== "queued" || task.failure_count === 0) return null;
  const wait = -ago(task.available_at, dbNow);
  return wait > 0
    ? `Retry ${task.failure_count + 1}/${task.max_attempts} becomes claimable in ${seconds(wait)} (exponential backoff with jitter)`
    : `Retry ${task.failure_count + 1}/${task.max_attempts} is claimable now`;
}

function TaskPanel({ task, run }: { task: Task; run: RunDetail }) {
  const note = nextAttemptNote(task, run.db_now);
  return (
    <section className="card task-panel" data-testid="task-panel">
      <div className="card-head">
        <h2>
          Task <span className="mono">{task.task_key}</span>
        </h2>
        <StatusBadge status={task.status} />
      </div>
      <dl className="facts">
        <dt>Type</dt>
        <dd className="mono">{task.task_type}</dd>
        <dt>Depends on</dt>
        <dd>{task.depends_on.length ? task.depends_on.join(", ") : "nothing (root task)"}</dd>
        <dt>Failures / budget</dt>
        <dd>
          {task.failure_count} / {task.max_attempts}{" "}
          <span className="muted small">(failed, timed-out and lease-expired attempts count)</span>
        </dd>
        <dt>Timeout</dt>
        <dd>{task.timeout_seconds}s</dd>
        <dt>Queued at</dt>
        <dd>{dateTime(task.queued_at)}</dd>
        <dt>Finished at</dt>
        <dd>{dateTime(task.finished_at)}</dd>
      </dl>
      {note && (
        <div className="callout callout-info" data-testid="retry-timing">
          {note}
        </div>
      )}
      {task.status === "blocked" && (
        <div className="callout callout-warn">
          Blocked: an upstream task failed terminally, so this task was never dispatched. A manual retry of the run will
          unblock it.
        </div>
      )}
      {task.last_error && task.status !== "succeeded" && (
        <div className="callout callout-error">
          <strong>Last error:</strong> {task.last_error}
        </div>
      )}
      <h3>Attempts</h3>
      {task.attempts.length === 0 ? (
        <p className="muted">No attempts yet.</p>
      ) : (
        <div className="table-scroll">
          <table className="table attempts" data-testid="attempts-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Worker</th>
                <th>Status</th>
                <th>Started</th>
                <th>Last heartbeat</th>
                <th>Lease expires</th>
                <th>Duration</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {task.attempts.map((a) => (
                <tr key={a.id} data-status={a.status}>
                  <td>{a.attempt_number}</td>
                  <td className="mono small">{workerLabel(a.worker_id)}</td>
                  <td>
                    <StatusBadge status={a.status} />
                    {a.lease_lapsed && <span className="muted small"> (lapsed, awaiting scheduler)</span>}
                  </td>
                  <td>{clock(a.started_at)}</td>
                  <td>{clock(a.heartbeat_at)}</td>
                  <td>{a.status === "running" ? clock(a.lease_expires_at) : "—"}</td>
                  <td>{duration(a.started_at, a.finished_at, run.db_now)}</td>
                  <td className="small">{a.error ?? ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="io">
        <JsonView label="Input (persisted when the task became ready)" value={task.input} />
        <JsonView label="Output" value={task.output} />
      </div>
    </section>
  );
}

function Events({ runId }: { runId: string }) {
  const events = usePolling(() => api.events(runId), 2000, [runId]);
  if (!events.data) return events.loading ? <Loading label="Loading events" /> : null;
  return (
    <section className="card">
      <h2>Event log</h2>
      <ol className="events" data-testid="events">
        {[...events.data].reverse().map((e) => (
          <li key={e.id} className={`event event-${e.kind}`}>
            <span className="mono small muted">{clock(e.created_at)}</span>
            <span className="event-kind">{e.kind.replaceAll("_", " ")}</span>
            {e.task_key && <span className="mono">{e.task_key}</span>}
            <span>{e.message}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}

export function RunDetailPage() {
  const { runId = "" } = useParams();
  const location = useLocation();
  const reused = (location.state as { created?: boolean } | null)?.created === false;
  const run = usePolling(() => api.run(runId), 1500, [runId]);
  const [selected, setSelected] = useState<string | null>(null);
  const [action, setAction] = useState<{ busy: boolean; error: ApiError | null }>({ busy: false, error: null });

  if (run.loading) return <Loading label="Loading run" />;
  if (!run.data) {
    return run.error ? (
      <div className="page">
        <ErrorBox error={run.error} onRetry={run.refresh} />
        <Link to="/runs">← Back to runs</Link>
      </div>
    ) : null;
  }
  const r = run.data;
  const focus =
    r.tasks.find((t) => t.task_key === selected) ??
    r.tasks.find((t) => t.status === "running") ??
    r.tasks.find((t) => t.status === "failed") ??
    r.tasks[0];

  const control = async (kind: "cancel" | "retry") => {
    const question =
      kind === "cancel"
        ? "Cancel this run? Queued tasks stop now; running tasks stop at their next check. Effects that already happened are not undone."
        : "Retry the failed tasks? Succeeded tasks keep their outputs and are not re-run.";
    if (!window.confirm(question)) return;
    setAction({ busy: true, error: null });
    try {
      await (kind === "cancel" ? api.cancel(r.id) : api.retry(r.id));
      setAction({ busy: false, error: null });
      run.refresh();
    } catch (err) {
      setAction({ busy: false, error: err instanceof ApiError ? err : new ApiError(0, String(err)) });
    }
  };

  return (
    <div className="page">
      <StaleBanner error={run.stale ? run.error : null} />
      <Link to="/runs" className="back">
        ← Runs
      </Link>
      <div className="page-head">
        <div>
          <h1 data-testid="run-title">{String(r.input.title ?? r.workflow_name)}</h1>
          <div className="muted">
            {r.workflow_name} v{r.workflow_version} · run <span className="mono">{shortId(r.id)}</span> · created{" "}
            {dateTime(r.created_at)}
            {r.finished_at && <> · finished in {duration(r.created_at, r.finished_at, r.db_now)}</>}
            {r.manual_retry_count > 0 && <> · manual retries: {r.manual_retry_count}</>}
          </div>
        </div>
        <div className="controls">
          <StatusBadge status={r.status} />
          <button
            className="btn btn-danger"
            disabled={r.status !== "running" || action.busy}
            onClick={() => void control("cancel")}
            title={r.status === "running" ? "Request cooperative cancellation" : "Only running runs can be cancelled"}
            data-testid="cancel-button"
          >
            Cancel
          </button>
          <button
            className="btn btn-primary"
            disabled={r.status !== "failed" || action.busy}
            onClick={() => void control("retry")}
            title={r.status === "failed" ? "Retry failed and blocked tasks" : "Only failed runs can be retried"}
            data-testid="retry-button"
          >
            Retry failed tasks
          </button>
        </div>
      </div>
      {reused && (
        <div className="callout callout-info">
          This idempotency key was already used with the same document, so the existing run was returned instead of
          creating a new one.
        </div>
      )}
      {action.error && <ErrorBox error={action.error} />}
      {r.status === "failed" && r.error && (
        <div className="callout callout-error" data-testid="run-error">
          <strong>Run failed:</strong> {r.error}. Tasks downstream of the failure were blocked; independent tasks ran to
          completion.
        </div>
      )}
      {r.status === "cancelling" && (
        <div className="callout callout-warn">
          Cancellation requested. Waiting for running tasks to stop at their next cooperative check; a task that
          finishes first keeps its result.
        </div>
      )}

      <section className="card">
        <h2>Dependency graph</h2>
        <Dag tasks={r.tasks} selected={focus?.task_key ?? null} onSelect={setSelected} />
        <p className="hint">Select a task to see its input, output and attempts. Edges turn solid when the upstream task succeeds.</p>
      </section>
      <RecoveryPanel run={r} />
      {focus && <TaskPanel task={focus} run={r} />}
      <Events runId={r.id} />
    </div>
  );
}

import { Link } from "react-router-dom";

import { api, RUN_STATUSES, type Worker } from "../api";
import { Empty, ErrorBox, Loading, StaleBanner, Stat, StatusBadge } from "../components/common";
import { ago, dateTime, seconds, shortId } from "../format";
import { usePolling } from "../usePolling";

export function OverviewPage() {
  const overview = usePolling(api.overview, 2000, []);
  const recent = usePolling(() => api.runs(null, 6), 2000, []);

  if (overview.loading) return <Loading label="Loading overview" />;
  if (!overview.data) return overview.error ? <ErrorBox error={overview.error} onRetry={overview.refresh} /> : null;
  const o = overview.data;
  const healthy = o.workers.filter((w) => w.healthy).length;
  // Current view: healthy workers plus anything that went silent in the last 2 minutes (e.g. a
  // worker you just killed). Older stopped/stale instances are history, collapsed below.
  const current = o.workers.filter((w) => w.healthy || (w.status !== "stopped" && w.heartbeat_age_seconds < 120));
  const history = o.workers.filter((w) => !current.includes(w));

  return (
    <div className="page">
      <StaleBanner error={overview.stale ? overview.error : null} />
      <h1>Overview</h1>
      <section className="stats" aria-label="Run counts">
        {RUN_STATUSES.map((s) => (
          <Link key={s} to={`/runs?status=${s}`} className="stat-link">
            <Stat label={`${s} runs`} value={o.runs[s] ?? 0} tone={s} />
          </Link>
        ))}
        <Stat label="lease expirations recovered" value={o.lease_expirations} tone="recovered" />
      </section>

      <section className="card">
        <div className="card-head">
          <h2>Workers</h2>
          <span className="muted">
            {healthy} healthy · lease {o.settings.lease_seconds}s · heartbeat{" "}
            {o.settings.heartbeat_seconds}s
          </span>
        </div>
        {o.workers.length === 0 ? (
          <Empty>
            No worker has registered yet. Start one with <code>docker compose up -d worker-a worker-b</code>.
          </Empty>
        ) : (
          <>
            <WorkerTable workers={current} testId="workers-table" />
            {history.length > 0 && (
              <details className="history">
                <summary>
                  {history.length} earlier worker instance{history.length === 1 ? "" : "s"} (stopped or long gone)
                </summary>
                <WorkerTable workers={history} testId="workers-history" />
              </details>
            )}
          </>
        )}
        <p className="hint">
          A worker is healthy when its last heartbeat is newer than one lease ({o.settings.lease_seconds}s), measured
          with PostgreSQL&apos;s clock. A crashed worker turns stale; the scheduler then expires its leases and other
          workers take the work.
        </p>
      </section>

      <section className="card">
        <div className="card-head">
          <h2>Recent runs</h2>
          <Link to="/runs">All runs →</Link>
        </div>
        {recent.data && recent.data.items.length === 0 && (
          <Empty>
            No runs yet. <Link to="/submit">Submit a document</Link> to start one.
          </Empty>
        )}
        {recent.data && recent.data.items.length > 0 && (
          <ul className="run-list">
            {recent.data.items.map((r) => (
              <li key={r.id}>
                <Link to={`/runs/${r.id}`}>
                  <StatusBadge status={r.status} />
                  <span className="run-title">{r.title ?? r.workflow_name}</span>
                  <span className="muted mono">{shortId(r.id)}</span>
                  <span className="muted">
                    {r.task_succeeded}/{r.task_total} tasks · {seconds(ago(r.created_at, o.db_now))} ago
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}

function WorkerTable({ workers, testId }: { workers: Worker[]; testId: string }) {
  return (
    <div className="table-scroll">
          <table className="table" data-testid={testId}>
            <thead>
              <tr>
                <th>Worker</th>
                <th>Health</th>
                <th>Last heartbeat</th>
                <th>In flight</th>
                <th>Attempts</th>
                <th>Started</th>
              </tr>
            </thead>
            <tbody>
              {workers.map((w) => (
                <tr key={w.id} className={w.healthy ? "" : "row-dim"}>
                  <td>
                    <strong>{w.name}</strong>
                    <div className="muted mono small">{w.id}</div>
                  </td>
                  <td>
                    <StatusBadge status={w.healthy ? "healthy" : w.status === "stopped" ? "stopped" : "stale"} />
                  </td>
                  <td>{seconds(w.heartbeat_age_seconds)} ago</td>
                  <td>
                    {w.in_flight} / {w.concurrency}
                    {!w.healthy && w.status !== "stopped" && <div className="muted small">last reported</div>}
                  </td>
                  <td>
                    {w.attempts_total} total, {w.attempts_running} running
                  </td>
                  <td>{dateTime(w.started_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
    </div>
  );
}

import { Link, useSearchParams } from "react-router-dom";

import { api, RUN_STATUSES, type RunStatus } from "../api";
import { Empty, ErrorBox, Loading, StaleBanner, StatusBadge } from "../components/common";
import { dateTime, shortId } from "../format";
import { usePolling } from "../usePolling";

const PAGE = 25;

export function RunsPage() {
  const [params, setParams] = useSearchParams();
  const raw = params.get("status");
  const status = RUN_STATUSES.includes(raw as RunStatus) ? (raw as RunStatus) : null;
  const page = Math.max(0, Number(params.get("page") ?? 0) || 0);
  const runs = usePolling(() => api.runs(status, PAGE, page * PAGE), 2000, [status, page]);

  const select = (next: RunStatus | null) => {
    const p = new URLSearchParams();
    if (next) p.set("status", next);
    setParams(p);
  };
  const goto = (n: number) => {
    const p = new URLSearchParams(params);
    p.set("page", String(n));
    setParams(p);
  };

  return (
    <div className="page">
      <StaleBanner error={runs.stale ? runs.error : null} />
      <div className="page-head">
        <h1>Runs</h1>
        <Link className="btn" to="/submit">
          Submit document
        </Link>
      </div>
      <div className="filters" role="group" aria-label="Filter by status">
        <button className={`chip ${status === null ? "chip-on" : ""}`} onClick={() => select(null)}>
          all
        </button>
        {RUN_STATUSES.map((s) => (
          <button
            key={s}
            className={`chip chip-${s} ${status === s ? "chip-on" : ""}`}
            onClick={() => select(s)}
            data-testid={`filter-${s}`}
          >
            {s}
          </button>
        ))}
      </div>

      {runs.loading && <Loading label="Loading runs" />}
      {!runs.loading && !runs.data && runs.error && <ErrorBox error={runs.error} onRetry={runs.refresh} />}
      {runs.data && runs.data.items.length === 0 && (
        <Empty>{status ? `No ${status} runs.` : "No runs yet. Submit a document to start one."}</Empty>
      )}
      {runs.data && runs.data.items.length > 0 && (
        <>
          <table className="table runs-table" data-testid="runs-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Document</th>
                <th>Run</th>
                <th>Progress</th>
                <th>Attempts</th>
                <th>Manual retries</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {runs.data.items.map((r) => (
                <tr key={r.id}>
                  <td>
                    <StatusBadge status={r.status} />
                  </td>
                  <td>
                    <Link to={`/runs/${r.id}`}>{r.title ?? "(untitled)"}</Link>
                    <div className="muted small">
                      {r.workflow_name} v{r.workflow_version}
                    </div>
                  </td>
                  <td className="mono">{shortId(r.id)}</td>
                  <td>
                    <div className="progress" aria-label={`${r.task_succeeded} of ${r.task_total} tasks succeeded`}>
                      <div style={{ width: `${(100 * r.task_succeeded) / Math.max(1, r.task_total)}%` }} />
                    </div>
                    <span className="muted small">
                      {r.task_succeeded}/{r.task_total}
                    </span>
                  </td>
                  <td>{r.attempt_total}</td>
                  <td>{r.manual_retry_count}</td>
                  <td>{dateTime(r.created_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="pager">
            <button className="btn btn-small" disabled={page === 0} onClick={() => goto(page - 1)}>
              ← Newer
            </button>
            <span className="muted">
              {page * PAGE + 1}–{Math.min(runs.data.total, (page + 1) * PAGE)} of {runs.data.total}
            </span>
            <button
              className="btn btn-small"
              disabled={(page + 1) * PAGE >= runs.data.total}
              onClick={() => goto(page + 1)}
            >
              Older →
            </button>
          </div>
        </>
      )}
    </div>
  );
}

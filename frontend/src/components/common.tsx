import type { ReactNode } from "react";

import type { ApiError } from "../api";

export function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`badge badge-${status}`} data-testid="status-badge">
      {status.replace("_", " ")}
    </span>
  );
}

export function Loading({ label = "Loading" }: { label?: string }) {
  return (
    <div className="state state-loading" role="status">
      <span className="spinner" aria-hidden /> {label}…
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="state state-empty">{children}</div>;
}

export function ErrorBox({ error, onRetry }: { error: ApiError; onRetry?: () => void }) {
  const offline = error.status === 0 || error.status === 503;
  return (
    <div className="state state-error" role="alert">
      <strong>{offline ? "Backend unreachable" : "Request failed"}</strong>
      <span>{error.message}</span>
      {error.errors.length > 0 && (
        <ul>
          {error.errors.map((e) => (
            <li key={e}>{e}</li>
          ))}
        </ul>
      )}
      {onRetry && (
        <button className="btn btn-small" onClick={onRetry}>
          Try again
        </button>
      )}
    </div>
  );
}

export function StaleBanner({ error }: { error: ApiError | null }) {
  if (!error) return null;
  return (
    <div className="stale-banner" role="status">
      Live updates paused — {error.message}. Showing the last data received; retrying automatically.
    </div>
  );
}

export function JsonView({ value, label }: { value: unknown; label: string }) {
  if (value === null || value === undefined) {
    return <div className="json-empty">{label}: none yet</div>;
  }
  return (
    <details className="json" open>
      <summary>{label}</summary>
      <pre>{JSON.stringify(value, null, 2)}</pre>
    </details>
  );
}

export function Stat({ label, value, tone }: { label: string; value: ReactNode; tone?: string }) {
  return (
    <div className={`stat ${tone ? `stat-${tone}` : ""}`}>
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  );
}

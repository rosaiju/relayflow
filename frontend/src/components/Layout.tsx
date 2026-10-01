import { NavLink, Outlet } from "react-router-dom";

import { usePolling } from "../usePolling";

interface Health {
  status: string;
  database: string;
  fault_injection: boolean;
}

async function loadHealth(): Promise<Health> {
  const response = await fetch("/api/health").catch(() => null);
  if (!response) return { status: "unreachable", database: "unknown", fault_injection: false };
  return (await response.json()) as Health;
}

function Connection() {
  const { data, loading } = usePolling(loadHealth, 3000, []);
  let tone = "ok";
  let label = "Connected";
  if (loading && !data) {
    tone = "wait";
    label = "Connecting…";
  } else if (!data || data.status === "unreachable") {
    tone = "down";
    label = "API unreachable";
  } else if (data.database !== "ok") {
    tone = "down";
    label = "Database unreachable";
  }
  return (
    <div className={`connection connection-${tone}`} data-testid="connection" title="Polls /api/health every 3 s">
      <span className="dot" aria-hidden /> {label}
      {data?.fault_injection && <span className="fault-pill">fault injection on</span>}
    </div>
  );
}

export function Layout() {
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <svg viewBox="0 0 32 32" width="22" height="22" aria-hidden>
            <circle cx="9" cy="16" r="5" />
            <circle cx="23" cy="9" r="4" />
            <circle cx="23" cy="23" r="4" />
            <path d="M13 14l6-3M13 18l6 3" />
          </svg>
          RelayFlow
        </div>
        <nav>
          <NavLink to="/" end>
            Overview
          </NavLink>
          <NavLink to="/runs">Runs</NavLink>
          <NavLink to="/submit">Submit</NavLink>
        </nav>
        <Connection />
      </header>
      <main>
        <Outlet />
      </main>
      <footer>
        Educational project — at-least-once execution, local use only. State shown is read from PostgreSQL via the
        API.
      </footer>
    </div>
  );
}

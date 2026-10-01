// Time formatting. Relative times are computed against PostgreSQL's clock (`db_now` from the API)
// when available, so a skewed browser clock cannot make a lease look expired or alive.

export function shortId(id: string): string {
  return id.slice(0, 8);
}

export function workerLabel(workerId: string): string {
  // Worker ids look like "worker-a:hostname:pid:abcd1234". In Docker every pid is 1, so the
  // random suffix (unique per process start) is what distinguishes a restarted worker.
  const parts = workerId.split(":");
  const name = parts[0] ?? workerId;
  const instance = parts[3];
  return instance ? `${name} #${instance.slice(0, 4)}` : name;
}

export function clock(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleTimeString([], { hour12: false }) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

export function dateTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { hour12: false });
}

export function seconds(value: number): string {
  const abs = Math.abs(value);
  if (abs < 1) return `${Math.round(value * 1000)} ms`;
  if (abs < 120) return `${value.toFixed(1)} s`;
  return `${Math.round(value / 60)} min`;
}

/** Seconds from `iso` until `now` (positive = in the past). */
export function ago(iso: string, nowIso: string): number {
  return (new Date(nowIso).getTime() - new Date(iso).getTime()) / 1000;
}

export function duration(startIso: string, endIso: string | null, nowIso: string): string {
  return seconds(ago(startIso, endIso ?? nowIso));
}

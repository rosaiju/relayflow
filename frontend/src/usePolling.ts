import { useCallback, useEffect, useRef, useState } from "react";

import { ApiError } from "./api";

export interface Polled<T> {
  data: T | null;
  error: ApiError | null;
  loading: boolean; // true only until the first response (success or failure)
  stale: boolean; // last refresh failed but older data is still shown
  refresh: () => void;
}

/**
 * Polls `load` every `intervalMs`. Polling (rather than SSE) keeps the server stateless and is
 * plenty for a local dashboard; every poll reads persisted state from PostgreSQL via the API.
 * Requests never overlap, and polling pauses while the tab is hidden.
 */
export function usePolling<T>(load: () => Promise<T>, intervalMs: number, deps: unknown[]): Polled<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [loading, setLoading] = useState(true);
  const inFlight = useRef(false);
  const generation = useRef(0);
  const loader = useCallback(load, deps);

  const tick = useCallback(async () => {
    if (inFlight.current || document.visibilityState === "hidden") return;
    inFlight.current = true;
    const mine = generation.current;
    try {
      const value = await loader();
      if (mine === generation.current) {
        setData(value);
        setError(null);
      }
    } catch (err) {
      if (mine === generation.current) {
        setError(err instanceof ApiError ? err : new ApiError(0, String(err)));
      }
    } finally {
      inFlight.current = false;
      if (mine === generation.current) setLoading(false);
    }
  }, [loader]);

  useEffect(() => {
    generation.current += 1;
    setData(null);
    setError(null);
    setLoading(true);
    inFlight.current = false;
    void tick();
    const timer = window.setInterval(() => void tick(), intervalMs);
    const onVisible = () => void tick();
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [tick, intervalMs]);

  return { data, error, loading, stale: error !== null && data !== null, refresh: () => void tick() };
}

import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { api, ApiError } from "../api";
import { ErrorBox } from "../components/common";
import { usePolling } from "../usePolling";

const SAMPLE_TITLE = "Harbor maintenance log (synthetic)";
const SAMPLE_TEXT = `The harbor crew inspected the north pier at dawn. The pier lights were dim, so the crew
replaced four lamps and logged the work. A storm warning arrived at noon; the crew secured the
boats, checked the mooring lines, and inspected the pier again before the storm. The storm passed
at night. In the morning the crew inspected the boats, the lines, and the lights once more.`;

const TASKS = ["validate", "word_count", "keywords", "report", "notify"] as const;

function newKey(): string {
  return `ui-${crypto.randomUUID()}`;
}

export function SubmitPage() {
  const navigate = useNavigate();
  const health = usePolling(
    async () => (await fetch("/api/health").then((r) => r.json())) as { fault_injection?: boolean },
    10000,
    [],
  );
  const faultsAvailable = health.data?.fault_injection === true;
  const [title, setTitle] = useState(SAMPLE_TITLE);
  const [text, setText] = useState(SAMPLE_TEXT);
  const [key, setKey] = useState(newKey);
  const [delayTask, setDelayTask] = useState<string>("");
  const [delay, setDelay] = useState(8);
  const [keywordFailures, setKeywordFailures] = useState<"none" | "once" | "exhaust">("none");
  const [crashAfterNotify, setCrashAfterNotify] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<ApiError | null>(null);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    const demo: Record<string, unknown> = {};
    if (delayTask) demo.delay_seconds = { [delayTask]: delay };
    if (faultsAvailable && keywordFailures !== "none") {
      demo.fail_attempts = { keywords: keywordFailures === "once" ? [1] : [1, 2, 3] };
    }
    if (faultsAvailable && crashAfterNotify) demo.crash_after_execute = { notify: [1] };
    const input: Record<string, unknown> = { title, text };
    if (Object.keys(demo).length) input.demo = demo;
    try {
      const result = await api.submit({ workflow_name: "document-processing", input, idempotency_key: key });
      navigate(`/runs/${result.run_id}`, { state: { created: result.created } });
    } catch (err) {
      setError(err instanceof ApiError ? err : new ApiError(0, String(err)));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="page narrow">
      <h1>Submit a document</h1>
      <p className="muted">
        Runs the <code>document-processing</code> workflow: validate → (word count ∥ keywords) → report → notify the
        mock notification service. Use synthetic text only.
      </p>
      <form className="card form" onSubmit={submit} data-testid="submit-form">
        <label>
          Title
          <input value={title} onChange={(e) => setTitle(e.target.value)} maxLength={200} required name="title" />
        </label>
        <label>
          Text <span className="muted small">({text.length.toLocaleString()} / 20,000 characters)</span>
          <textarea value={text} onChange={(e) => setText(e.target.value)} rows={8} maxLength={20000} required name="text" />
        </label>
        <label>
          Idempotency key
          <div className="row">
            <input value={key} onChange={(e) => setKey(e.target.value)} maxLength={200} className="mono" name="key" />
            <button type="button" className="btn btn-ghost" onClick={() => setKey(newKey())}>
              New key
            </button>
          </div>
          <span className="hint">
            Submitting again with the same key and the same document returns the same run. The same key with a different
            document is rejected (409).
          </span>
        </label>

        <fieldset>
          <legend>Demo options</legend>
          <label className="inline">
            Slow down task
            <select value={delayTask} onChange={(e) => setDelayTask(e.target.value)} name="delay-task">
              <option value="">none</option>
              {TASKS.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
            by
            <input
              type="number"
              min={1}
              max={30}
              value={delay}
              onChange={(e) => setDelay(Number(e.target.value))}
              className="num"
              name="delay"
            />
            seconds
          </label>
          <span className="hint">
            A cooperative delay gives you time to kill the worker running that task (for example{" "}
            <code>docker compose kill worker-a</code>) and watch another worker recover it.
          </span>
          {faultsAvailable ? (
            <>
              <label className="inline">
                Inject <code>keywords</code> failures
                <select
                  value={keywordFailures}
                  onChange={(e) => setKeywordFailures(e.target.value as "none" | "once" | "exhaust")}
                  name="keyword-failures"
                >
                  <option value="none">none</option>
                  <option value="once">attempt 1 only (automatic retry recovers)</option>
                  <option value="exhaust">attempts 1-3 (exhausts the retry budget; run fails)</option>
                </select>
              </label>
              <label className="check">
                <input type="checkbox" checked={crashAfterNotify} onChange={(e) => setCrashAfterNotify(e.target.checked)} />
                Crash the worker after <code>notify</code> attempt 1 delivers, before it reports success
              </label>
            </>
          ) : (
            <span className="hint">
              Fault-injection options are hidden because the backend runs with fault injection disabled (the default).
            </span>
          )}
        </fieldset>

        {error && <ErrorBox error={error} />}
        <div className="row end">
          <button className="btn btn-primary" type="submit" disabled={busy} data-testid="submit-button">
            {busy ? "Submitting…" : "Start workflow"}
          </button>
        </div>
      </form>
    </div>
  );
}

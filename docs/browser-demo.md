# Browser demo script (about 5 minutes)

Run these in PowerShell from the repository root. Each step lists what to say and what
to point at. Everything on screen is read from PostgreSQL through the API.

## Setup (before the audience arrives)

```powershell
$env:RELAYFLOW_ENABLE_FAULT_INJECTION = "1"   # enables the demo-only failure options
docker compose up -d --build
```

Open http://127.0.0.1:8080. The header should say **Connected**, with an orange
"fault injection on" pill. The Overview should list **worker-a** and **worker-b** as
*healthy*.

## 1. The happy path (1 min)

1. Go to **Submit** and keep the sample harbor log. Click **Start workflow**.
2. On the run page, point at the graph: `validate` → `word_count` ∥ `keywords` →
   `report` → `notify`. "Tasks are only dispatched after their dependencies succeed."
3. Click `notify`. Show the output: `notification_id`, `idempotency_key`, and
   `duplicate: false`.
4. Point at the event log: each line was written in the same transaction as the
   state change it describes.

## 2. Kill a worker mid-task (2 min)

1. **Submit** again and set *Slow down task* to `keywords` for `20` seconds. Start it.
2. The `keywords` node pulses blue (running). Click it: the attempts table shows which
   worker has it, plus the lease expiry time.
3. In PowerShell, kill **that** worker (replace `worker-a` with the one shown):
   ```powershell
   docker compose kill worker-a
   ```
4. Go to **Overview**: the killed worker turns *stale* once its heartbeat is older than
   one lease (10 s).
5. Back on the run page, about 11 s after the kill: attempt 1 becomes **lease expired**,
   and attempt 2 is claimed by the other worker. The **Recovered after worker failure**
   panel explains it with database timestamps. `validate` still shows one attempt: its
   completed output was preserved, not recomputed.
6. Restart the killed worker: `docker compose up -d worker-a`.

Say: "This is at-least-once. If the dead worker had already caused an external effect,
it would not be undone, which is why the notify step uses an idempotency key."

## 3. Duplicate-safe notification (1 min)

1. **Submit** with *Crash the worker after notify attempt 1 delivers* checked.
2. When `notify` runs, its worker delivers the notification and then exits on purpose,
   before telling RelayFlow. About 11 s later, attempt 2 runs on the other worker.
3. Click `notify`: its output shows `duplicate: true`. Open
   http://127.0.0.1:8100/notifications: there is **one** record for this run's key,
   with `delivery_count: 2`.

Say: "RelayFlow sent the same key twice, and the receiver deduplicated it. This only
works because the receiver implements idempotency."

## 4. Failure, blocking and manual retry (1 min)

1. **Submit** with *Inject keywords failures: attempts 1-3*.
2. Watch the **retry timing** note ("retry 2/3 becomes claimable in 0.8 s") while the
   backoff runs. After three failures `keywords` is **failed**, `report` and `notify`
   are **blocked**, `word_count` still **succeeded**, and the run is **failed**.
3. Click **Retry failed tasks**. Attempt 4 succeeds, and the run completes.
   `word_count` still has exactly one attempt.

## 5. Cancel (30 s)

1. Submit with *Slow down task* `keywords` 25 s, then click **Cancel** while it runs.
2. `keywords` stops at its next cooperative check. Downstream tasks become
   **cancelled**, and `validate` keeps its result.

## Cleanup

```powershell
docker compose stop
Remove-Item Env:RELAYFLOW_ENABLE_FAULT_INJECTION
```
(`docker compose down -v` would also delete the database volume.)

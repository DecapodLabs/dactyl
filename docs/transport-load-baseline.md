# Transport Pool Bounds, Fairness, and Load Baseline (`#144`)

Issue [#144](https://github.com/DecapodLabs/dactyl/issues/144) establishes the measured transport load, cancellation, fairness, and resource-bounds baseline for Dactyl's pooled HTTP adapters (`NeonAdapter`, `SupabaseAdapter`, `CloudConnector`) backed by `SharedHttpClient` (`src/http_pool.rs`) and `ResourceBudget` (`src/budget.rs`).

A passing offline/loopback fixture never implies live hosted execution. `local_postgres`, `hosted_supabase`, and `live_propodus` remain distinct external qualification profiles (`unavailable` until executed against live infrastructure).

---

## 1. Aggregate Pool Bounds & Client Sharing

`src/http_pool.rs` bounds concurrency, client proliferation, and file-descriptor growth across both synchronous and asynchronous callers:

| Bound | Constant / Field | Value / Range | Scope & Overflow Behavior |
| --- | --- | --- | --- |
| Process-wide in-flight HTTP ceiling | `MAX_PROCESS_IN_FLIGHT` | `64` | Shared `PROCESS_LIMIT` across all `SharedHttpClient` pools in the process. Callers exceeding the ceiling queue up to `request_timeout` and fail closed with `AdapterErrorKind::Timeout` (`MutationOutcome::NotAttempted`). |
| Live distinct budget pools | `MAX_LIVE_POOLS` | `64` | `CLIENT_CACHE` tracks `Weak<SharedHttpClient>` keyed by `PoolSettings { connect_timeout, max_in_flight, idle_per_host, idle_timeout }`. Dropped pools (`strong_count == 0`) are pruned on `acquire_client`; a 65th simultaneously live distinct `PoolSettings` fails closed with `AdapterErrorKind::Unavailable` (`MutationOutcome::NotAttempted`). |
| Per-pool in-flight limit | `ResourceBudget::http_max_in_flight` | `1..=64` (default `16`) | Enforced by each `SharedHttpClient`'s scoped `RequestLimit` before acquiring `PROCESS_LIMIT`. |
| Per-host idle keep-alive cap | `ResourceBudget::http_max_idle_per_host` | `0..=64` (default `32`) | Passed to `reqwest::ClientBuilder::pool_max_idle_per_host` and `reqwest::blocking::ClientBuilder::pool_max_idle_per_host`. Setting `0` disables keep-alive pooling. |
| Idle connection TTL | `ResourceBudget::http_idle_timeout` | `> 0` (default `15s`) | Passed to `pool_idle_timeout`. |
| Async runtimes cached per pool | `MAX_ASYNC_RUNTIMES_PER_POOL` | `16` | Keyed by `Option<tokio::runtime::Id>`; evicts oldest entry when exceeded so sequential/ephemeral Tokio runtimes never leak or reuse stale hyper connection tasks. |
| Wire & metadata caps | `max_wire_bytes`, `BODY_LIMIT`, `MAX_CLOUD_HEADERS` | `4 MiB` default (`16 KiB` auth), `16` headers / `4096 B` total | Enforced incrementally in `read_bounded_body` / `read_bounded_body_async` and `extract_response_metadata`. |

### Key Pool-Sharing Rules

- **Budget-key equality (`PoolSettings`)**: Two connections or connectors with identical `(connect_timeout, http_max_in_flight, http_max_idle_per_host, http_idle_timeout)` share the same `SharedHttpClient` and its scoped `RequestLimit` even if they target different origins or configure different `request_timeout` or `max_wire_bytes` values.
- **Sync / Async runtime isolation**:
  - Async callers (`*_async`) run directly on their active Tokio runtime via `async_client()` with zero worker-thread handoffs.
  - Synchronous callers outside Tokio execute directly on `blocking_client()`; synchronous callers inside a Tokio runtime offload blocking client initialization (`dactyl-http-init`), request execution (`dactyl-neon-http`, `dactyl-supabase-http`, `dactyl-control-rpc`, `dactyl-cloud-auth`), and `SharedHttpClient::drop` (`dactyl-http-drop`) onto short-lived OS threads so `reqwest::blocking` never panics on nested runtime drop.

---

## 2. Queueing Time vs. Execution Time & Deadline Semantics

### Does `request_timeout` cover both queueing and execution?

**Yes.** Every HTTP operation across `NeonAdapter`, `SupabaseAdapter`, and `CloudConnector` computes a single operation deadline **before** entering the pool queue:

```text
deadline = Instant::now() + budget.request_timeout()
  ├─ Phase 1 (Queueing): acquire_permit(deadline) / acquire_permit_async(deadline)
  │    ├─ Timeout in queue ──────────────────────────────► AdapterErrorKind::Timeout + MutationOutcome::NotAttempted
  │    └─ Deadline expired before .send() (remaining == 0) ► AdapterErrorKind::Timeout + MutationOutcome::NotAttempted
  └─ Phase 2 (Execution): remaining = deadline - Instant::now()
       ├─ reqwest .timeout(remaining) + tokio::time::timeout(remaining, send/read_bounded_body_async)
       ├─ bounded single pre-execution 401 use_dpop_nonce retry shares the same `deadline`
       └─ Timeout after .send() on mutating op ───────────► AdapterErrorKind::Timeout + MutationOutcome::Uncertain
```

- **Queueing time (`queue_ms`)** is the elapsed duration from operation entry until the server begins servicing the dispatched HTTP request (dominated by `RequestLimit` FIFO wait under saturation plus TCP/HTTP framing overhead).
- **Service / execution time (`service_ms`)** is the elapsed duration spent by the remote origin handling the request and streaming the response within the remaining budget (`deadline - Instant::now()`).
- **Pre-send vs. post-send boundary**:
  - Timeouts inside `RequestLimit` or between permit acquisition and `request.send()` (including before dispatching a `401 use_dpop_nonce` retry) fail with `MutationOutcome::NotAttempted` and `EffectStatus::NotAttempted` (`ObservationSource::LocalPreflight` on Supabase/CloudConnector).
  - Timeouts or abrupt socket resets after `request.send()` on mutating operations fail with `MutationOutcome::Uncertain` (and `EffectProvenance::transport_fault` / `"outcome_unknown"`), never auto-replayed by Dactyl.

---

## 3. Fairness & Isolation Limits (and Propodus Boundary)

- **FIFO arrival-order slot handoff (`RequestLimit`)**: Both scoped pool limits and `PROCESS_LIMIT` use a unified FIFO `VecDeque` waiter queue (`LimitState`) across synchronous (`Condvar`) and asynchronous (`tokio::sync::Notify`) callers:
  - Newly arriving hot callers cannot barge ahead of queued waiters (`state.waiters.is_empty()` is required on the fast path).
  - Releasing a `RequestSlot` transfers ownership directly to the oldest non-granted waiter and wakes only that waiter (preventing thundering-herd wakeups and sync/async starvation).
  - `WaiterGuard` deterministically cancels queued waiters on timeout or dropped Tokio futures (`tokio::select!`, `JoinHandle::abort()`, `tokio::time::timeout`), immediately forwarding any already-granted slot to the next waiter so slots never leak.
- **What Dactyl intentionally does NOT implement**:
  - Dactyl does **not** implement per-tenant, per-org, or per-user weighted queues, admission quotas, priority tiers, or background retry/scheduler loops (`src/budget.rs`).
  - **Propodus** owns tenant admission policy and multi-tenant quota enforcement; Dactyl enforces only local in-process pool bounds (`ResourceBudget` + `MAX_PROCESS_IN_FLIGHT` + `MAX_LIVE_POOLS`), FIFO slot fairness within a pool, and typed failure/provenance reporting.

---

## 4. Measured Baseline & Regression Thresholds

Measured via `cargo test --locked --all-features --test transport_load_fairness_144 -- --nocapture` against loopback TCP/HTTP keep-alive origins on macOS arm64 (debug build, fixed PRNG seeds `0xD4C7_1440_2026_0001` and `0xD4C7_1440_2026_0002`):

### 4.1 Sustained Mixed Sync/Async Multi-Origin Workload (`240` measured + `3` warmup ops)

Workload: 3 origins (`Neon`, `Supabase`, `CloudConnector`), 2 distinct `PoolSettings` budgets (`http_max_in_flight = 6` shared by Supabase+Cloud, `http_max_in_flight = 4` for Neon), 180 hot async ops, 40 concurrent sync OS-thread ops, and 20 periodic cold sync/async ops (`3–7 ms` per-request server service delay).

| Metric | Observed Baseline | CI Regression Threshold |
| --- | --- | --- |
| Total latency `p50` / `p95` / `p99` | `125.04 ms` / `225.71 ms` / `232.92 ms` | `p99 < 1500.0 ms` |
| Service latency `p50` / `p95` / `p99` | `6.66 ms` / `9.16 ms` / `9.53 ms` | Bounded by `3–7 ms` synthetic delay |
| Queue+wire latency `p50` / `p95` / `p99` | `119.50 ms` / `219.53 ms` / `227.49 ms` | `p99 < 1400.0 ms` |
| Cold sync max / Cold async max | `169.93 ms` / `235.92 ms` | `0` timeouts / `0` starvation |
| Peak in-flight (Neon / Supabase / Cloud) | `4` (`<= 4`) / `6` (`<= 6`) / `4` (`<= 6`) | Strictly `<= http_max_in_flight` per pool |
| Accepted TCP connections / Peak idle | `28` across `243` reqs / `28` idle | `<= 48` accepted (`>= 5x` keep-alive reuse) |
| Open FDs (`before` -> `peak` -> `keepalive` -> `after_drop`) | `45` -> `94` -> `92` (`+47`) -> `19` (`-26`) | Keep-alive delta `<= +64`; post-drop delta `<= +8` |
| Process RSS (`before` -> `peak` -> `after`) | `16.72 MiB` -> `20.62 MiB` -> `19.06 MiB` (`+2.34 MiB`) | RSS delta `<= +64 MiB` |
| Decoded result bytes / Wire response bytes | `6,796 B` / `24,654 B` | Exact expected byte accounting |

### 4.2 Burst Fault, Cancellation, Oversized & Churn Workload (`160` ops)

Workload: Mixed pooled (`idle=6`) + unpooled churn (`idle=0`), sync + async callers, slow headers (`180 ms > 95 ms` timeout), stalled body reads after fast headers, abrupt TCP disconnects mid-JSON, unframed `16 KiB` oversized payloads (`max_wire_bytes = 2048`), and `12 ms` dropped Tokio futures, followed by a 12-way post-burst health probe.

| Metric | Observed Baseline | CI Regression Threshold |
| --- | --- | --- |
| Outcomes (`ok` / `cancelled` / `typed_faults`) | `31` / `16` / `113` | `0` panics, `0` untyped faults |
| Accepted TCP connections / Open after drop | `40` / `0` | `open_conns_after == 0` |
| Post-burst 12-way probe | `12/12` succeeded | `0` leaked permits or deadlocks |
| Post-drop FD delta (`before` -> `after_drop`) | `22` -> `11` (`-11`) | Post-drop FD delta `<= +16` |
| Process RSS (`before` -> `after`) | `17.59 MiB` -> `19.23 MiB` (`+1.64 MiB`) | Bounded growth without leaks |

### 4.3 Verified Boundary Tests (`tests/transport_load_fairness_144.rs` & `src/http_pool.rs`)

- **Queue + Execution Deadline Accounting**: `deadlines_cover_queue_plus_execution_and_presend_timeouts_stay_not_attempted` proves queue-expired mutations across Neon, Supabase, and CloudConnector return `AdapterErrorKind::Timeout` + `MutationOutcome::NotAttempted` (`EffectStatus::NotAttempted`, `LocalPreflight`) without reaching the server, while operations that exhaust remaining budget post-send return `MutationOutcome::Uncertain`.
- **Process-Wide Ceiling & Pool Exhaustion**: `aggregate_process_ceiling_and_distinct_budget_pool_bounds_under_mixed_load` proves `MAX_LIVE_POOLS = 64` fails closed at pool 65, recovers when a pool drops, and caps 96 concurrent requests across 64 distinct pools (`sum(http_max_in_flight) = 128`) to `peak_in_flight <= 64` (`MAX_PROCESS_IN_FLIGHT`).
- **FIFO Mixed Sync/Async Handoff & Cancellation**: `http_pool::pool_tests::fifo_order_and_cancellation_handoff_preserve_mixed_sync_async_fairness` proves strict FIFO ordering across interleaved sync and async waiters and clean slot forwarding when a granted async future is dropped before its next poll.

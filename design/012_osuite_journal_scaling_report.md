# 012 — Osuite chat replay and Tantra journal scaling: investigation report

## Finding

Osuite's slow historical-chat display is primarily an application projection problem. Its browser subscribes from sequence zero for an uncached chat and receives every Tantra event, including reasoning and tool-argument deltas it never displays. A separate Tantra scaling defect exists: a coordinated writable connection eagerly reloads and parses the complete root journal before it can claim or release writer control. Sending a command also repeats full-journal work. Fix that library path in a separate Tantra phase; do not change Tantra's durable event stream to mean UI messages.

## Evidence, 2026-10-02

The inspected Tantra checkout is commit `1258f12`, which Osuite pins in `backend/app/pyproject.toml`.

- Three existing Osuite sessions contained 4,427, 4,243, and 2,754 events but only 38, 10, and 16 user-input plus final-text entries respectively. These entries are an upper bound on the proposed UI message count because several text parts can belong to one assistant turn. In the 4,243-event session, 2,923 were `reasoning_delta`, 626 were `tool_call_delta`, and 585 were `text_delta`.
- `PostgresStore.read_page` parsed the complete 2,754–4,427-event journals in 18–48 ms against local PostgreSQL; `reduce_journal` took another 2–3 ms. These measurements do not support blaming PostgreSQL page reads for the observed seconds-long UI load. They do not measure the full coordinator claim path or remote deployments.
- Osuite's `DashboardSocket.stream` sends each raw event frame. The browser reduces and persists each frame, including ignored event types, and re-renders its transcript. Osuite will address that in its separate P4 history phase.
- In `packages/tantra/src/tantra/runtime.py`, `Runtime._claim` routes a coordinated writable connection to `claim_writer`. `Runtime._handle_request` calls `_tree_headers` and `_journal` for every actor **before** branching on `ClaimWriterPayload` or `ReleaseWriterPayload`. The claim/release branches use no journal data. Thus a chat open/close pays work proportional to all prior events even if no model command runs.
- `SendPayload` uses those preloaded journals for command lookup, then `CoordinatedStore.enqueue` in `packages/tantra/src/tantra/coordinator.py` reads and parses all root events again to check duplicate command IDs. Uncoordinated `PostgresStore.enqueue` in `packages/tantra/src/tantra/stores/postgres.py` has the same full scan. This is a separate scaling concern for long-lived conversations, not proof that it dominates today's UI delay.
- Tantra's compaction changes model context, not durable journal retention. Truncating journals or skill tool results would weaken replay, command idempotency, and model recovery. Osuite must redact skill bodies only in its UI projection; the journal retains them.

## Separate Tantra phase proposed

1. Add a regression benchmark/test with increasingly long PostgreSQL journals. Measure coordinated claim, release, first send, duplicate send, and takeover; assert replay and writer fencing remain correct.
2. Remove eager journal loading from claim/release. For send, replace duplicate-command full scans with a durable indexed lookup or equivalently bounded store operation, preserving same-UUID/same-payload idempotency and cross-actor command identity. Avoid a second scan inside `enqueue`.
3. Keep `Runtime.connect(after=...)` and `Store.read_page` event semantics intact. Verify two-instance owner replacement, crash recovery, cancellation, and replay under both short and long journals. Document before/after latency and query count.

## Boundary

Osuite P4 returns its latest 20 visible messages from an authorized application endpoint and subscribes after that response's sequence watermark. It can ship without the Tantra optimization. The Tantra phase should improve runtime command/connection scaling for every consumer, without adding Osuite-specific message counting or UI filtering to the library.

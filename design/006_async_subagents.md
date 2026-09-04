# Async subagents — Spec

## Goal
- Replace blocking delegation with attached async tasks: a parent launches subagents, keeps reasoning, inspects them, exchanges messages, waits without polling the model, and force-kills a task subtree.
- Sarathi users may message a busy root; the root alone decides whether to redirect that input to descendants.

## Scope
- **In:** core async child lifecycle; durable agent inboxes and actionable notices; parent-to-descendant messages; child-to-parent notifications; status, event, transcript, result, wait, and kill tools; per-harness concurrency; recovery; Sarathi pilot; docs; `tantra-harness` 0.5.0.
- **Out:** detached/background execution after the root stream closes; a scheduler or worker registry; cross-process pub/sub; direct user-to-child messages or child controls; ordinary-tool inboxes; process termination; side-effect rollback; arbitrary JSON messages; Agni UX; compatibility shims for blocking delegation.

## Decisions
- **Execution ownership:** all tasks stay attached to the consumed root `run()`/`resume()` stream. This preserves Tantra's consumption-driven durability model and avoids introducing a scheduler.
- **Delegation contract:** generated subagent tools and `Context.spawn()` return a durable `TaskRef` immediately. `Context.fan_out()` is removed; repeated async spawns plus the harness concurrency cap replace it.
- **Concurrency:** `Harness(max_concurrency=4)` limits running descendants across one root tree. Excess tasks remain durably queued and can be inspected, messaged, or killed before starting.
- **Parent liveness:** a parent remains model-active after spawning. It explicitly calls `task_wait` to sleep until user input or an actionable task notice arrives.
- **Parent completion:** an agent cannot complete while it owns unfinished tasks. An attempted final response is retained as non-terminal assistant text, followed by a harness notice requiring `task_wait` or `task_kill`, then resampling.
- **Message targets:** Sarathi users target root sessions only. An agent may inspect, message, wait for, read results from, or kill only its own descendants.
- **Delivery:** messages persist immediately and enter the target's context before its next provider sample. Active provider requests and active tools finish first unless the owning parent force-kills the task.
- **Stale tool calls:** a message or actionable notice received after a sample but before all sampled tool calls finish causes every not-yet-started call in that sample to receive a synthetic skipped result. The agent resamples with the new information instead of executing stale actions.
- **Automatic notices:** only task completion, failure, kill, explicit child notification, and root user input wake a waiting agent or enter its next sample automatically. Routine child events remain inspectable and live-streamed but do not consume parent context.
- **Notice batching:** all pending actionable notices enter the next sample in durable sequence order and are deduplicated by ID.
- **Inspection:** `task_status` returns lifecycle state and a bounded event page; `task_messages` returns the last `N` model-visible messages. Reasoning blocks and live token deltas are excluded.
- **Result delivery:** terminal notices contain task identity and state, not the full output. The parent calls `task_result` for final text or `submit_output` data.
- **Child notification:** a child may send its direct parent one bounded text message through `notify_parent`. Lifecycle events remain structured harness notices.
- **Graceful shutdown:** the parent sends an ordinary instruction asking the child to persist work and finish. There is no separate cooperative-stop task tool.
- **Force kill:** `task_kill` first persists intent, then immediately cancels locally owned child coroutines for the target subtree. It does not wait for cleanup or roll back external effects.
- **Remote kill:** a kill written from another process is durable but only noticed at the next store poll or execution boundary. Immediate coroutine cancellation is guaranteed only inside the root-stream owner.
- **Tool cancellation:** force-killing an agent cancels its active tool coroutine. Bundled subprocess tools must terminate their process groups on `CancelledError`; custom tools own cancellation cleanup.
- **Failure:** child failure generates an actionable parent notice; siblings continue and the parent decides recovery.
- **Breaking release:** ship as `tantra-harness` 0.5.0 with no compatibility layer. Update all in-repo callers and migration docs.

## Existing behavior being replaced
- `packages/tantra/src/tantra/harness.py:_subagent_tool` currently awaits `ctx.spawn()` and returns only the terminal child result.
- `packages/tantra/src/tantra/loop.py:TurnLoop._spawn` drives one child inline; `_fan_out` owns temporary worker tasks and resolves only after every child finishes.
- `packages/tantra/src/tantra/context.py:assemble_messages` turns only `TurnStarted.input` into user input; no mid-turn inbox exists.
- `packages/tantra/src/tantra/loop.py:TurnLoop._append` absorbs concurrent `CancelRequested` records only. Any other blind append raises `SeqConflict`.
- `apps/sarathi/backend/src/sarathi/api/ws.py` queues user frames in memory until the active turn exits. The composer is disabled while running in `apps/sarathi/ui/src/app/chat/[sessionId]/page.tsx`.

## Public contract

### Python
- Add exported immutable `TaskRef` with `task_id: str` and `agent: str`.
- Change `Context.spawn(agent: str, input: str) -> TaskRef`. It creates and links the child before returning; child execution may still be queued by `max_concurrency`.
- Remove `Context.fan_out` and its callback plumbing.
- Add `Harness.send_user_message(root_session_id: str, message: str) -> str`. It validates `parent_id is None`, a live incomplete root turn, non-empty text, and the 32,768-character limit; it returns `message_id` after persistence.
- Add `Harness(max_concurrency: int = 4)`. Reject values below one.
- Preserve `Harness.cancel()` for host-requested cooperative cancellation and Sarathi's root Stop action. Async task launch and task kill do not reuse its turn-specific contract.

### Model tools
- Reserve these names alongside `submit_output`; harness construction fails on custom-tool or subagent-name collisions.
- Generated `<agent>(task: str)` launches that declared subagent and returns `{"task_id": ..., "agent": ...}` immediately.
- `task_status(task_id: str, after_seq: int | None = None, limit: int = 20)` returns state, agent, parent, depth, latest sequence, a chronological event page, `next_seq`, and `has_more`. Valid range: 1-100.
- `task_messages(task_id: str, limit: int = 20)` returns the latest 1-100 model-visible user, assistant, and tool messages in chronological order. It excludes system prompts, reasoning, deltas, and synthetic provider retries.
- `task_result(task_id: str)` returns terminal text, structured output, failure, or killed state. It errors while queued, running, waiting, or awaiting input.
- `task_send(task_id: str, message: str)` persists an instruction and returns its message ID. It rejects terminal tasks, empty text, text above 32,768 characters, and non-descendants.
- `task_kill(task_id: str)` persists and force-kills the whole target subtree. Repeating it returns the existing killed state without new effects.
- `task_wait(task_ids: list[str] | None = None)` waits for the next actionable notice or user message. `None` means all unfinished owned descendants; explicit IDs must be owned descendants. It returns immediately if a matching notice is already pending or every selected task is terminal.
- `notify_parent(message: str)` persists one direct-parent message and returns its message ID. It rejects root sessions, empty text, and text above 32,768 characters.

## Durable event contract
- Update the event-union freeze in `design/001_v1_spec.md` before changing `packages/tantra/src/tantra/events.py`; record why externally appended control events are now valid.
- Add `AgentMessageQueued`:
  - `type = "agent_message_queued"`
  - `message_id: str`
  - `sender_session_id: str | None`; `None` only for a Sarathi/host user message to a root.
  - `source: Literal["user", "parent", "child"]`
  - `text: str`
- Add `TaskNoticeQueued`:
  - `type = "task_notice_queued"`
  - `notice_id: str`
  - `task_session_id: str`
  - `state: Literal["completed", "failed", "killed"]`
  - `terminal_seq: int`
- Add `KillRequested`:
  - `type = "kill_requested"`
  - `request_id: str`
  - `requested_by_session_id: str`
- Keep `CancelRequested` unchanged so old logs and host cooperative cancellation retain their meaning.
- All three new event types are legal blind appends and legal sequence-conflict absorptions in `TurnLoop._append`. Any other foreign event still raises `SeqConflict`.
- Message IDs derive from the initiating tool `call_id` for agent-originated messages and are UUIDs for host messages. Notice IDs derive from `task_session_id + terminal_seq`. Replay may append duplicate envelopes after a cross-session crash; state and context deduplicate by ID.
- Task IDs are child session IDs. No second task table or persisted handle store is introduced.
- Child state is derived from its log, not `SessionHeader.status`:
  - `queued`: child exists, has no `TurnStarted`, and has no `KillRequested`.
  - `running`: latest turn is incomplete and no ask or `task_wait` call is pending.
  - `waiting`: an incomplete `task_wait` call is pending.
  - `awaiting_input`: an unanswered `AskRaised` is pending.
  - `completed`: terminal `TurnCompleted` without `stop_reason="killed"`.
  - `failed`: terminal `TurnFailed`.
  - `killed`: `KillRequested` is effective, including a queued task with no turn.
- `SessionHeader.status` remains a non-authoritative projection. Do not add a task database or rely on header state for correctness.

## Context and delivery
- `assemble_messages()` renders each deduplicated `AgentMessageQueued` once as a `UserMessage` with a stable source prefix and message ID; the original text is unchanged.
- `TaskNoticeQueued` renders as a concise `UserMessage` containing task ID, agent, and terminal state. Output is never embedded.
- A message is pending when no later `SampleStarted` exists in that session. Starting a sample consumes every earlier pending message/notice as one ordered batch; no acknowledgement event is needed.
- A pending inbox item blocks compaction from moving the effective floor past that item before its first sample.
- At provider completion, before each tool call, after each tool call, and before terminal completion, refresh the session suffix and absorb messages, notices, cancellation, and kill requests.
- When pending inbox items exist, synthesize `ToolCallCompleted(is_error=True)` for every requested but unstarted call with result `skipped: newer agent message`; preserve normal tool-call/result pairing before resampling.
- A message arriving during a tool does not interrupt that tool. Once it returns, remaining calls are skipped and the target resamples.
- Live deltas stay live-only. `task_messages` reads persisted completed samples, so an in-flight answer appears only after its durable parts land.

## Attached task supervisor
- Create one internal supervisor per consumed root `run()`/`resume()` stream and pass it to every descendant loop. Do not expose it as a scheduler service.
- The supervisor owns:
  - the tree-wide semaphore configured by `Harness.max_concurrency`;
  - local child `asyncio.Task`s keyed by child session ID;
  - a merged `Emitted` queue so root consumers continue receiving all descendant events;
  - local wakeups for inbox, notice, completion, and kill events;
  - reconstruction of queued and incomplete descendants on root resume.
- `Context.spawn()` resolves after child creation and parent `ChildSessionSpawned` persistence, not after semaphore admission or child completion.
- Fix the existing child-creation gap: if child creation succeeds but parent linkage does not, resume reuses the orphan by a deterministic child ID derived from parent session ID, parent call ID, and spawn index. It must never create a twin.
- Queued tasks acquire no lease and start no turn. On semaphore admission they use ordinary `Harness.run()`; incomplete tasks use `Harness.resume()`.
- Closing or abandoning the root generator cancels local runners without writing `KillRequested`; queued and incomplete tasks remain resumable. Reconnect reconstructs the tree and reapplies the same cap.
- The supervisor emits descendant events through the root stream exactly once. Child events remain persisted only in child logs; recursive replay remains required.
- A waiting parent uses local wakeups first and polls its own event-log suffix every one second for cross-process messages. It refreshes its lease at least every `lease_ttl / 3` while waiting.
- `task_wait` suspension is replayable through the existing incomplete tool call. It does not use `AskRaised`, does not set `pending_ask`, and does not suspend ancestors.

## Completion and notification flow
1. A child reaches `TurnCompleted`, `TurnFailed`, or effective `KillRequested`.
2. The supervisor records the terminal child state before notifying the parent.
3. It appends one deterministic `TaskNoticeQueued` to the direct parent's log and wakes that parent locally.
4. If the process dies between terminal persistence and notice append, root resume scans linked terminal children and appends any missing deterministic notices.
5. The parent receives ordered notices before its next sample or as the wake reason from `task_wait`.
6. The parent calls `task_result` when it needs the output. Normal child completion never resolves the original launch tool again.
- Explicit `notify_parent` uses its child tool-call ID as message ID. Replay can repeat the cross-log append but cannot repeat the model-visible message.
- A grandchild notifies its direct parent. Notices do not fan out to every ancestor.

## Kill flow
1. Validate that the target is a descendant of the caller using `SessionHeader.parent_id` traversal.
2. Snapshot the target subtree and append `KillRequested` deepest-first, including queued sessions.
3. Cancel every locally owned runner in that subtree immediately; do not await graceful child completion before returning `task_kill`.
4. Each cancelled active loop converts supervisor cancellation into durable `TurnCompleted(stop_reason="killed")`, settles leases, and emits one parent notice. Do not persist `TurnFailed` for intentional kill.
5. A queued task with `KillRequested` never starts. Its state is terminal without a synthetic `TurnStarted`.
6. If descendants appear after the first snapshot, the target's effective kill prevents new spawn admission and a second descendant scan kills any child linked during the race.
- `task_kill` cannot undo filesystem, network, database, or subprocess effects completed before cancellation.
- Update `packages/tantra/src/tantra/extratools/shell.py` so timeout and coroutine cancellation both terminate and await the process group before re-raising.
- Telemetry maps killed turns to existing outcome `cancelled` with `stop_reason="killed"`; do not add a new `Tracer` protocol outcome.

## Sarathi pilot
- Keep the WebSocket client contract targetless: `user_message` always addresses the root attached to that socket; `cancel` still addresses the root tree.
- When the root has an incomplete turn, persist `user_message` through `Harness.send_user_message()` instead of placing it in the connection-local FIFO. The active root absorbs it at its next boundary or wakes from `task_wait`.
- When the root is idle, `user_message` continues to start a new turn through `Harness.run()`.
- Persist before acknowledging or rendering a sent message. Reconnect must replay the message from the root log; disconnect must not lose it.
- Enable the composer while status is running or waiting. Keep direct controls out of `SubagentBlock`; the user steers children by telling the root.
- Extend the chat reducer for new message/notice/kill events and async launch results. Nested child activity continues routing by `Emitted.session_id` and `depth`.
- Render queued/running/waiting/awaiting-input/completed/failed/killed task state. Do not render hidden reasoning in parent inspection results.
- Keep the root Stop button. It calls existing recursive `Harness.cancel()`; it is not a per-child force-kill control.
- A second WebSocket may append a root user message without acquiring the turn lease. Normal ownership metadata checks still apply.

## Observability
- Keep child `invoke_agent` spans parented to the launch tool span by capturing its tracer handle at spawn. The child span may outlive the short launch span; document this rather than adding span links or a scheduler trace model.
- Add task ID, message source, terminal state, and killed stop reason to existing event-derived logs/UI, not message bodies to telemetry content when content capture is off.
- Hooks receive each newly persisted event once from its owning loop. Events absorbed after a blind append must also be surfaced once to the active root stream and hooks; current cancellation absorption silently drops that live acknowledgement and must be corrected for all absorbable control events.

## Considered & rejected
- **Keep blocking delegation beside async handles** — rejected to avoid two orchestration models and because the user chose a breaking replacement.
- **Retain `fan_out`** — repeated async spawn plus one tree-wide cap covers it without a second waiting API.
- **Detached children** — requires scheduling, worker recovery, and cross-process notification ownership; attached tasks satisfy the Sarathi use case.
- **Immediate cross-process kill** — requires pub/sub and a worker registry. Durable boundary polling is sufficient outside the owning process.
- **Terminate worker processes** — per-child process isolation is a different runtime and is not needed to cancel owned coroutines.
- **Cooperative `task_stop`** — ordinary messaging handles save-and-exit; `task_kill` is the only dedicated stop operation.
- **Direct user-to-child control** — bypasses root orchestration and adds authorization/UI paths with no requested use case.
- **Ordinary-tool inboxes** — turns the harness into a generic actor system. Tools remain cancellable only through their owning agent.
- **Every child event as parent context** — routine progress would flood model context. Event cursors provide opt-in visibility.
- **Full transcript including reasoning** — provider-specific hidden reasoning is unnecessary for control and unsafe to copy into another model context.
- **Arbitrary JSON or typed notification kinds** — bounded text covers agent coordination; asks, progress, and lifecycle events already have typed contracts.
- **Compatibility shim/deprecation cycle** — the package is pre-1.0 and every in-repo consumer will migrate in the same release.

## Implementation phases

```text
P0 durable live inbox
  └─ P1 attached async tasks
       └─ P2 task communication + force kill
            ├─ P3 Sarathi pilot
            └─ P4 docs + 0.5 release (after P3 verification)
```

- P0-P2 are sequential because each changes the same event/context/loop contracts.
- P3 may begin only after P2's model tools freeze. P4 documentation can be drafted alongside P3, but the version bump and release verification wait for P3.

### Conventions (all phases)
- uv workspace; Python 3.13; Ruff line length 120; asyncio pytest auto mode; no code comments.
- Core tests use `FakeProvider` + `MemoryStore`, no network; concurrency tests use gated providers/events and assert both emitted streams and durable logs.
- Store behavior goes through `packages/tantra/src/tantra/testing.py`; recovery tests rebuild a fresh `Harness` over the same store.
- Root commands: `just lint`, `just test`, and `just stress`. Sarathi backend: `just lint` + `just test` from `apps/sarathi/backend/`. UI: `yarn lint` + `yarn build` from `apps/sarathi/ui/`. Docs: `uv run mkdocs build --strict --site-dir out/docs`.
- Run the phase-specific checks plus root `just lint` + `just test` before marking a phase done.
- **Contract freeze:** from P0, the three event schemas, ID derivation, ordering, deduplication, and boundary-delivery rules; from P1, `TaskRef`, task states, generated/model tool schemas, and attached supervisor ownership. Changing them means updating this spec first, then telling dependent phases.

### Keeping this spec current
- Update the status marker on the heading and tick the checklist as you go.
- When the build deviates from the plan, **strike the original line and say why it changed** — `~~original~~ **Cut in P4.** <reason>`. Never silently rewrite; the reason a plan changed is worth more than the plan.
- After a phase lands, add only detail that would surprise the next reader — a constant whose value is load-bearing, a behavior that isn't what the name suggests, an ordering that matters. Skip anything the code already says plainly.
- Problems found but not fixed go to Open Decisions or a Follow-up note, with enough detail to act on later. Don't fix them inline and don't leave them unrecorded.

### Phase 0 — durable live inbox · deps: none · blocks all · —
- Update `design/001_v1_spec.md`, `packages/tantra/src/tantra/events.py`, exact event-union tests, and event docs for `AgentMessageQueued`, `TaskNoticeQueued`, and `KillRequested`.
- Add `Harness.send_user_message`, blind-append absorption, ID deduplication, model rendering, pending-message detection, compaction floor protection, and stale tool-call skipping.
- Surface absorbed control events once through hooks and the active stream.
- This phase ships standalone as durable host/user steering of an active root at safe boundaries; no async child API exists yet.
- **Verify:** gated-provider tests append a root message from a fresh harness during a sample and during a tool; the first skips all sampled calls, the second lets the active call finish and skips the remainder; one ordered message appears after process replay; unknown foreign events still raise `SeqConflict`; all store conformance suites parse and retain the new events.
- Checklist:
  - [x] Existing contract freeze updated before event code
  - [x] Event schemas, parser union, exports, and exact-union tests
  - [x] Root-only public message ingress and validation
  - [x] Conflict absorption, live acknowledgement, and deduplication
  - [x] Context rendering, compaction guard, and stale-call synthesis
  - [ ] Cross-harness and all-store tests
  - [ ] `just lint` + `just test`
- **Follow-up:** PostgreSQL conformance remains pending because Docker/PostgreSQL is unavailable in this environment.

### Phase 1 — attached async task lifecycle · deps: P0 · —
- Add `TaskRef`, `Harness.max_concurrency`, and the root-scoped supervisor; change generated delegates and `Context.spawn`; remove `fan_out`, and migrate core callers. ~~Migrate Agni and Sarathi agent callers in P1.~~ **Moved to P4 by user scope decision.**
- Implement deterministic child identity/link recovery, durable queued state, tree reconstruction, merged live events, lease heartbeat, task completion notices, and no-orphan parent completion.
- Add `task_status`, `task_messages`, `task_result`, and `task_wait` with lineage authorization and fixed bounds.
- Preserve existing child asks: `awaiting_input` remains visible, answering the child and resuming the root reconstructs the attached tree.
- Update tracing tests for concurrently active child spans and exactly-once hook/event forwarding.
- **Verify:** one parent launches six children with `max_concurrency=2`, continues sampling before they finish, inspects queued/running transcripts, waits without additional provider calls, receives ordered terminal notices, and reads all results; abandoning then resuming with a fresh harness creates no twins, never exceeds two active children, and completes the same task IDs.
- Checklist:
  - [x] `TaskRef`, async spawn, generated launch tools, `fan_out` removal
  - [x] Tree-wide supervisor, queue cap, event merge, and heartbeat
  - [x] Deterministic child creation and fresh-harness reconstruction
  - [x] Status, transcript, result, and wait tools
  - [x] Parent completion guard and actionable terminal notices
  - [x] Ask, tracing, hook, abandonment, and nested-tree regressions
  - [ ] `just lint` + `just test` + targeted stress scenario
- **Follow-up:** PostgreSQL stress remains skipped because Docker is unavailable; the exact `just` wrappers could not run because `just` and `uv` are absent, so `.venv` Ruff/Pytest equivalents were used.

### Phase 2 — task communication and force kill · deps: P1 · —
- Add `task_send`, `notify_parent`, and `task_kill`; enforce descendant/direct-parent authority and message bounds.
- Wire local wakeups and one-second remote polling for parent/child messages, explicit notifications, and kills.
- Implement deepest-first two-pass subtree kill, queued-task kill, local coroutine cancellation, durable killed completion, deterministic parent notices, and replay idempotency.
- Make bundled shell subprocesses cancellation-safe; document custom-tool cleanup and irreversible-side-effect limits.
- **Verify:** nested root/mid/leaf tests prove root-to-leaf course correction skips stale calls, leaf notification wakes only mid, a save-and-exit message returns persisted output, force-kill interrupts an active provider and active shell process locally, queued descendants never start, siblings continue, duplicate replay events appear once in model context, and remote kill takes effect at the next boundary.
- Checklist:
  - [x] Parent-to-descendant and child-to-parent message tools
  - [x] Lineage checks, bounds, ordering, and deterministic IDs
  - [x] Local wakeups and remote wait polling
  - [x] Recursive immediate kill and race-closing second scan
  - [x] Shell and custom-tool cancellation contract
  - [x] Nested, replay, and side-effect tests
  - [x] MemoryStore, FileSystemStore, and SQLiteStore stress tests
  - [ ] PostgreSQL stress tests
  - [ ] `just lint` + `just test` + `just stress`
- **Follow-up:** Phase-specific tests pass on MemoryStore, FileSystemStore, and SQLiteStore, but PostgreSQL is skipped because Docker is unavailable. The exact `just` wrappers cannot run because `just` and `uv` are absent; `.venv` Ruff and core tests pass. Repository-wide tests remain blocked by the deferred Agni async migration, and full stress remains blocked by the pre-existing notice-unaware `stress/test_kitchen.py` policy; both files are unchanged in P2.

### Phase 3 — Sarathi pilot · deps: P2 · ∥ P4 docs draft · —
- Backend: route mid-turn `user_message` to durable root ingress, keep idle messages as new turns, preserve root-only authorization, and remove in-memory waiting for active-turn user messages.
- UI: enable the composer while active, render durable user messages once, update async subagent/task states, and keep child blocks observational with no direct controls.
- Reconnect: replay root messages and recursive child state, reconstruct unfinished tasks, and avoid duplicate bubbles/notices.
- Keep root recursive cooperative Stop behavior and verify it alongside parent force-kill tools.
- **Verify:** browser/API test launches nested work, sends user guidance while the root waits and while a child runs, observes the root inspect and redirect it, then asks the root to kill one child; refresh during each state preserves one transcript, the same task IDs, bounded concurrency, and no resumed killed task.
- Checklist:
  - [ ] Durable active-turn WebSocket ingress
  - [ ] Running/waiting composer and root-only controls
  - [ ] Async task reducer and nested transcript rendering
  - [ ] Reconnect/replay deduplication
  - [ ] Sarathi backend `just lint` + `just test`
  - [ ] UI `yarn lint` + `yarn build`
  - [ ] Desktop and mobile browser flow
  - [ ] Root `just lint` + `just test`

### Phase 4 — documentation and 0.5 release · deps: P3 · —
- Migrate Agni and Sarathi agent callers and instructions to the async task lifecycle.
- Rewrite `docs/guides/subagents.md`, `docs/concepts/durability.md`, `docs/concepts/turn-loop.md`, `docs/reference/{events,harness,loop,tools}.md`, and `docs/sharp-edges.md` for async task ownership, messaging, inspection, waiting, cancellation versus kill, and recovery.
- Update package/readme examples and all blocking `spawn`/`fan_out` references. Include a 0.4-to-0.5 migration showing launch, wait, inspect, result, notify, message, and kill.
- Bump `packages/tantra/pyproject.toml` and exported version to 0.5.0; update Sarathi's dependency floor and lockfile.
- **Verify:** repository search finds no documented blocking `Context.spawn` result or `Context.fan_out`; strict MkDocs build passes; a clean sync runs the documented async example; all root, stress, Sarathi backend, and UI checks pass.
- Checklist:
  - [ ] Concepts, guides, references, sharp edges, and README
  - [ ] 0.4-to-0.5 migration
  - [ ] Version, dependency floor, and lockfile
  - [ ] Strict docs build and clean documented example
  - [ ] Full root, stress, backend, and UI verification

## Open Decisions
- None. New transport, scheduler, tool-mailbox, or direct-child UI requirements require a new spec rather than expansion during implementation.

## Risks
- **Loop rewrite:** parent sampling, child execution, and event streaming become concurrent. Mitigation: one root-scoped supervisor, one tree cap, deterministic gated tests, and no detached ownership.
- **Cross-session atomicity:** child terminal state and parent notice cannot commit together. Mitigation: deterministic IDs, deduplicated context, and resume-time reconciliation.
- **Kill side effects:** coroutine cancellation cannot undo completed external writes. Mitigation: explicit force-kill naming, save-and-exit through messaging, subprocess cleanup, and documentation for custom tools.
- **Lease duplication:** long provider/tool calls can still exceed lease TTL. Mitigation: waiting heartbeat in this feature; retain the existing boundary limitation for arbitrary long operations and cover duplicate-side-effect risk in sharp edges.
- **Context growth:** durable messages and task notices add same-turn history. Mitigation: actionable-only automatic delivery, bounded text/pages, output-on-demand, and compaction protection only until first delivery.
- **Starvation:** repeated user/task messages can repeatedly skip pending calls. Mitigation: ordered batches consume all pending items before one resample; new arrivals wait for the next boundary.
- **Queued-tree races:** a child may link while kill traverses descendants. Mitigation: killed ancestors reject spawn admission and kill performs a second scan.
- **Breaking consumers:** external custom tools may depend on blocking `spawn` or `fan_out`. Mitigation: 0.5 semver signal and explicit migration, without runtime compatibility code.

## Success criteria
- A parent launches multiple child tasks, continues reasoning, inspects status/events and last messages, receives bounded actionable notices, and retrieves terminal outputs without blocking on launch.
- Parent and child exchange durable text messages; delivery survives process replacement and changes behavior before the next sample's stale calls run.
- A parent force-kills any owned descendant subtree; local coroutines stop immediately, queued descendants never start, remote requests stop at the next boundary, and no killed task resumes.
- A parent with unfinished work waits without consuming model calls and wakes for user input, child notification, completion, failure, or kill.
- Sarathi users can steer a busy root, but cannot directly message or control children; refresh/reconnect preserves task IDs, messages, state, and results.
- `max_concurrency` is enforced across nested descendants before and after recovery.
- Existing logs remain readable, no detached work survives root-stream abandonment, and all documented verification commands pass.

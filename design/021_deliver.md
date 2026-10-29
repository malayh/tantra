# 021 — Deliver and on_cancel (tantra 3.1)

*Requested by antivenom: `~/Code/antivenom/design/20261028_09_tantra_deliver.md` (contract) and `20261028_09_agent_sandboxes.md` (consumer: background sandbox tasks).*

## Goal

- Code outside a turn (a worker job, a webhook, a timer) wakes any agent of a tree with a message.
- A cancel that stops agents tells the host which agents, so the host can stop their work outside tantra.
- Success means all of these work:
  - A worker's runtime delivers to a subagent of a tree that another runtime owns. The subagent starts a turn with that message.
  - The writer of the tree doesn't change.
  - The agent sees `[event <source>] <text>`. ACP shows it as a `session_message` with no sender and `_meta.tantra.source`, never as `user_message`.
  - A retry with the same `command_id` queues the message once.
  - `cancel_agent` on an idle child calls `on_cancel` with the child and its descendants.

## Scope

**In**
- `Runtime.deliver`, on the local and the coordinator path.
- `InputQueued.origin = "event"`.
- `Hook.on_cancel`.
- ACP projection of event inputs. `tantra-acp-client` 2.2.0.
- Docs, changelogs, `tantra-harness` 3.1.0, `tantra-acp-client` 2.2.0.

**Out**
- Delivery to a finished agent.
- Scheduling. Callers schedule with their own job system.
- Subscriptions, unsubscribe or retraction inside tantra.
- A cap on the delivered text.
- A telemetry span for `deliver`.
- A guard against unknown envelopes in the coordinator dispatcher.
- Any antivenom code.

## Why a new API

Every wake-up today queues an `InputQueued`, then `_enqueue` → `_notify` → `_activate`. None of the three callers fits an external event:

| Caller | Limit |
| --- | --- |
| `Connection.send` → `_accept_send` (`runtime.py:2666`) | Root only. Needs a writable connection (`_check_writer`, `2646`). With a coordinator, a second writer takes the writer from the user's tab. Queues `origin="user"`, so ACP shows a `user_message`. |
| Agent `send` tool → `_actor_send` (`runtime.py:1949`) | Only inside a tool call, only across a direct parent–child edge. |
| Child replies → `_deliver_lifecycle_locked` (`runtime.py:1763`) | Internal, child → parent only. |

## Deliver

```python
async def deliver(
    self, agent_id: UUID, input: str, *, command_id: UUID, source: str
) -> CommandReceipt
```

- `source` must match `^[a-z][a-z0-9_]{0,31}$`, otherwise `ValueError`.
- The queued event is `InputQueued(command_id=cid, input=f"[event {source}] {input}", origin="event")`. `sender_id` and `submitted_by` stay `None`.
- The target is any session that exists and isn't finished: root, child or grandchild. The root comes from `_header(agent_id)`: `header.root_id or header.id`.
- Errors: unknown id → `SessionNotFound`. Finished → `TantraError(f"agent {agent_id} is finished")`. Same `command_id` with different content → `InvalidCommandReuse`.
- No `Connection`, no writer token. The writer of the tree never changes.
- The same `command_id`, target and content returns `duplicate=True` and wakes the target again. *Why:* a crash between enqueue and activation leaves a queued input with no running task. `_actor_send` re-activates on duplicates for the same reason.
- Wrapped in `_shielded`, as `_send` is.
- The input queues behind the current turn like any other input. A pending ask blocks it until answered.

### Local path

Under `_lock(root_id)`:
- `_ensure_open()`, `_agent_for(header.agent)`.
- `_tree_command(root_id, cid)`: a hit equal to `(target_id, event)` is a duplicate. Any other hit raises `InvalidCommandReuse`.
- `_actor_finished(target_id)` → error.
- `_enqueue`, then `_notify`, bump `_activations[target_id]`, `_activate(target_id, root_id)`. Duplicates also notify and activate.

### Coordinator path

- Caller: `_request_command(root_id, "deliver", DeliverPayload(...), None)`. With no owner, the caller acquires ownership in `_request_command`, as for every command.
- `coordinator.py`:
  - `DeliverPayload(type="deliver", command_id: UUID, agent_id: UUID, input: str, source: str)`. It carries the raw input. The owner builds the event, so the prefix is built in one place.
  - Add it to `CommandPayload` and `"deliver"` to `CommandOperation`.
  - `CommandEnvelope.validate_operation` (`coordinator.py:119`) treats `deliver` like `claim_writer` and `delete`: no writer token allowed.
- `_handle_request` on the owner:
  - A `DeliverPayload` branch that skips `validate_writer` and the `assert` at `runtime.py:2322`.
  - Add `DeliverPayload` to the condition that loads `headers` (`runtime.py:2285-2294`), next to `SendPayload`, so stores without `lookup_command` or `lookup_finished` get fallback journals.
  - Duplicate check through `find_command`, finished check on the target (`lookup_finished(target)` or `self._finished(journals[target])`), then `store.enqueue(target, event)`.
  - After commit: activate and notify the target. Change `_accepted_input(root_id)` (`runtime.py:2531`) to `_accepted_input(agent_id, root_id)` and update its three callers: the send branch, `_cancel_and_drain` and the local `_accept_cancel`. Duplicates run it too.
- **Surprise:** `CoordinatedStore.enqueue` on a session of another root raises `LeaseLost`, not a clean error (`_check_header`, `coordinator.py:1802`). It can't happen here: the root always comes from the target's own header.

### Root-only cancel

- Root-only cancel keeps queued event inputs, the same as child replies.
- Today it selects the inputs to cancel by `sender_id is None`, in two places: `_root_only` (`runtime.py:303`) and the local `_accept_cancel` (`runtime.py:2818`). Events also have no sender.
- New filter in both: `item.origin == "user" or (item.origin is None and item.sender_id is None)`. The second half covers inputs journaled before `origin` existed.
- Tree cancel removes queued events like every other queued input. No change.

## on_cancel

```python
class Hook:
    async def on_cancel(self, root_id: UUID, agent_ids: list[UUID]) -> None: ...
```

| Cancel | Queued event inputs | `agent_ids` | Runs on |
| --- | --- | --- | --- |
| Tree cancel (`descendants=True`) | Removed | Every agent in the tree, from `_tree_headers(root_id)` | The runtime that called `Connection.cancel`, after the cancel is saved |
| Root-only cancel (`descendants=False`) | Kept | Not called | — |
| `cancel_agent` | Removed for the child and its descendants | The child and its descendants (`actors`, `runtime.py:1991`) | The owner, inside the parent's tool call, after its lock block |

- **The list is computed, not taken from `CancellationRequested.targets`.** Targets list only agents with a live turn (`_operational_targets`, `runtime.py:2519`; the local path reads `self.active`, `runtime.py:2808`). An idle agent waiting on background work is never in it.
- **Finished agents are included.** A finished agent can still own work outside tantra.
- **Root-only cancel doesn't call it.** Root-only keeps the tree's work: children, child replies, queued events. antivenom's Stop is root-only (`AcpServer(cancel_descendants=False)`).
- **Awaited, like every hook.** `cancel` and `cancel_agent` return after the hook returns. Hosts move slow work (process kills, network calls) out of the hook into their own job or task.
- **Never on the owner's request handler.** *Why:* the owner runs one request per tree at a time (`_dispatchers`, `coordinator.py:1148`). A hook there that calls tantra for the same tree, such as `deliver`, waits behind its own request until the 10 s request timeout. A hook error there would also skip `_release_if_idle_locked` (`runtime.py:2460`) and leave the tree owned.
- **Coordinator path:** the owner's `CancelPayload` branch adds `agent_ids` (hex) to the reply, from the `headers` it already loads, for new and duplicate cancels. The caller's `_accept_cancel` reads `result.get("agent_ids", [])` and awaits the hook when it isn't empty. *Why `.get`:* a 3.0.0 owner during a rolling deploy replies without it, and Stop must keep working.
- **Local path:** `_accept_cancel` builds the list under the lock and awaits the hook after the lock block.
- **`_actor_cancel`:** awaits the hook with `actors` after its lock block.
- **Called after the cancel is saved, duplicates included.** A retried cancel re-runs the hook. Hosts must make the hook idempotent.
- **Errors reach the requester,** like every other hook error: `Connection.cancel` raises, `cancel_agent` returns a tool error. The cancel stays saved, and repeating it re-runs the hook.

## ACP projection

- `_classify` (`acp/projection.py:328`): `origin == "event"` → strip `[event <source>] ` and return `SessionMessage(message_id, sender_session_id=None, recipient_session_id=self._id, content=[text], meta={"tantra": {"source": source}})`.
  ```json
  {"sessionUpdate": "session_message", "messageId": "…", "recipientSessionId": "…",
   "content": [{"type": "text", "text": "task 3f2a finished, exit 0\n…"}],
   "_meta": {"tantra": {"source": "sandbox"}}}
  ```
- `_message` takes `sender: str | None` and an optional `source`. `SessionMessage.sender_session_id` is already optional (`acp/wire.py:175`).
- Event inputs never enter the queue list: `_enqueue` skips anything `_classify` handles.
- Hidden agents need no change. `_withheld` keys on the sender, and events have none. A hidden agent's own session isn't served.
- **Until P3 lands,** an event falls through to `_synthetic`: `user_message` on a root, and a message from the parent on a child. No release ships between P1 and P3.
- `tantra-acp-client` 2.2.0: `MessageEntry.source: string | null`, read from `_meta.tantra.source` in `message()` (`src/transcript.ts:250`). `protocol.ts` doesn't change.

## Decisions

- **Name `deliver`.** `_deliver_lifecycle` is private, so there's no clash.
- **New origin `"event"`, not `"message"`.** `"message"` needs a sender on a direct edge (`projection.py:338`). An event has no sender session.
- **The caller owns the text.** tantra adds only the prefix. *Why the prefix carries `source`:* the agent must see the source in its input anyway. It mirrors `[agent <id> replied]`, and ACP parses it back the same way.
- **No store migration.** `origin` lives inside the event JSON. No column, index or constraint reads it.
- **Reuse `session_message`.** `MessageEntry` already allows a null sender and reads `_meta.tantra` flags (`finished`, `reply`). Older clients render a sender-less message instead of dropping an unknown update.
- **`on_cancel` is a `Hook` method, not a `Runtime` argument.** Hosts already pass the same hooks to every runtime, so both the requester and the owner have it.
- **No `tantra.deliver` span.** tantra has no spans for `send`, `answer` or `cancel`. The turn the event starts is traced, and the caller can trace its own call.

## Considered & rejected

- **`Connection.send` with a server-side writer.** Root only, takes the writer from the user's tab, shows as the user's message.
- **A new `event_message` update.** Older clients drop it. tantra's own non-standard updates use the `_tantra_` prefix.
- **Subscriptions, unsubscribe or retract in tantra.** They need journal events and revocation rules, and they don't stop the work. The host's own task record with a conditional state change gives "deliver once or never".
- **Passing `CancellationRequested.targets` to the hook.** It misses idle agents, which is exactly the case that matters.
- **Calling `on_cancel` for root-only cancel.** It would let hosts kill work that root-only cancel keeps.
- **Running `on_cancel` in a background task.** Every other hook is awaited. One exception would surprise hosts, and hosts that need slow work can queue it themselves.
- **Running `on_cancel` on the owner's request handler.** A same-tree call from the hook waits on itself, and a hook error skips the idle release. See on_cancel.
- **`source` as an `InputQueued` field.** It would duplicate the prefix the agent needs.
- **Guarding the dispatcher against unknown envelopes.** It doesn't protect 3.0.0 runtimes, which are the ones that crash.

## Implementation phases

### Phase 1 — `Runtime.deliver` · deps: none · ✅ DONE

- **Deliverables**
  - `events.py`: `"event"` in `InputQueued.origin`.
  - `coordinator.py`: `DeliverPayload`, the operation, the envelope rule.
  - `runtime.py`: `deliver`, the local path, the `_handle_request` branch, `_accepted_input(agent_id, root_id)`, the root-only cancel filter in both places.
  - Tests: `tests/test_deliver.py` (local), coordinator cases in `tests/test_postgres_runtime.py`.
- **Verify**
  - `deliver` to an idle child starts a turn whose input is `[event sandbox] …`. The root's next turn is the child's reply, not the event.
  - An open writable connection on the root stays the writer, on both paths.
  - Coordinator: a second runtime without ownership calls `deliver`; the owner runs the turn.
  - Coordinator: with no owner, the caller becomes the owner and runs the turn.
  - Same `command_id` twice → `duplicate=True`, one queued input. Changed input → `InvalidCommandReuse`.
  - Finished target → `TantraError`. Unknown id → `SessionNotFound`. Bad `source` → `ValueError`.
  - Root-only cancel keeps a queued event on the root. Tree cancel removes it. A legacy queued input with `origin=None` and `sender_id=None` is still cancelled by root-only cancel.
- **Checklist**
  - [x] origin
  - [x] payload and envelope rule
  - [x] local path
  - [x] owner branch and `_accepted_input`
  - [x] root-only filter
  - [x] tests
- **Notes after landing**
  - On the coordinator path a finished target reaches the caller as `RemoteExecutionError`, a `TantraError` subclass: the owner's `tantra_error` code isn't in `_request_command`'s error map. Coordinated `send` behaves the same.
  - `_agent_for` runs only on the local path. On the coordinator path an owner without the agent registered fails at drain, as for `send`.
  - The coordinator duplicate test keeps the root busy so the owner keeps ownership. Otherwise the duplicate re-acquires ownership and recovery starts the queued input by itself, which hides a missing re-activation.
  - `test_postgres_attribution.py::test_takeover_executes_accepted_unstarted_input_with_original_attribution` fails now and then in full runs under load, also on the code before this phase. It passes alone.

### Phase 2 — `Hook.on_cancel` · deps: P1 · ✅ DONE

- **Deliverables**
  - `hooks.py`: `on_cancel`, a no-op by default.
  - `runtime.py`: a helper that awaits `on_cancel` on each of `self.hooks`; the agent lists; the awaited calls in the local and coordinator `_accept_cancel` and in `_actor_cancel`; `agent_ids` in the owner's `CancelPayload` reply.
  - Tests: `tests/test_on_cancel.py` (local), coordinator cases in `tests/test_postgres_runtime.py`.
- **Verify**
  - Tree cancel on a tree with an idle child calls `on_cancel` once with every agent id, on both paths.
  - With two runtimes, the hook runs on the runtime that called `cancel`, not on the owner.
  - `cancel_agent` calls it with the child and its descendants, also when the child is idle.
  - Root-only cancel doesn't call it.
  - A duplicate cancel calls it again.
  - A hook that raises makes `cancel` raise and `cancel_agent` return a tool error. The cancel stays saved, and a retry calls the hook again.
  - A hook that calls `runtime.deliver` on the same tree completes, on both paths.
  - A cancel reply without `agent_ids` doesn't call the hook.
- **Checklist**
  - [x] hook method
  - [x] agent lists
  - [x] call sites and reply field
  - [x] tests
- **Notes after landing**
  - `Connection.cancel` runs inside `_shielded`. If the calling task is cancelled, the hook still runs to the end, and its error is dropped by `_consume`, as for every shielded command. `_actor_cancel` isn't shielded, so a cancelled parent turn also cancels its hook.
  - The two-runtime test keeps the root busy so the owner keeps ownership. Otherwise the duplicate cancel can make the caller the owner. The hook runs on the caller either way.
  - `test_postgres_runtime.py::test_tree_recovery_preserves_cancellation_targets_and_interrupts_only_started` fails now and then in full runs: it asserts ownership under a 0.12 s lease before any cancel. It passes alone.

### Phase 3 — ACP, client, docs, release · deps: P1, P2 · CODE DONE, release pending owner approval

- **Deliverables**
  - `acp/projection.py`: the `"event"` branch, `_message` changes.
  - `tantra-acp-client`: `MessageEntry.source`, a reducer test, a fixture for live and replay.
  - Docs in `website/src/content/docs/docs/`:
    - `reference/runtime.md`: `deliver`.
    - `reference/hooks.md`: `on_cancel`, and a note that every hook is awaited: the turn or the cancel waits for it, so slow work belongs outside the hook. The same note in `guides/permissions-hooks.md`.
    - `reference/events.md`: the `"event"` origin.
    - `reference/acp.md` and `acp/tantra-profile.md`: `_meta.tantra.source`.
    - `guides/tools.md`: a section on long-running tools: a task record, a conditional state change, `deliver`, `on_cancel`.
  - `packages/tantra/CHANGELOG.md` 3.1.0 with the deploy order and the no-downgrade rule. Client README.
  - Versions: `pyproject.toml` 3.1.0, `package.json` 2.2.0. Publish after the owner approves.
- **Verify**
  - Projection test: an event on a child projects as `session_message` with no sender, the child as recipient, `source`, and the text without the prefix. No queue update is emitted.
  - The same on a root.
  - Client test: the reducer sets `source`. Replay gives the same transcript as live.
  - Existing projection and client tests pass unchanged.
- **Checklist**
  - [x] projection
  - [x] client
  - [x] docs
  - [x] changelog, versions
  - [ ] release
- **Notes after landing**
  - The golden `test/fixtures/session.json` now holds an event turn. It is read by the client's `transcript.test.mjs` and by Sarathi's `apps/sarathi/ui/src/app/chat/state.test.ts`, whose reply-flag list needed one more entry. That was the only Sarathi change. `just test` doesn't run the Sarathi test.
  - The main checkout's `apps/sarathi/ui/node_modules/tantra-acp-client` is a stale copy. To run Sarathi's tests against a branch, link that branch's built client.
  - Sarathi labels a message with no sender as "Parent · Received", so an event would show as from the parent. Sarathi never calls `deliver`, so this is left as is.
  - `test_postgres_runtime.py::test_cancel_agent_through_the_coordinator_leaves_no_notice_after_takeover` fails now and then in full runs, also before P2. It passes alone.

### Downstream (outside this spec)

- antivenom: bump both pins in a deploy that doesn't call `deliver` yet. Only the sandbox phase that calls `deliver` ships after every runtime runs 3.1.0.

### Conventions (all phases)

- Follow `AGENTS.md`. Run `just lint` and `just test` before marking a phase done. Client: `npm run lint` and `npm test` in `packages/tantra-acp-client`.
- No code comments.
- **Contract freeze (end of P1):** the `deliver` signature, `origin="event"`, the `[event <source>] ` prefix, the `source` pattern, `DeliverPayload`. **End of P2:** the `on_cancel` signature and the table of when it runs. **End of P3:** `_meta.tantra.source`. Changing any of them means updating this spec and the antivenom specs first.

### Keeping this spec current

- Update the status marker on each heading and tick the checklist as you go.
- Keep the plan matching the current truth. Add one brief deviation note only when behaviour, scope, contracts or phase boundaries materially change.
- After a phase lands, add only the details that would surprise the next reader.
- Put unresolved problems in Open Decisions with enough detail to act on later. Don't fix them outside the requested phase.

## Open Decisions

- **Event inputs and compaction.** Default: they compact like any other input.
- **Text size.** Default: no cap; the caller caps (antivenom keeps a 16 000-char tail). Add a cap like the 32 KiB reply cap if a caller doesn't.

## Risks

- **Mixed versions break 3.0.0 runtimes.**
  - A 3.0.0 owner parses each request with an unguarded `CommandEnvelope.model_validate` (`coordinator.py:1217`). One `"deliver"` request ends its `_dispatch` task, which nothing restarts. That runtime stops handling commands for every tree it owns until it restarts.
  - 3.0.0 also can't read a journal that holds an `"event"` input.
  - Every runtime runs 3.1.0 before any caller uses `deliver`.
- **No downgrade** to 3.0.0 once an event input is journaled.
- **The caller can become the owner.** A tree is usually idle while background work runs, so the caller often takes ownership and runs the woken turn, and every turn after it while it owns the tree. Its runtime must register the same agents, provider, hooks and `deps_factory` as the main one.
- **`on_cancel` can be missed** when the requester stops between the saved cancel and the hook. A retried cancel runs it again. Otherwise the host's work runs on and delivers later.
- **A slow hook slows the cancel.** `cancel` and `cancel_agent` wait for it.
- **Wake-ups cascade.** An event to a grandchild wakes the grandchild, its reply wakes the child, and that reply wakes the root: three turns.
- **Context grows** with large event texts. Compaction never prunes inputs.

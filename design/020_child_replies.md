# 020 — Child Replies (tantra 3.0)

## Goal

Every completed child turn delivers that turn's final assistant message to its direct parent. Remove `finish`. A parent must never wait on a result that a child wrote but did not hand over.

## Evidence

- **Live multi-agent chat run** (`z-ai/glm-5.3-flash`, 2026-10-27):
  - 11 of 11 short calculator children called `finish`.
  - Only 1 of 7 long essay children did. The other 6 wrote their essay, ended the turn, and stayed idle.
  - The lead then told the user that the results would still arrive.
- **Harness survey** (source read at pinned commits, 2026-10-27):
  - Harnesses that require a completion tool all need a nudge loop, and they still lose output:
    - Gemini CLI `complete_task`: one 60 s grace turn, then a fixed failure string. The child's text is lost.
    - Goose `final_output`: "You MUST call the `final_output` tool NOW" on every turn, up to max turns.
    - Roo Code `attempt_completion`: 4 nudges, then it blocks on a human.
  - Async back-and-forth harnesses have no `finish`. Each child turn's final text goes to the parent:
    - Codex `multi_agent_v2`: `FINAL_ANSWER` goes to the parent's mailbox.
    - pi: the durable background-subagent example.
    - opencode: background tasks.
  - Blocking harnesses (opencode `task`, OpenAI Agents SDK `as_tool`, Cline) return the last message with no tool calls.

## Scope

In:

- A reply input on every completed child turn.
- Removing the `finish` tool and its terminal-tool plumbing.
- Children with `output_schema` reply with their `submit_output` JSON.
- Journals written by 2.x load and behave as before.
- ACP projection of replies, and `MessageEntry.reply` in `tantra-acp-client`.
- New tool descriptions and child guidance.
- Sarathi, docs and the stress bench.

Out:

- Nudges and retries.
- Closing or deleting children.
- Partial text on `max_steps` or `failed` (see Open Decisions).
- Correlating replies with requests.
- Parents reading child history.

## Decisions

- **A reply on every completed child turn.**
  - Source text: the turn's last `SampleCompleted`, which is the sample without tool calls. Read it through `_turn_result` (`runtime.py:3332-3369`).
  - Don't use `loop._final_text` (`loop.py:843-853`): it spans the whole history, not the turn.
  - `stop_reason="completed"` → that text. If it's empty, use `(no text reply)`.
  - `stop_reason="output"` → the canonical JSON of `TurnCompleted.output`, using the encoding at `runtime.py:2017`. This also fixes today's loss of a child's `submit_output` result: the parent currently gets status only.
  - *Why:* models write long results as plain text, and repeating them inside a tool argument is the step they skip.
- **Other outcomes are unchanged.** `max_steps`, `failed`, `interrupted`, and `cancelled` (when the parent didn't cancel) keep today's status-only notice. A cancel by the parent stays silent.
- **Format:** `[agent <child-uuid> replied] <text>`.
- **Cap:** 32 KiB of UTF-8.
  - Encode, cut at the last complete code point at or below 32 768 bytes, and append `\n[reply truncated at 32 KiB]`.
  - *Why:* duplicate detection compares full content (`memory.py:187-193`, `postgres.py:948`, `coordinator.py:1580`), so the cut must be byte-deterministic.
- **Origin stays `turn_ended`, and the command id stays `uuid5(child, turn_id.hex)`** (`runtime.py:270`).
  - *Why:* there's no new event literal, so 2.x readers don't raise `CorruptLog` (`events.py:51`, `postgres.py:1688-1697`).
  - With the same id, each turn is still delivered once.
- **Delivery: build, enqueue, and treat `InvalidCommandReuse` on that id as delivered.**
  - *Why, upgrade:* turns that 2.x already delivered carry status-only text under the same id. The first drain after the upgrade builds a reply for them, the store rejects it as a reuse, and nothing is delivered twice.
  - *Why, cost:* reconcile runs on every drain for every child (`runtime.py:1759-1768`, `2085`). `Runtime._delivered` keeps the last command id delivered per child, so the reply text is built and the journal read at most once per child turn per process. Postgres reads through the indexed `read_turn` (`postgres.py:1360-1376`); the other stores scan.
  - The cache holds ids, not text, so it stays small. A miss only costs one rebuild that the store dedupes.
- **`finish` is removed.**
  - The tool (`runtime.py:1821-1825`).
  - `_actor_finish` (`2004-2067`).
  - The `finish` permission check (`338-353`). The name stays reserved for child agents: legacy journals project a child's `finish` call as a finished message, so an app tool with that name would be mistaken for one.
  - `terminal_tool="finish"` (`2163`, `loop.py:455-477`, `523-532`).
  - `FinishResult` (`loop.py:117-120`, `676-677`, `722-724`).
  - The code that produces `TurnCompleted(stop_reason="finished")` (`loop.py:821-829`).
- **Legacy data stays readable and is never written.**
  - These stay in the code:
    - `AgentFinished` in the event union.
    - The `finished` origin and status literals (`events.py:51`, `244`).
    - The `_actor_finished` checks (`runtime.py:1128-1133`).
    - The `finished` skip in `_lifecycle_input` (`255`).
  - A child finished under 2.x stays closed, and its `[agent X finished]` input projects as before.
  - No store migration.
- **tantra never closes a child.** An idle child costs nothing. The parent stops work with `cancel_agent`.
- **No descendant-completion guarantee.** A mid-level child may reply while its own children still work, and it replies again when they wake it. `tree_status` stays the authoritative tree view.
- **Child guidance is keyed on `parent_id`.**
  - Today it's keyed on `terminal_tool == "finish"` (`loop.py:768-800`, `context.py:41-45`), so removing `finish` would silently drop it.
  - The new text: "When your turn ends, your parent receives your final message as your reply, or your submitted output if you submit one. Put the complete result there. Use send only for messages during a turn. Your parent may send you more work."
- **New framework tool text:**
  - `spawn`: "Create a declared child agent, queue its input, and return its ID. Each time the child's turn ends, a new input arrives: [agent <id> replied] with its final message, or [agent <id> turn ended] with its status if it failed or stopped. Do not poll for it. Available agent types: …"
  - `status`: "Return durable status for one direct child agent. Replies arrive as new input; use this only for a child that hasn't replied."
  - `cancel_agent`: "Cancel the active and queued work of one direct child agent and its descendants. No reply follows. The child stays reusable through send."
  - `send` is unchanged.
- **ACP projection** (`acp/projection.py:306-375`):
  - Origin `turn_ended` with the `replied` prefix projects as a child → parent `session_message` with `_meta.tantra.reply: true`.
  - Status-only notices still project nothing.
  - The profile version stays 1 (`acp/agent.py:392-397`). *Why:* the change is additive, and an old client renders a reply as a plain message.
- **Semver:** 3.0.0. A model-facing tool is removed, parents now receive child text, and new sessions never produce `_tantra_finished`. The docs' promise of "no child assistant text" is reversed (`website/…/reference/events.md:19`).

## Considered and rejected

- **`finish()` with no arguments that attaches the last message:** cheap to call, but a model can still forget it, and then the parent waits forever.
- **Nudge loops**, as in Gemini CLI, Goose and Roo: they cost turns and still lose output.
- **The parent reads the child's history:** the reply already carries the one message the parent needs.
- **A new `replied` origin:** 2.x readers would raise `CorruptLog` during a rolling deploy.
- **A new command-id namespace:** after the upgrade, it would deliver every child's latest turn a second time.
- **007's objection, "automatic reply forwarding creates unbounded ping-pong or needs request/reply correlation state":**
  - Replies only flow upward and stop at the root. Every downward hop is an explicit `send`.
  - 2.x already wakes the parent on every child turn end, so the number of wake-ups doesn't change; only the payload grows.
  - No correlation state is needed, because replies are keyed by child and turn.
- **008's objection, "auto-finish breaks reusable children":** nothing is closed, so children stay reusable.

## What breaks

| Consumer | Breaks | Changes |
| --- | --- | --- |
| tantra core | `finish` tool and `FinishResult`; about 54 finish/lifecycle tests; finished assertions in the store conformance suite (`testing.py:375-390`, `649-657`) | The conformance assertions become legacy-only |
| `tantra-acp-client` | Nothing | Additive `MessageEntry.reply` (`transcript.ts:46-53`, `248-255`); regenerate `test/fixtures/session.json` |
| Sarathi | Backend `agent.py:45-48`; `test_subagent.py:22-31`, `71-83` (asserts the spawn text); e2e `provider.py`, `acp-client.mjs:507-524` | UI `state.ts:86`, `96` and `transcript.tsx:183`: "Idle — awaiting parent" becomes "Replied" |
| Docs | Nothing | `README.md:67`; website `guides/subagents.md:13-27`, `reference/events.md:12-21`, `concepts/durability.md:29`, `reference/acp.md:100-122`, `acp/tantra-profile.md`, `tools.md:9`, `context.md:9` |
| Stress bench | `bench/behavior.py:51-97`, `381`, `529-736`; `bench/faults.py:72`, `282-346`; `test_tree.py:125-317`; `test_kitchen.py:594-608` | — |
| Apps with subagents | Scripted test providers that call `finish` | UIs that label `_tantra_finished` as finished should show idle children by their latest reply |
| osuite | Nothing: no agent has subagents (`agents/investigator.py:142`, `agents/dashboard.py:75`), and the local DB has 0 child sessions | Bump both pins; the projection cache key includes the harness version, so cached transcripts drop once |
| knowmyleads | Nothing: pinned to 1.0.0 | — |

- **Privacy:** a view that redacts child text now sees it in the parent's input.
- **Rolling deploy:** nothing in the stored schema changes. In a mixed fleet, 2.x hides replies in views, and 2.x children still have `finish`. Stop 2.x runtimes before starting 3.0, as with 1.4 → 2.0.

## Implementation phases

### Phase 1 — Core replies, finish removed, ACP projection · deps: none · ✅ DONE

- **Deviation (owner approved, 2026-10-27):** the ACP projection, the client's `MessageEntry.reply` and the regenerated fixture moved here from P2. The ACP tests used `finish` for child results, so splitting them across phases would rewrite them twice.
- **Deliverables**
  - `runtime.py`:
    - `_lifecycle_input` building replies through `_turn_result`
    - delivery that treats a reused id as delivered, with the per-child cache
    - the cap
    - the `output` stop-reason JSON
    - removing `finish`, while keeping the legacy readers
  - `loop.py`: remove the terminal-tool and `FinishResult` paths.
  - `context.py`: child guidance keyed on `parent_id`.
  - New tool descriptions.
  - The projection's reply meta, `MessageEntry.reply` in the client, and the regenerated fixture.
  - Tests adapted and added.
- **Verify**
  - A child that ends with text gives the parent exactly one `[agent X replied] <text>` input, including across a restart and across the existing enqueue-failure reconciliation tests.
  - A journal written by 2.2.0 loads with no `InvalidCommandReuse` and no second delivery. It holds a status-only notice, a finished child, and a `[agent X finished]` input. The finished child still rejects `send`.
  - A 40 KB reply truncates to the same bytes on two deliveries, and the duplicate is dropped.
  - An `output_schema` child delivers canonical JSON.
  - A warm drain reads no journal (`test_postgres_command_reads.py`).
  - A reply projects as a child → parent `session_message` with `reply: true`.
  - A legacy finished child projects unchanged (projection tests); the regenerated fixture shows replies.
  - The client reducer test passes.
  - The `justfile` checks pass.
- **Checklist**
  - [x] reply builder, cap, delivery
  - [x] `finish` removed, legacy readers kept
  - [x] guidance and tool text
  - [x] output_schema reply
  - [x] legacy fixture journal test
  - [x] projection, client, fixture
  - [x] tests and `justfile` checks
- **Build notes**
  - `just test`: 1265 pass; the 4 pgvector memory tests in `test_postgres_store.py` fail on the local Postgres before and after this change.
  - A queued `send` to a child that used to finish now runs as another child turn. `test_a_hidden_child_leaks_no_lifecycle_input_and_cuts_still_hold` gained that turn.
  - The `_REPLIED` prefix in `projection._synthetic` only keeps origin-less classification equal to the structured path. 3.0 always writes an origin on replies.

### Phase 2 — Sarathi, docs, stress · deps: P1 · —

- **Deliverables**
  - Sarathi backend, UI and e2e.
  - Website docs and README.
  - The stress bench.
  - The changelog. The owner cuts the 3.0.0 release.
- **Verify**
  - The Sarathi e2e passes.
  - A live run with `z-ai/glm-5.3-flash`: 7 essay children all deliver their text to the lead, and the lead never polls `status`.
- **Checklist**
  - [ ] Sarathi
  - [ ] docs, stress bench, changelog
  - [ ] live check

### Downstream (outside this spec)

- osuite: bump the pins, run `just test`, and stop 2.x runtimes before deploying.

### Conventions (all phases)

- Follow `AGENTS.md` and run the `justfile` checks before marking a phase done.
- No code comments.
- **Contract freeze (P1):** the reply format, the cap, the origin, the command id and the legacy behaviour. Changing any of them means updating this spec first.

### Keeping this spec current

- Update the status marker on each heading and tick the checklist as you go.
- Keep the plan matching the current truth. Add one brief deviation note only when behaviour, scope, contracts or phase boundaries materially change.
- After a phase lands, add only the details that would surprise the next reader.
- Put unresolved problems in Open Decisions with enough detail to act on later.

## Open Decisions

- **Partial text on `max_steps` and `failed`.** Default: status only, as today.
- **Cap size.** Default: 32 KiB for all agents. A per-agent override can come later.
- **A child that calls `send` and then ends with text** delivers twice. Default: accept it; the guidance says `send` is for messages during a turn.
- **Interim replies from mid-level actors reach their parent.** Default: accept them.

## Risks

- **Parent context grows.** 12 replies at the cap add up to 384 KiB in one lead turn. Compaction never prunes inputs, and the current input is irreducible (`compaction.py:213-216`, `251-287`).
- **A narrated last line becomes the reply,** for example "Done, sending it now." The guidance asks for the complete result in the final message.
- **Wake-ups stay as they are, but each one now carries text,** so a lead turn costs more tokens.
- **Process stops between the child terminal and parent delivery:** convergence relies on the existing reconciliation, which now does one journal read per undelivered turn.

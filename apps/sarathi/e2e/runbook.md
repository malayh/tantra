# Sarathi E2E Runbook

Manual-but-agent-driven end-to-end pass over the compose stack. Executed by Claude Code with browser tools + shell access. Produces one report per run.

## Prerequisites

- From `apps/sarathi/`: `cp .env.example .env` and fill:
  - `OPENAI_BASE_URL` / `OPENAI_API_KEY` — OpenAI-compatible endpoint.
  - `SARATHI_MODELS` — csv, **at least two** real cheap models (first = default). Scenario 9 needs two.
  - `BRAVE_API_KEY` — required; without it `web_search` is silently dropped and scenarios 3/5/8 cannot pass.
  - `SECRET_KEY`, `NEXTAUTH_SECRET` — any non-empty strings.
- `docker compose up -d --build` from `apps/sarathi/`. After editing `.env`, recreate with `docker compose up -d backend` — `docker compose restart` keeps the old env.
- `docker compose ps` → `db` healthy, `backend` healthy, `ui` healthy, `migrate` exited 0.
- UI: <http://localhost:3000> · API: <http://localhost:8000> (`GET /api/health` → ok).
- Thoughts (scenario 2) only appear if the endpoint emits reasoning deltas. Not a bug in the app — SKIP with the model named.

## Reset

- `docker compose down -v` — drops the db and uploads volumes; all users, sessions, memories, files gone.
- Run the scenarios in order on a **fresh** stack. They share state (accounts, sessions, memories) by design.

## Instructions to the executing agent

- Drive the browser with whatever browser tools this session has. Take a screenshot at each expected observation — screenshots are the evidence.
- **Judge behaviourally, never by exact text.** The LLM output is nondeterministic. "Cited answer" means links/sources are present and on-topic, not a specific sentence. "Grounded" means the distinctive facts from the fixture show up, not a specific phrasing.
- Shell is allowed and needed: `docker compose restart backend` (scenario 6), `docker compose logs backend` when diagnosing a FAIL.
- **Flaky rule:** a failing scenario gets **exactly one** re-run. Still failing → FAIL. Record both attempts in the notes.
- A scenario that cannot run (missing key, endpoint lacks a capability) is SKIP with the reason, not FAIL.
- Do not fix code mid-run. Record the defect; the run reports what the stack does today.
- Write the report to `apps/sarathi/e2e/reports/<YYYY-MM-DD>.md` from the template at the bottom of this file. `reports/` is gitignored.

## UI reference (for locating things)

- Sidebar: `New chat`, session list (title + relative time), footer = email · brain icon (Memory) · sun/moon (`Toggle theme`) · log out.
- Header: session title · model `Select` (`aria-label="Model"`) · connection dot (green = WS open).
- Composer: paperclip (`Attach file`, accepts `.pdf`/`.docx`) · textarea (Enter sends, Shift+Enter newline) · `Send` / `Stop` button (swaps while a turn runs).
- Transcript items: `Thinking` collapsible (open while streaming, auto-collapses when the sample finishes) · markdown text with a streaming cursor · tool chips (`web_search`, `web_fetch`, `read_doc`, `memory_write`, `memory_recall`, spinner while in flight, click to expand the result) · subagent block badged **`researcher`** (lowercase — the badge is the agent name, not the class name; bot icon, nested items inside) · approval card titled `Run memory_write?` with `Approve` / `Deny`.

---

## Scenario 1 — Auth and route guard

**Steps**

1. Open <http://localhost:3000/chat> in a clean browser profile (logged out).
2. From `/login`, follow `Sign up`. Register user **A** (`a-<runid>@example.com` / password **≥ 8 chars** — the form enforces `minLength=8`).
3. Confirm the app lands on `/chat`.
4. Log out (footer log-out icon), then log back in as A from `/login`.
5. Log out again, then re-enter <http://localhost:3000/chat> directly.

**Expect**

- Step 1 redirects to `/login?callbackUrl=...` — no chat shell flashes.
- Step 2 lands on `/chat` ("Start a conversation" empty state, sidebar "No sessions yet").
- Step 4 returns to `/chat` with the sidebar showing A's email in the footer.
- Step 5 redirects to `/login` again.

---

## Scenario 2 — Basic chat, streaming, thoughts, title

**Steps**

1. As user A on `/chat`, send: `Explain what a write-ahead log is and why databases use one.`
2. Watch the transcript from send to completion without touching anything.
3. When the turn finishes, look at the sidebar entry for this session — **do not reload**.

**Expect**

- URL becomes `/chat/<id>` where `<id>` is 32 hex chars, no dashes (`uuid4().hex`); the user bubble renders on the right.
- A `Thinking…` shimmer (or an open, pulsing `Thinking` block) appears before any answer text.
- Answer text streams in visibly — partial text with a blinking cursor, growing over time, not one paste at the end.
- If the endpoint emits reasoning: a `Thinking` block is present and **collapsed** once the sample completes; expanding it shows reasoning text. If no reasoning ever appears → **SKIP this sub-check**, note the model id from `SARATHI_MODELS`.
- Composer is disabled while the turn runs and re-enables when it completes.
- Within a few seconds of turn completion the sidebar row changes from `New chat` to a real generated title relevant to the question — **with no page reload**. The header title updates too.

---

## Scenario 3 — Web search

**Steps**

1. `New chat`. Send a question that cannot be answered from parametric memory, e.g. `Search the web: what happened in the news this week about the European Space Agency? Cite your sources.`
2. Watch the tool chips as they appear.
3. Expand one completed chip.

**Expect**

- A `web_search` chip appears with a spinner and an argument summary (the query), then the spinner clears.
- Usually one or more `web_fetch` chips follow (model's choice — not required to pass; `web_search` alone with a cited answer is a PASS).
- Expanding a completed chip shows a JSON result block with real result data.
- The final answer references sources (URLs / named outlets) that match the chips' arguments — not invented links.
- If `BRAVE_API_KEY` is empty, no `web_search` chip can exist → SKIP with that reason.

---

## Scenario 4 — PDF attachment and grounding

The fixture `e2e/fixtures/sample.pdf` is a 3-page **fictional** memo. Nothing in it is guessable — the model must actually read it.

Per-page distinctive facts:

| Page | Fact |
|---|---|
| 1 | The Vantroxil Relay Mesh sustained **41,879 handshakes per second** at the **Brindlecourt** test range (trial `BRINDLECOURT-III`, lead engineer Dr. Isolde Vranck). |
| 2 | Drift traced to the **Quellmarsh oscillator**, which loses **0.734 microseconds** per thermal cycle above **62 degrees Celsius**; remedy is the **Thorne-Ladbroke TL-88** (0.011 µs, 812 credits/node). |
| 3 | Deployment blocked until **Ospreydown** ground station clears audit ticket **KHS-99427**, budget **3.2 million credits**, decision date 02 September 2031. |

**Steps**

1. `New chat`. Attach the fixture — **do not click the paperclip**: it opens a native OS file chooser that browser tools cannot drive. Set the file directly on the hidden input `input[type=file][accept=".pdf,.docx"]` with your file-upload tool (or your chooser-interception path), using the absolute path `/home/malay/Code/tantra/apps/sarathi/e2e/fixtures/sample.pdf`.
2. Send with the text: `Read the attached memo. What caused the calibration drift, how much time does it lose per thermal cycle and above what temperature? Also: what is blocking deployment, and what is the budget on that ticket?`
3. Watch the chips, then read the answer.

**Expect**

- Before send: a file chip with the filename appears above the textarea (removable via its X).
- After send: the chip renders inside the user bubble; the raw `[attachment: … path=…]` marker is **not** visible in the bubble.
- A `read_doc` chip appears with the upload path as its argument summary, spins, then completes without an error style.
- The answer names the **Quellmarsh oscillator**, **0.734 microseconds** (or 0.734 µs), and **62 °C** — page 2 — plus **KHS-99427** / **Ospreydown** and **3.2 million credits** — page 3. Numbers may be reworded but must match.
- FAIL if the answer is generic, hedges that it cannot read the file, or invents different names/numbers.
- Optional extra: ask a follow-up `What was the peak handshake rate and where was it measured?` → expect **41,879** and **Brindlecourt** (page 1, from history without a second `read_doc`).

---

## Scenario 5 — Subagent

**Steps**

1. `New chat`. Send: `Research the current state of WebAssembly component model adoption in depth — use the researcher subagent, then synthesise what it finds.`
2. Watch the transcript while it runs. Do not expand anything until the block stops spinning.
3. Expand the `researcher` block.

**Expect**

- A block badged `researcher` (lowercase, bot icon) appears with a spinner, showing the delegated task as its summary. It renders **in place of** the delegate tool chip — there must not be both.
- While the subagent runs the block is open and shows **nested** activity inside its left rule: `web_search` / `web_fetch` chips and the subagent's own text.
- When the subagent finishes, its spinner clears and the block collapses.
- After the block completes, the root agent streams a **synthesis** answer below it (text outside the block).
- The composer stays disabled until the *root* turn completes (a subagent turn emits two `turn_completed`s; the composer must not unlock early).

---

## Scenario 6 — Memory HITL, durability, scoping

**Steps**

1. `New chat` as user A. Send: `Remember that I prefer my code reviews to focus on failure modes before style.`
2. Wait for the approval card. **Do not answer it.**
3. From `apps/sarathi/`: `docker compose restart backend`. Wait for `docker compose ps` to show `backend` healthy again.
4. Reload the browser page (F5).
5. On the reloaded page, click `Approve`.
6. When the turn completes, open the memory panel (brain icon in the sidebar footer).
7. `New chat` (same user A). Send: `What do I prefer in code reviews? Check what you remember.`
8. Log out. Sign up user **B** (`b-<runid>@example.com`). Open B's memory panel, then send B: `What do I prefer in code reviews?`

**Expect**

- Step 2: a card titled `Run memory_write?` with the tool arguments in a body block and `Approve` / `Deny` buttons; the composer is locked; the `Stop` button is **not** offered while an ask is pending.
- Step 3–4: after the restart + reload the page reconnects (connection dot green), the transcript replays the whole turn so far, and **the approval card re-renders as pending** — exactly one card, not two. This is the durable-ask proof; FAIL if the card is gone, duplicated, or already shows Approved/Denied.
- Step 5: the card flips to `Approved`, a `memory_write` chip completes, and the turn finishes with a confirming answer.
- Step 6: the panel lists a row whose title/body carries the failure-modes preference (kind label above it). Not "No memories yet".
- Step 7: in a **fresh session**, the answer states the preference; usually a `memory_recall` chip is visible (chip optional, correct recall is not).
- Step 8: B's memory panel says `No memories yet`, and B's answer does **not** know A's preference (a `memory_recall` chip returning nothing is fine). FAIL on any leak across accounts.
- Log back in as A afterwards — the remaining scenarios run as A.

---

## Scenario 7 — Deny path

**Steps**

1. As user A, `New chat`. Send: `Remember that my favourite deployment window is Tuesday at 3pm.`
2. On the approval card, click `Deny`.
3. Wait for the turn to complete, then open the memory panel.

**Expect**

- The card flips to `Denied`; the composer unlocks; the turn **completes** (no error banner, no hang) and the model acknowledges it did not save.
- The `memory_write` chip, if expanded, shows a denial/error result rather than a memory id.
- The memory panel is **unchanged** from scenario 6 — the Tuesday-3pm fact is absent.

---

## Scenario 8 — Cancel

**Steps**

1. `New chat`. Send a prompt that fans out: `Research in depth how three different Rust async runtimes compare on scheduler design — delegate to the researcher and be thorough.`
2. Wait until the `researcher` block is visibly spinning **with at least one tool chip inside it**.
3. Click `Stop`. Start a timer.
4. Wait up to ~30s.

**Expect**

- Cancel takes effect at the **next store boundary** — the in-flight LLM sample or tool call finishes first. Latency of a few seconds up to ~30s is expected and is a PASS; instant is not required.
- The whole session tree ends cancelled — the signal is the **root turn showing `Stopped.`** (the subagent block's spinner clears on any root turn end, so it proves nothing by itself).
- The composer returns to idle: `Stop` reverts to `Send`, the textarea is editable, and a new message can be sent in the same session afterwards (send `hi` to confirm).
- FAIL if the turn keeps streaming past ~30s, ends `failed` with an error banner, or the composer stays locked.

---

## Scenario 9 — Model picker

**Steps**

1. Open a session and note the model shown in the header dropdown.
2. Send a deliberately long-running prompt (e.g. `Write a 1000-word essay on the history of the relational database.`) — a short one can finish in 1–2s on a cheap model and leave nothing to observe. While it is still streaming, try to open the dropdown.
3. When idle, switch to the second model in `SARATHI_MODELS`.
4. Send: `In one short sentence, what model are you?` and watch the turn.
5. Optional stronger evidence: `docker compose logs --tail=200 backend` and look for the new model id in the request path, or watch the network frames for `sample_started` carrying the model.

**Expect**

- Step 2: the dropdown is **disabled** mid-turn (does not open / cannot change).
- Step 3: the selection persists in the header after the PATCH; switching back and forth works.
- Step 4: the next turn runs and completes on the new model. Behavioural evidence is acceptable — a self-identification consistent with the new model, or a clearly different response style/latency. A model that misidentifies itself is not a FAIL on its own; prefer the step-5 evidence when it's ambiguous.
- FAIL only if the selection does not stick, the PATCH errors, or the turn still demonstrably runs on the old model.

---

## Scenario 10 — Reconnect mid-turn + theme persistence

**Steps**

1. Set the theme with the sun/moon toggle to the **opposite** of the current one. Note which.
2. `New chat`. Send a prompt that does a tool call **before** a long generation: `Search the web for how Postgres MVCC works, then write a detailed 800-word explanation with examples.`
3. Wait until the `web_search` chip has **completed** and answer text is streaming, then reload the page (F5).
4. Watch without interacting.

**Expect**

- After reload the connection dot goes green and the transcript **replays the persisted turn**: the user message plus every **completed** item — the finished `web_search` chip and any finalised text/thinking blocks — in order, no duplicates.
- **Deltas are not persisted.** The sample that was interrupted mid-stream left only its start in the log, so the partial text on screen before the reload does **not** come back, and resume issues a **brand-new sample** — the answer regenerates from scratch. That is correct behaviour, not a defect.
- **Pass criterion:** the turn resumes on the new socket and runs to completion with a coherent, complete final answer, and the composer unlocks at the end. Do **not** assert the post-reload text matches what was on screen before it.
- The theme chosen in step 1 survives the reload (no flash back to the default dark).
- Re-reload after completion: the finished transcript replays identically and no turn restarts.

**Note — busy toast on reload:** the new connection can race the old one's 60s lease and surface `Another turn is running — retry in Ns`. If that happens, wait out the stated retry and reload once more. This does **not** consume the flaky re-run.

---

## Report template

Copy into `e2e/reports/<YYYY-MM-DD>.md` and fill.

````markdown
# Sarathi E2E Report — <YYYY-MM-DD>

- **Commit:** `<git rev-parse --short HEAD>`
- **Stack:** `docker compose ps` — db / migrate / backend / ui status
- **Endpoint:** `<OPENAI_BASE_URL host>`
- **Models:** `<SARATHI_MODELS>` (default: `<first>`)
- **Embedder:** `<EMBEDDING_MODEL or "none — keyword recall">`
- **Brave key present:** yes/no
- **Browser:** `<tool/browser>`
- **Run duration:** `<mm:ss>`

## Summary

| # | Scenario | Result | Re-run? |
|---|---|---|---|
| 1 | Auth + route guard | PASS/FAIL/SKIP | no |
| 2 | Basic chat, streaming, thoughts, title | | |
| 3 | Web search | | |
| 4 | PDF grounding | | |
| 5 | Subagent | | |
| 6 | Memory HITL + durability + scoping | | |
| 7 | Deny path | | |
| 8 | Cancel | | |
| 9 | Model picker | | |
| 10 | Reconnect + theme | | |

**Totals:** x PASS · y FAIL · z SKIP

## Details

### 1 — Auth + route guard · PASS/FAIL/SKIP
- **Evidence:** <screenshots, URLs observed, log lines>
- **Notes:** <deviations, timings, anything surprising>

### 2 — Basic chat, streaming, thoughts, title · PASS/FAIL/SKIP
- **Evidence:**
- **Thoughts sub-check:** PASS / SKIP (endpoint emitted no reasoning deltas — model `<id>`)
- **Title observed:** `<title>` (no reload: yes/no)
- **Notes:**

### 3 — Web search · PASS/FAIL/SKIP
- **Evidence:** chips seen (`web_search` xN, `web_fetch` xN), sources cited
- **Notes:**

### 4 — PDF grounding · PASS/FAIL/SKIP
- **Evidence:** `read_doc` chip seen; facts recovered: Quellmarsh / 0.734 µs / 62 °C / KHS-99427 / Ospreydown / 3.2M
- **Notes:**

### 5 — Subagent · PASS/FAIL/SKIP
- **Evidence:** `researcher` block, nested chips, synthesis after
- **Notes:**

### 6 — Memory HITL + durability + scoping · PASS/FAIL/SKIP
- **Evidence:** card pending → `docker compose restart backend` → reload → card re-rendered (1 card) → Approve → panel row → fresh-session recall → user B isolated
- **Notes:**

### 7 — Deny path · PASS/FAIL/SKIP
- **Evidence:**
- **Notes:**

### 8 — Cancel · PASS/FAIL/SKIP
- **Evidence:** time from Stop to idle: `<s>`; child + root both cancelled
- **Notes:**

### 9 — Model picker · PASS/FAIL/SKIP
- **Evidence:** picker disabled mid-turn; switched `<a>` → `<b>`; next-turn evidence
- **Notes:**

### 10 — Reconnect + theme · PASS/FAIL/SKIP
- **Evidence:** completed items replayed, turn re-sampled and completed coherently, composer unlocked, theme survived
- **Busy toast on reload:** yes/no (waited out + reloaded once — not a flaky re-run)
- **Notes:**

## Deviations from spec

Anything the stack did that `design/003_webapp.md` does not describe, or describes differently. One bullet each: what was expected, what happened, where (`file:line` if known).

- <none / …>

## Follow-ups

- <bugs to file, flakiness to watch, runbook wording to tighten>
````

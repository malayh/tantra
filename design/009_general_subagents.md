# 009 — General-purpose Subagents and Research Skill

## Goal

Replace Sarathi's capability-specific `Researcher` actor with one general-purpose `Subagent`. Skills provide task specialization, beginning with an on-demand `research` skill supporting `shallow`, `normal`, and `deep` intensity.

## Scope

In:

- Durable optional child display names.
- One general-purpose Sarathi subagent type.
- A packaged research skill available to the root and subagent.
- Research intensity carried in the task rather than the generic skill API.
- Sarathi API and UI support for effective child names.
- Tantra 1.1.0 documentation and release preparation shared with 008.

Out:

- Dynamic actor definitions assembled from skills.
- Recursive general-purpose delegation.
- Child-to-human asks or parent-routed approvals.
- A parameterized generic skill loader.
- Database migrations for existing headers or journals.
- Resuming or sending new work to legacy `researcher` actors.
- Publishing or tagging Tantra 1.1.0.

## Decisions

- `Sarathi` may spawn one-level `Subagent` actors. `Subagent.subagents` is empty.
- Sarathi works inline by default. It spawns only when the user requests a subagent or independent or parallel work materially helps.
- Research does not itself require delegation. Sarathi and Subagent may both load `research`.
- The generic tool remains `skill(name)`. The task names the research level; omitted level means `normal`.
- Subagents receive every non-interactive application capability: web search when configured, web fetch, document reading, memory recall, and skill loading. Their applicable framework tools are parent messaging and finish.
- Approval-gated capabilities such as `memory_write` are unavailable to subagents. Human asks remain root-only.
- Optional subagent names are durable display labels only. They do not change the registered actor type, prompt, model, permissions, or restart lookup.
- Actor type remains `agent="subagent"`; UI and status surfaces use the effective display name.
- Existing `researcher` journals and status remain readable. After deployment they cannot resume or accept new work.
- This feature ships with the already-pending child-lifecycle work in Tantra 1.1.0.

## Named child contract

Extend the framework tool:

```python
spawn(
    agent_name: str,
    input: str,
    name: str | None = None,
) -> str
```

- `agent_name` remains the registered durable actor type.
- Runtime trims `name`; an empty result is treated as omitted.
- The child UUID and input command UUID remain derived from the parent actor, turn, call, and operation. Naming does not change identity.
- An identical repeated spawn, including its normalized name, deduplicates.
- Reuse of the deterministic child identity with a different normalized name raises `InvalidCommandReuse`.
- Spawn still returns the canonical child UUID string.

Add compatible fields:

- `SessionHeader.name: str | None = None`
- `SessionCreated.name: str | None = None`
- `ChildCreated.name: str | None = None`
- `ActorStatus.name: str`

`ActorStatus.name` resolves to `SessionHeader.name` when present and otherwise to `SessionHeader.agent`. Existing headers and events therefore retain their actor-type labels without backfill. The JSON-backed header and event storage needs no schema migration.

The Sarathi actors endpoint exposes the effective `name` alongside `agent`. Generated client models and UI state preserve both: `agent` is the reconstruction type and `name` is presentation.

## General-purpose subagent

Sarathi registers exactly two actor classes:

- `Sarathi`, the root actor, declares `[Subagent]`.
- `Subagent`, registered as `subagent`, declares no children.

Both declare `skills = ["research"]`. Sarathi's Runtime receives a `FileSystemSkills` catalogue rooted at the packaged Sarathi skill directory.

Subagent receives web search when configured, web fetch, `read_doc`, and `memory_recall`. Runtime adds `skill`, `send`, and `finish`. It does not receive `memory_write`, any other approval-gated application tool, `spawn`, or `status`.

Sarathi's prompt instructs it to:

- work inline unless delegation is explicitly requested or independently useful;
- preserve a user-specified research level when working inline or delegating;
- include the level in the delegated task and omit it only when `normal` is intended;
- optionally give a child a short descriptive display name;
- synthesize explicit `finish()` results for the user.

Subagent's prompt instructs it to:

- execute the assigned task using ordinary tools and on-demand skills;
- never attempt a human ask;
- load `research` when the task requires current, sourced investigation;
- use `finish(result)` exactly once to return findings or a clear blocked or incomplete result to its parent.

Plain terminal lifecycle messages remain status-only. They do not substitute for `finish(result)`.

## Research skill

Add a packaged `research/SKILL.md` with one catalogue name, `research`. Its index description tells agents to load it for current, sourced, or comparative investigation.

The task selects one intensity:

| Level | Effort target |
|---|---|
| `shallow` | One focused search pass and roughly 2–3 strong sources. |
| `normal` | Multiple query angles, roughly 4–6 sources, and cross-checking important claims. |
| `deep` | Decompose the question, run follow-up searches, prefer primary sources, target 8 or more useful sources when available, and resolve contradictions or report gaps. |

The targets guide effort rather than enforce quotas. Every level:

- fetches only user-provided URLs or URLs discovered through search;
- cites the URLs actually consulted;
- distinguishes confirmed findings from inference or uncertainty;
- stops when further searching has low expected value or the existing turn limit is reached.

## Sarathi presentation and compatibility

- Running-strip, inline child button, drawer title, and descendant selectors display `ActorStatus.name`.
- Existing actor placement, lazy journal subscriptions, retained cursors, and status polling remain unchanged.
- Old events without `name` display their actor type.
- Legacy `researcher` actors remain discoverable through stored headers and journals because status and event reads do not require registry activation.
- Any attempt to reactivate a legacy `researcher` fails through the existing unknown-agent path. No compatibility alias or header rewrite is added.

## Considered and rejected

- **Dynamic actor definitions from skills:** rejected because restart would need durable reconstruction rules for generated definitions.
- **A thin Researcher wrapper:** rejected because capability belongs in a skill, not an actor type.
- **Three research skills:** rejected because one body can define intensity without catalogue duplication.
- **A `level` parameter on `skill()`:** rejected because research intensity is domain-specific and does not belong in the library-wide skill contract.
- **Parent-routed approval:** rejected because it recreates child ask coordination.
- **Recursive workers:** rejected until a concrete task needs delegation below the root.
- **Migrating legacy researcher headers:** rejected because historical viewing already works and continued execution is not required.

## Implementation phases

### Phase 0 — Durable child names · deps: none · CODE DONE · VERIFICATION PENDING

Deliverables:

- Extend Runtime's `spawn` framework tool with optional `name` normalization.
- Persist names in `SessionHeader`, `SessionCreated`, and `ChildCreated` while keeping `agent` as the registry key.
- Return the effective name through `ActorStatus` and preserve it across fresh Runtime construction.
- Treat a changed name on deterministic spawn reuse as `InvalidCommandReuse`.
- Update event, Runtime, subagent, and status reference documentation.

Verify:

- Explicit and omitted names round-trip through Memory, filesystem, SQLite, and PostgreSQL stores.
- `tree_status()` returns effective names without reading journals or activating actors.
- Identical retries deduplicate and a changed name conflicts without modifying the existing child.
- Existing records without name fields validate and fall back to the actor type.
- Focused Runtime and store suites, full Tantra tests, Ruff check and format check, and `git diff --check` pass.

Checklist:

- [ ] Spawn contract
- [ ] Durable name fields
- [ ] Status exposure
- [ ] Idempotency behavior
- [ ] Store and Runtime tests
- [ ] Reference documentation

### Phase 1 — General Sarathi subagent and research skill · deps: P0 · —

Deliverables:

- Replace active `Researcher` code with the registered `Subagent` actor and remove Researcher-specific tests and current documentation.
- Share Sarathi's non-interactive tool construction with Subagent without adding an abstraction outside the existing wiring function.
- Package `research/SKILL.md`, register `FileSystemSkills` in the application Runtime, and restrict both actors to the research skill.
- Update root and child prompts for inline work, optional delegation, level propagation, names, root-only asks, and explicit finish.
- Add effective name to the authenticated actors response, regenerate the client, and render it in every child UI placement.
- Preserve legacy researcher records as read-only history without aliases or migrations.

Verify:

- Root and child can independently discover and load the same research skill.
- The skill describes all three intensity levels and defaults omission to `normal`.
- Subagent has search, fetch, document, recall, skill, send, and finish capabilities but no memory write, spawn, status, or human ask.
- Sarathi remains the only actor that can spawn, and its declared child type is `subagent`.
- Named children keep `agent="subagent"` while all UI placements show the effective name; unnamed children display `subagent`.
- The skill asset exists in the built backend package and Docker image.
- Sarathi backend tests, generated-client checks, native UI tests, UI lint and build, relevant Tantra regressions, Ruff checks, and `git diff --check` pass.
- Brave verifies a named explicitly requested child, inline root research, child research and finish, drawer inspection, refresh, cancellation, and absence of child asks.

Checklist:

- [ ] General Subagent
- [ ] Shared tool wiring
- [ ] Research skill
- [ ] Delegation prompts
- [ ] Actor-name API and UI
- [ ] Legacy-history behavior
- [ ] Automated and Brave verification

### Phase 2 — Documentation and Tantra 1.1.0 · deps: P1, 008 P2 follow-up · —

Deliverables:

- Document named subagents, general-purpose delegation, task skills, research levels, root-only asks, and approval-tool restrictions.
- Add migration notes that legacy researcher history remains readable but non-resumable.
- Complete the outstanding documentation and release work from 008 Phase 3.
- Set `tantra-harness` and the lockfile to 1.1.0 and Sarathi's dependency floor to `>=1.1`.
- Add one 1.1 changelog entry covering child lifecycle, lazy observation, named actors, and skill-based research.
- Do not publish or tag.

Verify:

- Ruff check and format check pass.
- Tantra package and stress suites pass.
- All store contracts pass against real PostgreSQL.
- Sarathi backend tests pass.
- Native UI tests, UI lint, and production build pass.
- Strict MkDocs build, lock check, package build, Docker build, and `git diff --check` pass.
- Brave verifies inline research, explicitly requested named delegation, drawer inspection, all three research levels, finish delivery, cancellation, refresh, and absence of child asks.
- Mark 009 complete and close 008 Phase 3 only after the combined release checks pass.

Checklist:

- [ ] Public and migration documentation
- [ ] Combined 1.1 changelog
- [ ] Version and lockfile
- [ ] Full automated verification
- [ ] PostgreSQL verification
- [ ] Live Brave verification

### Conventions (all phases)

- Follow the repository's implementation and independent-review routine one requested phase at a time.
- Preserve actor UUID, FIFO journal, status, lifecycle, and lazy-subscription contracts.
- Add no dependency, dynamic actor registry, approval router, migration table, or recursive delegation.
- Update generated API clients whenever the actors response changes.
- Run each phase's focused checks plus the full applicable lint and test commands before marking it done.
- **Contract freeze:** spawn name semantics, `ActorStatus.name`, the `subagent` identity, research levels, and root-only delegation freeze in their introducing phase. Update this specification before changing them.

### Keeping this spec current

- Update the phase status marker and checklist during implementation.
- Record only deviations affecting behavior, contracts, scope, or phase boundaries.
- Keep routine implementation history out of the document.
- Put unresolved problems in Open Decisions or a follow-up note rather than implementing them across phase boundaries.

## Open Decisions

None.

## Risks

- Delegation necessity remains a model judgment rather than a Runtime rule.
- Research intensity is instruction-driven and bounded by the existing step limit.
- Legacy researcher actors remain historical only; attempting to activate one fails.
- Optional names are model-generated labels and may be omitted or poorly chosen.
- Sarathi tool wiring can drift between root and child; tests must assert the exact capability sets.

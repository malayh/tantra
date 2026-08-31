# Working guidelines

Behavioral guidelines to reduce common coding-agent mistakes. Merge them with project-specific instructions as needed.

These guidelines favor caution over speed. Use judgment for trivial tasks.

## Think before coding

Do not assume or hide confusion. Surface tradeoffs.

Before implementing:

- State assumptions explicitly. Ask when uncertainty materially changes the result.
- Present multiple plausible interpretations instead of silently choosing one.
- Point out simpler approaches and push back when warranted.
- If the request is unclear, name what is unclear and ask before changing code.

## Simplicity first

Write the minimum code that solves the requested problem.

- Add no unrequested features.
- Avoid abstractions used only once.
- Do not add speculative flexibility or configurability.
- Do not handle scenarios that cannot occur.
- If a solution is substantially longer than necessary, simplify it.
- Ask whether a senior engineer would consider the solution overcomplicated.

## Surgical changes

Touch only what the task requires. Clean up only consequences of your own changes.

- Do not improve adjacent code, comments, or formatting.
- Do not refactor unrelated code.
- Match the existing style.
- Mention unrelated dead code instead of deleting it.
- Remove imports, variables, functions, and files made obsolete by your changes.
- Every changed line must trace directly to the user's request.

## Goal-driven execution

Define success criteria and work until they pass.

Turn requests into verifiable goals:

- "Add validation" means test invalid inputs and make those tests pass.
- "Fix the bug" means reproduce it with a test and make that test pass.
- "Refactor X" means establish that relevant tests pass before and after.

For multi-step tasks, state a brief plan in this form:

1. Step and its verification check.
2. Step and its verification check.
3. Step and its verification check.

Strong success criteria reduce unnecessary clarification and rework.

These guidelines are working when diffs contain fewer unnecessary changes, solutions need fewer rewrites, and material ambiguity is resolved before implementation.

## General rules

- Do not write code comments. NO COMMENTS AT ALL
- Keep plans, specs, and user responses brief.
- Prefer short bullets where they improve readability.


## Orchestrating implementation

Roles:
- The main session runs `gpt-5.6-sol` with `reasoning_effort: "high"`. It orchestrates only: plans, spawns subagents, verifies, commits. It does not write implementation code itself.
- Implementation and review run in general-purpose subagents with `gpt-5.6-sol` with `reasoning_effort: "high"`. Never use `fork` — forks inherit main session model.

For each phase in the spec:
1. Enter plan mode. Plan the phase from the spec: files, approach, verify criteria. Exit plan mode for approval.
2. Spawn a general-purpose subagent (`gpt-5.6-sol`, synchronous) to implement:
   - Prompt must include: spec path, phase number, the approved plan, and "follow the spec's Conventions section".
   - Subagent implements, runs `just lint` + `just test`, reports what passed. (or equivalent commands)
3. Spawn a second general-purpose subagent (`gpt-5.6-sol`, synchronous) to review:
   - Prompt: review the phase diff against the spec's deliverables and Verify criteria; report defects with file:line.
4. If the review finds real defects, send them back to the implementer subagent (SendMessage) or spawn a fix subagent. Re-review only if changes were large.
5. Orchestrator verifies: run the phase's Verify criteria from the spec, plus `just lint` + `just test`.
6. Update the spec: status marker, checklist ticks, deviations struck with reasons.
7. Ask use to run compact
8. Ask user to commit the phase.

Rules:
- Subagents start with zero context — the prompt and the spec must carry everything.
- One phase at a time unless the spec's dependency graph says parallel; parallel phases use worktree isolation.
- Never mark a phase done with failing tests. Report failures honestly.

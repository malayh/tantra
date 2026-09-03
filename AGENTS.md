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


## Phase orchestration

These rules apply only when the `phase-orchestrator` skill is invoked for one phase of an existing feature spec.

```yaml
phase_orchestrator:
  implementer_model: gpt-5.6-sol
  implementer_reasoning_effort: high
  reviewer_model: gpt-5.6-sol
  reviewer_reasoning_effort: high
```

- The main session only plans, delegates implementation, verifies, coordinates review fixes, and maintains the phase status in the spec. It does not write implementation code.
- Make no edits and spawn no subagent until the user explicitly approves a decision-complete phase plan.
- During planning, stop if the requested numbered phase is missing or malformed, or its Conventions or Verify criteria are missing or unusable.
- Use zero-context implementation and review agents when the harness supports it. Pass the spec path, phase number, approved plan, and an instruction to follow the spec's Conventions section.
- Run exactly one implementation agent at a time, followed by a separate read-only reviewer. Route confirmed defects back to the original implementer when possible and allow at most two fix-and-review rounds after the initial review; then stop and report remaining defects without marking the phase done.
- The reviewer checks the current phase diff against its deliverables and Verify criteria and reports defects with `file:line` references.
- The orchestrator runs the phase Verify criteria plus `just lint` and `just test`. Never mark a phase done while required checks fail.
- Make only the spec edits expressly required by its `Keeping this spec current` block: phase status and checklist, explicit deviation notes, surprising post-phase details, and unresolved problems in Open Decisions or a Follow-up note.
- Do not create worktrees or branches, stage, commit, merge, push, reset, stash, or otherwise mutate Git. Read-only Git checks may establish the baseline and detect contamination.
- Preserve pre-existing user changes. Stop before implementation if they overlap the phase or make attribution unsafe.
- Finish with the phase results, verification, review outcome, and changed files. Ask the user to inspect the diff, run `/compact` when supported, and commit the phase.

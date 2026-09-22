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

## Subagent models

- Use only `gpt-5.6-terra` and `gpt-5.6-sol` for subagents, including reviewers.
- Use `gpt-5.6-terra` with `high` reasoning effort for routine, non-critical work. Do not use a cheaper model.
- Use `gpt-5.6-sol` with `high` reasoning effort for security, authorization, migrations, concurrency, architecture, public contracts, data-loss risk, or a failed Terra attempt. Do not use a more expensive model.

## Executing feature-spec phases

- Work on one requested phase at a time. Do not pull work from another phase into the current phase. If crossing a phase boundary is necessary, explain why and get user approval before proceeding.
- Delegate bounded work only when it materially saves time, reduces context pressure, or benefits from independent investigation.
- Apply the Ponytail skill at full intensity during implementation.
- Review once when code changes are made. Skip review for plans, specs, status updates, and documentation-only changes. Re-review only when review fixes materially change behavior.
- Run the phase Verify criteria and relevant project checks. Do not broaden or repeat checks without new changes, failures, or unresolved concerns.
- Update the feature spec according to its Keeping this spec current rules.

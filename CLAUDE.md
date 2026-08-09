Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

Tradeoff: These guidelines bias toward caution over speed. For trivial tasks, use judgment.

1. Think Before Coding
Don't assume. Don't hide confusion. Surface tradeoffs.

Before implementing:

State your assumptions explicitly. If uncertain, ask.
If multiple interpretations exist, present them - don't pick silently.
If a simpler approach exists, say so. Push back when warranted.
If something is unclear, stop. Name what's confusing. Ask.
2. Simplicity First
Minimum code that solves the problem. Nothing speculative.

No features beyond what was asked.
No abstractions for single-use code.
No "flexibility" or "configurability" that wasn't requested.
No error handling for impossible scenarios.
If you write 200 lines and it could be 50, rewrite it.
Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

3. Surgical Changes
Touch only what you must. Clean up only your own mess.

When editing existing code:

Don't "improve" adjacent code, comments, or formatting.
Don't refactor things that aren't broken.
Match existing style, even if you'd do it differently.
If you notice unrelated dead code, mention it - don't delete it.
When your changes create orphans:

Remove imports/variables/functions that YOUR changes made unused.
Don't remove pre-existing dead code unless asked.
The test: Every changed line should trace directly to the user's request.

4. Goal-Driven Execution
Define success criteria. Loop until verified.

Transform tasks into verifiable goals:

"Add validation" → "Write tests for invalid inputs, then make them pass"
"Fix the bug" → "Write a test that reproduces it, then make it pass"
"Refactor X" → "Ensure tests pass before and after"
For multi-step tasks, state a brief plan:

1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

These guidelines are working if: fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.


## Genearl guidelines
- DONOT WRITE COMMENTS. NO COMMENTS AT ALL.
- While writing plans or specs files or responding to users  brevity is the key. Don't be verbose. Write in short bullet points where ever possible.

## Orchestrating implementation

Roles:
- The main session runs Fable 5. It orchestrates only: plans, spawns subagents, verifies, commits. It does not write implementation code itself.
- Implementation and review run in general-purpose subagents with `model: "opus"` (cheaper). Never use `fork` — forks inherit Fable.

For each phase in the spec (`design/001_v1_spec.md`):
1. Enter plan mode. Plan the phase from the spec: files, approach, verify criteria. Exit plan mode for approval.
2. Spawn a general-purpose subagent (`model: "opus"`, synchronous) to implement:
   - Prompt must include: spec path, phase number, the approved plan, and "follow the spec's Conventions section".
   - Subagent implements, runs `just lint` + `just test`, reports what passed.
3. Spawn a second general-purpose subagent (`model: "opus"`, synchronous) to review:
   - Prompt: review the phase diff against the spec's deliverables and Verify criteria; report defects with file:line.
4. If the review finds real defects, send them back to the implementer subagent (SendMessage) or spawn a fix subagent. Re-review only if changes were large.
5. Orchestrator verifies: run the phase's Verify criteria from the spec, plus `just lint` + `just test`.
6. Update the spec: status marker, checklist ticks, deviations struck with reasons.
7. Run compact
8. Commit the phase.

Rules:
- Subagents start with zero context — the prompt and the spec must carry everything.
- One phase at a time unless the spec's dependency graph says parallel; parallel phases use worktree isolation.
- Never mark a phase done with failing tests. Report failures honestly.


# Skills

A skill is a folder with a `SKILL.md` in it. Progressive disclosure: only each skill's name and description (~30 tokens) ride in the system prompt, and the model pulls a body on demand with the auto-provided `skill` tool. Ten skills of 2 000 tokens each would otherwise burn 20 000 tokens before the user has spoken.

## Layout

```
skills/
  cold-email/
    SKILL.md
    references/frameworks.md
```

```markdown
---
name: cold-email
description: Write a cold outbound email that gets a reply.
---

# Cold email

Open on the reader's problem, not on yourself.
```

`FileSystemSkills(root)` scans one level of directories under `root` and parses the YAML frontmatter — top-level `key: value` lines between `---` fences. `name` and `description` are required. Block scalars (`>`, `|`, and the chomping variants) work; the parser is hand-rolled, so nested keys are ignored.

## Wiring

```python
harness = Harness(provider, store, [Writer], default_model=..., skills=FileSystemSkills("./skills"))
```

That injects a `skill` tool into every agent that is not opted out, and appends one extra system block per sample:

```text
Skills available via the skill(name) tool:
- cold-email: Write a cold outbound email that gets a reply.
```

`Agent.skills` filters:

- `None` (the default) — every skill in the catalogue.
- `["cold-email", "seo"]` — only those. A name that is not in the catalogue raises when the turn runs, so typos fail loudly.
- `[]` — opt out entirely: no index block, no `skill` tool.

!!! warning
    `skills = ()` is **not** recognised as opt-out. The check is `== []`, and a tuple is not equal to a list. Use `[]`.

## Loading

`skill(name)` returns the body, plus a `## Files` listing of everything else in the skill directory so the model knows what it can read next:

```text
# Cold email

Open on the reader's problem, not on yourself.

## Files
references/frameworks.md
```

An unknown or filtered-out name is an `is_error` result, not a failed turn — and the filter is checked before any I/O.

## Two things that surprise people

**The `skill` tool obeys the permission engine.** It is injected with `permission="allow"`, but there is no framework exemption: under `Harness(default_permission="ask")`, a sub-agent calling `skill` **asks**, because a rule-less ancestor contributes the harness default and sub-agent permissions are never widened. Give the parent a `{"skill": "allow"}` rule to grant silent loads. See [permissions](permissions-hooks.md).

**A malformed skill directory is skipped, not fatal.** Bad frontmatter, an unreadable or non-UTF-8 file, or a duplicate skill name lands in `FileSystemSkills.skipped` as `(path, reason)` and the siblings still index. A missing root still raises. Surface the list at startup — a silently absent skill is a capability the model believes it has:

```python
skills = FileSystemSkills("./skills")
await skills.index()
for path, reason in skills.skipped:
    print(f"skipping {path}: {reason}")
```

`skipped` is refreshed on every scan, and `load()` of a skipped name raises like an unknown one.

Skill output is also exempt from [compaction](compaction.md) pruning — dropping it would silently remove a capability mid-session.

## Next

- [Skills reference](../reference/skills.md) — the `Skills` protocol, for a database-backed catalogue.

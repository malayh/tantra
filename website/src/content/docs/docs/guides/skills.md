---
title: Skills
---

A `Skills` source indexes named instruction packages. `FileSystemSkills(root)` reads directories containing `SKILL.md` and records invalid entries in `skipped`.

```python
runtime = Runtime(
    provider,
    store,
    [Writer],
    skills=FileSystemSkills("./skills"),
)
```

Runtime injects the `skill` and `skill_file` tools into agents unless `Agent.skills = []`. `None` exposes the full catalogue; a list limits the index and `skill` to those names and fails construction or activation when a requested name is missing.

Both tools declare `allow`, so they run without approval, but any matching agent rule wins (`"*"`, `"*_file"`, …). Grant `"skill_file": "allow"` wherever `skill` is granted. On a child agent, an `ask` verdict for `skill_file` fails Runtime construction.

Skill names and descriptions appear in the execution-environment system block. Full skill bodies remain out of context until the actor calls `skill(name)`. Root and child actors have independent tool sets and skill filters, so a general-purpose child can load task-specific instructions without changing its durable actor type.

Sarathi demonstrates this pattern with one `subagent` actor and one packaged `research` skill. The delegated task carries the requested level: `shallow` uses one focused search pass, `normal` uses multiple query angles and cross-checks important claims, and `deep` decomposes the question and follows contradictions. An omitted level means `normal`.

## Skill files

`skill(name)` lists a skill's bundled files under `## Files`. `skill_file(skill, path)` reads one of them, or lists a folder. `path` is relative to the named skill's folder, so a path written in a `SKILL.md` works as written:

- `references/api.md` reads a file in the same skill.
- `../other-skill/notes.md` reads a file in a sibling skill.
- `.` lists the skill's folder, and `..` lists the whole catalogue.

Write cross-references from the referring skill's folder. In `../other-skill/notes.md`, `other-skill` is the sibling's folder, which need not match its skill name. Any skill in the catalogue can be passed as `skill`, whatever the agent's allowlist says. Results over 64 000 characters are truncated. See [Skills](/tantra/docs/reference/skills/) for the path rules and errors.

`FileSystemSkills` follows symlinks under its root, so a skill folder that is a symlink, as in `~/.claude/skills`, reads and lists like a plain folder. A symlink that points outside the root is readable too. This is by design: check downloaded skills for such links.

## Custom backends

A custom `Skills` source implements `index`, `load` and `read(name, path)`. `read` joins `path` to the skill's folder as text, refuses absolute paths and anything above the catalogue root, returns a file's UTF-8 text or a folder's sorted recursive listing relative to the skill's folder, and raises `TantraError` otherwise. Runtime doesn't check for `read`: without it, every `skill_file` call is a tool error.

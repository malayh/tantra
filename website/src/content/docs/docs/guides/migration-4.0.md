---
title: Migrate to 4.0
slug: docs/guides/migration-4.0
---

Tantra 4.0 adds the `skill_file` tool, so agents read any file in the skills catalogue and a path written in a `SKILL.md` works as written. `Skills.read(name, path)` is now required. Journal bodies and sequences are unchanged, and no store has a new migration.

## What changes for models

- Agents with skills get `skill_file(skill, path)` beside `skill`. `path` is relative to the named skill's folder: `references/x.md` stays in the skill and `../other-skill/x.md` reaches a sibling. A folder returns every file under it, one path per line.
- Any skill in the catalogue can be passed as `skill`, whatever `Agent.skills` allows. The allowlist still gates the index and `skill`.
- Results over 64 000 characters end with `[truncated at 64000 chars]`. Compaction prunes `skill_file` results and still never prunes `skill` output.
- The skills guidance now tells the model to read files a loaded skill refers to with `skill_file`. The system prompt changes once, so prompt caches for agents with skills are invalidated a single time.

## What changes for applications

- A custom `Skills` source must add `async def read(self, name: str, path: str) -> str`. Runtime doesn't check for it: without it, every `skill_file` call is a tool error. See [Skills](/tantra/docs/reference/skills/) for the path rules and errors.
- `skill_file` is a reserved tool name. An agent with skills that declares its own `skill_file` fails Runtime construction.
- Both skills tools declare `allow`, but any matching agent rule wins (`"*"`, `"*_file"`, …). Grant `"skill_file": "allow"` wherever `skill` is granted. On a child agent, an `ask` verdict for `skill_file` fails Runtime construction, so a child with `{"*": "ask", "send": "allow", "skill": "allow"}` that worked in 3.x now raises `TantraError`.
- `Agent.skills` no longer hides other skills' contents: `skill_file` reads any file in the catalogue, `SKILL.md` included. Use `skills = []` to hide the catalogue from an agent.
- `FileSystemSkills.load` now lists files under symlinked subfolders. A folder reached by two paths is listed once, under its real path when the listing reaches it, otherwise under the first link in sorted order.
- `FileSystemSkills` follows symlinks under its root, so a symlink that points outside the root is readable. Check downloaded skills for such links. `load` and folder listings walk symlinked folders synchronously, so a link to a large tree stalls them.
- Virtual paths are POSIX. `\` isn't a separator, so on Windows `FileSystemSkills` can't keep `..\` paths inside its root.

## Existing data

3.x journals load unchanged and need no migration.

## Deploy

There is no ordering constraint: 4.0 adds no journal events or coordinator payloads. In a mixed fleet, only 4.0 Runtimes offer `skill_file`.

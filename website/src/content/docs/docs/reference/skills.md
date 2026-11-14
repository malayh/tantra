---
title: Skills
---

`Skills.index()` returns `SkillInfo` records, `Skills.load(name)` returns a `Skill`, and `Skills.read(name, path)` returns one file or folder as text. All three are required. `FileSystemSkills` implements the protocol for `SKILL.md` directories.

Pass the source through `Runtime(skills=...)`. Agent filtering and permission handling occur in Runtime.

Runtime adds allowed skill names and index descriptions to one execution-environment system block. Calling `skill(name)` returns the body as a tool result. The body is therefore loaded on demand rather than copied into every sample. `Agent.skills=None` exposes the catalogue, a list is an allowlist, and `[]` disables both `skill` and `skill_file`.

## Virtual paths

`read(name, path)` treats the catalogue as one POSIX tree with a folder per skill.

- `path` is relative to skill `name`'s folder. It is normalised as text inside the catalogue, never resolved through storage.
- `..` reaches sibling skills. A path that is absolute or climbs above the catalogue root is refused.
- A file returns its UTF-8 text.
- A folder returns every file under it, recursively and sorted, one per line. Each path is relative to the skill's folder, so it reads back with the same `name`.
- In `FileSystemSkills`, a folder reachable by two paths is listed once, under its real path when the listing reaches it, otherwise under the first link in sorted order. A folder the process can't read is silently left out of listings.

`read` raises `TantraError` for an unknown skill, an absolute path or one above the root, nothing at the path, or a file that is not UTF-8. `FileSystemSkills` also raises `TantraError` when a file can't be read, naming the agent's path rather than the storage path.

## skill_file

`skill_file(skill, path)` returns `read(skill, path)`. Results over 64 000 characters are cut and end with `[truncated at 64000 chars]`. The cap applies to every backend.

The allowlist gates the index and `skill` only. Any skill in the catalogue can be passed as `skill`, so cross-references between skills always work.

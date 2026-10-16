# Skills

`Skills.index()` returns `SkillInfo` records and `Skills.load(name)` returns a `Skill`. `FileSystemSkills` implements the protocol for `SKILL.md` directories.

Pass the source through `Runtime(skills=...)`. Agent filtering and permission handling occur in Runtime.

Runtime adds allowed skill names and index descriptions to one execution-environment system block. Calling `skill(name)` returns the body as a tool result. The body is therefore loaded on demand rather than copied into every sample. `Agent.skills=None` exposes the catalogue, a list is an allowlist, and `[]` disables the tool.

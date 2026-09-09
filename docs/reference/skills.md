# Skills

`Skills.index()` returns `SkillInfo` records and `Skills.load(name)` returns a `Skill`. `FileSystemSkills` implements the protocol for `SKILL.md` directories.

Pass the source through `Runtime(skills=...)`. Agent filtering and permission handling occur in Runtime.

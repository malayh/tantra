# Skills

A `Skills` source indexes named instruction packages. `FileSystemSkills(root)` reads directories containing `SKILL.md` and records invalid entries in `skipped`.

```python
runtime = Runtime(
    provider,
    store,
    [Writer],
    skills=FileSystemSkills("./skills"),
)
```

Runtime injects the `skill` tool into agents unless `Agent.skills = []`. `None` exposes the full catalogue; a list exposes only those names and fails construction or activation when a requested name is missing.

The skill tool participates in normal permissions. Grant it on each agent that should load skills without a live approval.

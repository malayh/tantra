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

Skill names and descriptions appear in the execution-environment system block. Full skill bodies remain out of context until the actor calls `skill(name)`. Root and child actors have independent tool sets and skill filters, so a general-purpose child can load task-specific instructions without changing its durable actor type.

Sarathi demonstrates this pattern with one `subagent` actor and one packaged `research` skill. The delegated task carries the requested level: `shallow` uses one focused search pass, `normal` uses multiple query angles and cross-checks important claims, and `deep` decomposes the question and follows contradictions. An omitted level means `normal`.

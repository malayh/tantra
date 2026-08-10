# Skills

```python
from tantra import FileSystemSkills, Skill, SkillInfo, Skills
```

A catalogue of instructions, disclosed progressively: the index (name + description) rides in every system prompt, and the model calls `skill(name)` to pull one body into context.

Wire it with `Harness(skills=FileSystemSkills("./skills"))`. That injects a `skill` tool into every registered agent that has not opted out.

## `Skills` (protocol)

```python
async def index(self) -> Sequence[SkillInfo]
async def load(self, name: str) -> Skill
```

`index` is cheap and called once per turn; `load` pulls one skill in full and raises `TantraError` for an unknown name.

Filtering lives in the harness, not the catalogue: `Agent.skills` is validated against the full index (unknown names raise `TantraError`), so a custom `Skills` cannot push the filter down to a database.

## Data types

| Type | Fields |
|---|---|
| `SkillInfo` | `name: str`, `description: str` |
| `Skill` | `name: str`, `description: str`, `body: str`, `files: tuple[str, ...] = ()` |

`files` is populated only by `load` — paths relative to the skill directory, `SKILL.md` excluded.

## `FileSystemSkills(root)`

One directory per skill, each holding a `SKILL.md`:

```text
skills/
  seo-audit/
    SKILL.md
    references/checklist.md
  release-notes/
    SKILL.md
```

`SKILL.md` opens with a `---` fence, carries top-level `key: value` frontmatter, and everything after the closing fence is the body:

```markdown
---
name: seo-audit
description: Audit a site's technical SEO and report findings.
---

Crawl first, opine second.
```

Parsing rules, in brief:

- `name` and `description` are required; both are used in the prompt index.
- Quotes are stripped; nested keys are ignored; UTF-8 BOM and CRLF are tolerated.
- YAML block scalars work: `>`-family folds to spaces, `|`-family keeps newlines. Only an unindented `---` closes the frontmatter.
- The catalogue is rescanned on every `index` and `load`, so edits land without a restart.

### `.skipped`

A malformed, unreadable or duplicate-named skill directory is **skipped**, not fatal — a shared skills root must not brick every agent. After each scan, `skills.skipped` is a fresh `list[tuple[Path, str]]` of what was dropped and why. Surface it at startup:

```python
skills = FileSystemSkills("./skills")
await skills.index()
for path, reason in skills.skipped:
    print(f"skipped {path}: {reason}")
```

A missing or non-directory `root` still raises `TantraError`. `load()` of a skipped name raises too.

## The `skill` tool

Injected per agent at construction with `permission="allow"`, named `skill` (`tantra.skills.SKILL_TOOL`). It takes `name: str` and returns the body, plus a `## Files` listing when the skill ships reference files. A user tool named `skill` collides and raises at construction.

The index is ordered by **directory name** (`sorted(root.iterdir())`), which is not necessarily the order of the `name:` values in the frontmatter — a skill in `01-intake/` declaring `name: triage` still sorts by `01-intake`. The tool is advertised even when the catalogue is empty: injection is construction-time, the index is run-time.

## `Agent.skills`

| Value | Effect |
|---|---|
| `None` (default) | Every skill in the catalogue is indexed. |
| `["a", "b"]` | Only those, and unknown names raise `TantraError` at run time. |
| `[]` | Opt out: no `skill` tool, no index block. |

!!! danger "`skills = ()` is not an opt-out"
    The opt-out test is `agent.skills == []`, and `() != []`. A tuple registers the `skill` tool, indexes nothing, and rejects every name the model tries. Use `[]`.

!!! warning "Sub-agent skill loads ask under `default_permission="ask"`"
    The tool's declared `allow` wins only at depth 0. A rule-less ancestor contributes the harness default to the child's verdict, so children suspend on every `skill` call. Grant it on the parent: `permissions = {"skill": "allow"}`. See [Permissions](permissions.md).

## See also

- [Compaction](compaction.md) — `skill` output is never pruned.
- [Guides: skills](../guides/skills.md).

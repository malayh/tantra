# 022 — Skill files (tantra 4.0)

## Goal

- An agent reads any file in the skills catalogue on demand and lists any folder in it.
- A path written in a `SKILL.md` works as written: `references/x.md`, `../other-skill/notes.md`.
- Success means all of these work:
  - `skill_file("a", "../b/notes.md")` returns `b`'s notes, also when the agent's allowlist is `["a"]`.
  - `skill_file("a", "..")` lists every file in the catalogue. Each line can be passed back as `path` with `skill="a"`.
  - `skill_file("a", "../../etc/passwd")` and `skill_file("a", "/etc/passwd")` fail before the target is touched. (`FileSystemSkills` still scans every `SKILL.md` to find the anchor.)
  - A skill folder that is a symlink (the `~/.claude/skills` layout) reads and lists like a plain folder.
  - Compaction prunes `skill_file` results and still never prunes `skill` results.

## Scope

**In**
- `Skills.read(name, path)`, required.
- `FileSystemSkills.read`.
- The `skill_file` framework tool and one sentence in `SKILLS_GUIDANCE`.
- Docs, changelog, `tantra-harness` 4.0.0.

**Out**
- Writing, moving or deleting files.
- Binary files and images.
- Glob, grep or search.
- Paging past the size cap.
- The agent's skill allowlist gating reads.
- Backends other than the filesystem.
- Changes to Sarathi's skills.

## Virtual paths

- The catalogue is one virtual POSIX tree. Each skill has a folder in it. `name` is the working directory for `path`.
- Resolution is string-only: `posixpath.normpath(posixpath.join(folder, path))`.
  - Refused when the result is absolute, is `..`, or starts with `../`.
  - `.` is the catalogue root, and reading it lists the whole catalogue.
- The backend maps the resolved virtual path to its storage. The contract knows nothing about symlinks, buckets or rows.
- *Why string-only:* containment then holds for every backend, and the filesystem's details stay inside `FileSystemSkills`. Resolving through the OS first breaks symlinked skill folders: `root/a/..` resolves to the parent of the link target, outside the catalogue.
- A file returns its UTF-8 text.
- A folder returns every file under it, recursively, sorted, one per line. Each path is relative to the skill's folder (`../b/notes.md`), so it can be passed back with the same `name`.
- Errors raise `TantraError`: unknown skill, path above the root, nothing at the path, file not UTF-8. `FileSystemSkills` also turns an `OSError` on read (permission denied) into `TantraError` naming the agent's path, never the storage path.
- A skill's folder isn't its name. The folder holds its `SKILL.md`; the name comes from the frontmatter. The agent sees only names, which is why `name` is the anchor.

## Protocol

```python
class Skills(Protocol):
    async def index(self) -> Sequence[SkillInfo]: ...
    async def load(self, name: str) -> Skill: ...
    async def read(self, name: str, path: str) -> str: ...
```

- `read`'s docstring states the virtual-path contract above, in the style of the existing `index` and `load` docstrings.
- Required. The runtime doesn't check for it, as it doesn't check `index` or `load`. A backend without `read` fails on its first `skill_file` call, which the agent sees as a tool error.
- `load` stays. It is the parsed entry point (body without frontmatter plus file list), and compaction never prunes it. `read` is raw access to one file.
- Existing test fakes (`test_skills.py:RecordingSkills`, `test_runtime.py:Catalog`, Sarathi's `test_subagent.py:Catalogue`) never reach `read` and stay as they are.

## FileSystemSkills

- `read` finds the skill's directory through `_scan()`, like `load`. Its virtual folder is the directory's name relative to `root`.
- After virtual resolution, the concrete path is `self.root / virtual`. It is opened with normal OS semantics, so symlinks under `root` are followed: they are the operator's layout.
- A folder is walked with `os.walk(..., followlinks=True)`. A folder whose real path was already walked is skipped, so a symlink to an ancestor can't loop.
- **Surprise:** `Path.rglob("*")`, used by `_files` today (`skills.py:100`), doesn't descend into symlinked subfolders. A root-level `rglob` wouldn't enter symlinked skill folders at all.
- `_files` moves to the same walk and drops its own `SKILL.md`, so `load` and `read` list the same files. `load`'s list changes only for skills with symlinked subfolders.
- File IO stays synchronous inside the async methods, as in `load`.
- `_scan()` re-parses every `SKILL.md` on each call. This already happens on every `load` and now happens on every `skill_file` call too. Accepted.

## The `skill_file` tool

- `runtime.py`, next to `_skill_tool` (`runtime.py:137`): `_skill_file_tool(skills)` builds
  ```python
  async def skill_file(skill: str, path: str) -> str
  ```
- Description: "Read one file, or list one folder, in the skills catalogue. `path` is relative to the named skill's folder, exactly as that skill writes it; `../other-skill/file.md` reaches another skill. A folder returns every file under it, one path per line, each usable as `path` with the same skill."
- `permission="allow"`, like `skill`.
- Registered in `Runtime.__init__` beside `skill` (`runtime.py:366-372`): same condition (`skills is not None` and `agent.skills != []`), same duplicate-name check.
- `SKILL_FILE_TOOL = "skill_file"` in `skills.py`, beside `SKILL_TOOL`. `SKILL_FILE` already means `"SKILL.md"`.
- No allowlist check. Any skill in the catalogue can be the anchor. *Why:* `..` reaches the whole catalogue from any anchor anyway, so restricting the anchor protects nothing.
- Results are capped at 64 000 chars and end with `\n[truncated at 64000 chars]`, as in `read_doc` and `bash`. The cap lives in the tool, so it applies to every backend.
- Compaction needs no change. `_prune` exempts only `SKILL_TOOL` by name (`compaction.py:268`), so `skill_file` results are pruned.
- `SKILLS_GUIDANCE` (`context.py:37`) becomes: "Available skills can be loaded on demand with the skill tool. Read a file a loaded skill refers to with skill_file, passing that skill's name and the path exactly as written. Load a skill when its description matches the task or the user explicitly requests it:"
- The `## Files` section of `skill`'s output doesn't change. Its paths are already relative to the skill's folder.

## Decisions

- **`read` is required, 4.0.0.** The user chose one contract and one code path over keeping custom backends working unchanged.
- **`read` sits beside `load`, not merged into it.** See Considered & rejected.
- **`name` is the working directory.** Paths copied from a `SKILL.md` work without rewriting.
- **The whole catalogue is readable whatever `Agent.skills` says.** The allowlist gates the index and `skill` only, so cross-references between skills always work.
- **Containment is virtual and string-only.** The interface is storage-agnostic. `FileSystemSkills` follows symlinks under its root.
- **`skill_file` is a separate tool,** so its results can be pruned without compaction inspecting arguments.
- **Text only.** Binary reads are out of scope.
- **Listed paths are relative to the anchor skill,** so the agent passes them back unchanged.

## Considered & rejected

- **`read(path)` relative to the root.** The agent would have to rewrite `../b/x.md` into `b/x.md`, folder names are hidden from it, and database backends would have to invent a root.
- **`load(name, path=None)`.** Returns a `Skill` sometimes and a string other times, and compaction would have to inspect arguments.
- **Only `read`, with the runtime building `load` by parsing `SKILL.md`.** Forces the `SKILL.md` frontmatter format on every backend.
- **Optional `read` detected with `hasattr`.** Two code paths, and an agent's tools would depend on the backend.
- **Resolving through the OS, then checking the result is inside the root.** Breaks symlinked skill folders.
- **A symlink policy in the contract.** The interface shouldn't know what storage is behind it.
- **The skill's absolute folder path in `skill`'s output, read with `bash`.** Needs `bash`, works only on the filesystem, and enforces no containment.
- **A `path` argument on `skill`.** Its results would become unprunable, or compaction would have to inspect arguments.

## Implementation phases

### Phase 1 — `Skills.read` and `skill_file` · deps: none · ✅ DONE

- **Deliverables**
  - `skills.py`: `SKILL_FILE_TOOL`, `Skills.read` with its docstring, the virtual-path resolution, `FileSystemSkills.read`, the shared walk used by `_files`.
  - `runtime.py`: `_skill_file_tool`, registration and the duplicate-name check.
  - `context.py`: the new `SKILLS_GUIDANCE`.
  - Tests:
    - `tests/test_skills.py`: a file, a `../` read into a sibling skill, a folder listing relative to the anchor, a root listing through `..`, a symlinked skill folder (including `../` from it), a symlinked subfolder in listings and in `load`'s files, a symlink cycle that terminates, refused escapes (`../..`, `/etc/passwd`, `references/../../../x`), a missing path, a non-UTF-8 file, an unknown skill. Update the guidance assertion (`test_skills.py:128`).
    - `tests/test_runtime.py`: an agent with `skills = ["a"]` reads `../b/x.md` through `skill_file`, and also reads with `skill="b"`. The schema requires `skill` and `path`. The cap marker appears on a large file. `skills = []` gets no `skill_file`. An agent tool named `skill_file` raises `TantraError` at construction. Update the guidance assertion (`test_runtime.py:991`).
    - `tests/test_compaction.py`: next to `test_skill_output_is_never_stubbed`, a large `skill_file` result is stubbed.
    - `apps/sarathi/backend/tests/test_subagent.py`: add `skill_file` to both tool sets (lines 58 and 73).
- **Verify**
  - Every Goal success bullet holds as a test.
  - An escape is refused before any file is opened: the test points the escape at a real file outside the root and gets an error.
  - `load` on a plain skill lists the same files as before (`test_load_returns_the_body_without_the_frontmatter_and_lists_the_files` passes unchanged).
  - `just lint`, `just test`, and `.venv/bin/pytest apps/sarathi/backend/tests` all pass.
- **Checklist**
  - [x] protocol and virtual paths
  - [x] `FileSystemSkills.read` and the shared walk
  - [x] tool, registration, guidance
  - [x] tests
- **Notes after landing**
  - The real-path guard in `_walk` is shared across one walk, and folder names are sorted. A folder reachable by two paths is listed once, under the first in sorted order: with `a/tob -> ../b`, a root listing shows `b`'s files only under `a/tob/`.
  - A folder the process can't read is silently left out of listings (`os.walk` drops the error).
  - Listing paths are built with `PurePosixPath.relative_to(folder, walk_up=True)`, not `posixpath.relpath`, which calls `os.getcwd()` and fails if the cwd is deleted.
  - Checked against the real `~/.claude/skills` (symlinked skill folders): 14 skills, a 335-line root listing, sibling reads, and escapes refused.
  - Postgres tests skip without a DSN (311). This phase doesn't touch stores.

### Phase 2 — Docs, changelog, 4.0.0 · deps: P1 · —

- **Deliverables**
  - Docs in `website/src/content/docs/docs/`:
    - `reference/skills.md`: `read`, virtual paths, errors, the cap, and that the allowlist doesn't gate reads.
    - `guides/skills.md`: `skill_file`, writing cross-references between skills, and what a custom `Skills` backend must implement.
    - `concepts/architecture.md:32`: mention files read on demand with `skill_file`.
  - `packages/tantra/CHANGELOG.md` 4.0.0:
    - Breaking: `Skills.read` is required; `skill_file` is a reserved tool name.
    - Added: `skill_file`, `FileSystemSkills.read`.
    - Changed: `load`'s file list includes symlinked subfolders; the skills guidance text.
    - Compatibility: journals load unchanged with no migration; custom `Skills` backends must add `read`.
  - Versions: `packages/tantra/pyproject.toml` 4.0.0 and the `tantra-harness` entry in `uv.lock` (`uv.lock:1454`). Publish after the owner approves.
- **Verify**
  - `npm --prefix website run build` passes.
  - No doc still says `skill` is the only skills tool: `grep -rn "skill tool" website/src/content/docs`.
  - `just lint` and `just test` pass.
- **Checklist**
  - [ ] docs
  - [ ] changelog
  - [ ] versions
  - [ ] release

### Conventions (all phases)

- Follow `AGENTS.md`. No code comments. Docstrings only where the module already uses them (the `Skills` protocol).
- Run `just lint` and `just test` before marking a phase done. `just test` covers only `packages/tantra/tests` (`pyproject.toml:47`), so also run `.venv/bin/pytest apps/sarathi/backend/tests`.
- `uv` isn't on PATH here: use `/home/malay/Code/tantra/.venv/bin/{ruff,pytest}` directly.
- **Contract freeze (end of P1):** the `read(name, path)` signature and virtual-path rules, the `skill_file(skill, path)` signature and name, the listing format, the cap. Changing any of them means updating this spec first.

### Keeping this spec current

- Update the status marker on each heading and tick the checklist as you go.
- Keep the plan matching the current truth. Add one brief deviation note only when behaviour, scope, contracts or phase boundaries materially change.
- After a phase lands, add only the details that would surprise the next reader.
- Put unresolved problems in Open Decisions with enough detail to act on later. Don't fix them outside the requested phase.

## Open Decisions

- **Dotfiles in listings.** Default: listed, as `_files` lists them today. A skill folder that is a git checkout lists `.git/`, up to the cap.
- **An empty folder.** Default: an empty string.

## Risks

- **Custom `Skills` backends break on upgrade.** Without `read`, every `skill_file` call is a tool error. The changelog names it as breaking.
- **An agent tool already named `skill_file`** fails `Runtime` construction after the upgrade.
- **A symlink under the root that points outside it is readable.** That is the operator's layout, by design. A downloaded skill that ships such a link exposes its target.
- **Large catalogues.** A root listing of a big catalogue stops at the cap, and each `skill_file` call re-scans every `SKILL.md`.
- **`load` now follows symlinked subfolders.** A skill that ships `docs -> /` makes every `load` and folder read walk that tree synchronously, before the cap applies.
- **POSIX only.** `\` isn't a separator in virtual paths, but on Windows `self.root / virtual` would split on it, so `..\..\x` would escape.
- **The system prompt changes once** with the new guidance text, which invalidates prompt caches for agents with skills a single time.

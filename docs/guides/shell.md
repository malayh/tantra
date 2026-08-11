# Shell & ShellGuard

```python
from tantra.extratools.shell import ShellGuard, bash
```

Ships in the base install — stdlib only, no extra needed.

## `bash(*, timeout=120.0)`

A factory returning a `Tool` named `bash` with one model-facing parameter, `command: str`. It:

- runs the command through a shell in `Path.cwd()`, in its own process group (`start_new_session=True`), and kills the whole group on timeout;
- merges stdout and stderr into one string, so ordering between them is approximate;
- caps output at 64 000 characters with a `[truncated: N chars omitted]` note, and appends `[exit status N]` on a non-zero exit;
- emits `$ <command>` as `ToolProgress`.

`timeout` is a factory argument, deliberately: the model cannot raise its own cap. The tool declares `permission="ask"`, so a headless deployment must override it with a rule (`permissions = {"bash": "allow"}`).

## `ShellGuard`

A `Hook` implementing `before_tool`. It acts only on calls named `bash`; everything else passes through untouched.

```python
ShellGuard(on_trip="deny", deny_extra=["terraform destroy"])
```

- `on_trip="deny"` (default) returns `Denial(reason)` — the model sees `denied by hook: <reason>` and adapts. This works headless, where no human is wired to answer.
- `on_trip="ask"` returns `Escalation(reason)`, forcing the permission verdict to at least `ask`: the reason is prepended to the approval body and the call goes through the normal suspend/resume flow. One `AskRaised` total, even when the tool is already `permission="ask"`.
- `deny_extra` is a plain substring match against the raw command and against any code string the parser recursed into.

### It parses, it does not regex

The command is tokenized with `shlex` (punctuation-aware, comments disabled) and split on `&&`, `||`, `;`, `|`, `&`. Each segment is walked, and the guard recurses into the classic wrappers:

| Written as | Checked as |
|---|---|
| `sh -c "..."`, and the same for `bash`, `zsh`, `dash`, `ksh` | the inner command line |
| `xargs [flags] rm -rf` | `rm` with targets supplied at runtime |
| `env FOO=1 <cmd>`, `nohup`/`nice`/`timeout`/`setsid` `<cmd>` | `<cmd>` |
| `find … -delete`, `find … -exec <cmd>` | a delete of the search paths, or `<cmd>` |
| `python -c`, `node -e`, `perl -e`, `ruby -e` | the code string, scanned for destructive markers |
| `eval "..."` | the re-joined payload |

Default deny rules stay deliberately short — destructive and irreversible only: recursive `rm` targeting `/`, `~`, or any path outside the working directory; `dd` writing to a real `/dev/*`; `mkfs*`; `shutdown`/`reboot`/`halt`/`poweroff`; fork bombs; `sudo`/`doas`/`su`; recursive `chmod`/`chown`/`chgrp` on `/` or `~`.

### Fails closed

An unparseable command line trips the guard. So does a recursive `rm` whose target contains `$` or a backtick — `rm -rf ./build/$STAGE` is denied, a known false-positive class — and anything piped or redirected into a bare shell, since what would run cannot be read beforehand.

!!! danger "A guardrail, not a sandbox"
    `ShellGuard` closes known routes, not the category. Accepted gaps are recorded, not fixed: `case` bodies are not segmented, an `eval` payload loses its quoting when re-joined, and verdicts are **CWD-dependent** (`Path.cwd()` at check time — the same command is allowed in one directory and denied in another). A determined model routes around any guard; this was observed live against the unhardened version. For real isolation use OS-level sandboxing — a container, a VM, a user with no write access — and treat the guard as the thing that catches accidents.

## End to end

```python
import asyncio

from tantra import Agent, FakeProvider, Harness, MemoryStore, Sample, collect
from tantra.events import ToolCallCompleted, ToolCallStarted, TurnCompleted
from tantra.extratools.shell import ShellGuard, bash
from tantra.providers.base import ToolCall


class Shell(Agent):
    prompt = "You run shell commands."
    tools = [bash(timeout=30.0)]
    permissions = {"bash": "allow"}


async def main() -> None:
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[ToolCall(id="c1", name="bash", args='{"command": "sh -c \\"rm -rf /\\""}')]),
                Sample(tool_calls=[ToolCall(id="c2", name="bash", args='{"command": "printf ok"}')]),
                Sample(text="I will not do that."),
            ]
        ),
        MemoryStore(),
        [Shell],
        default_model="fake/model",
        hooks=[ShellGuard()],
    )
    sid = (await harness.create_session(Shell)).id

    events = await collect(harness.run(sid, "wipe the disk"))

    for done in [e.event for e in events if isinstance(e.event, ToolCallCompleted)]:
        print(done.is_error, done.result)
    print("started:", [e.event.call_id for e in events if isinstance(e.event, ToolCallStarted)])
    print([e.event.stop_reason for e in events if isinstance(e.event, TurnCompleted)])


asyncio.run(main())
```

```text
True denied by hook: the command recursively deletes '/' with `rm -r`, which is the filesystem root. ShellGuard refused to run it — do not retry it, take a narrower approach or ask the user to run it themselves
False ok
started: ['c1', 'c2']
['completed']
```

The denied call still records a `ToolCallStarted` before its error `ToolCallCompleted` — the pair is what a reader replays; `is_error` is what says the tool never ran — and the turn carried on.

Swap `ShellGuard()` for `ShellGuard(on_trip="ask")` and the same command suspends the turn with the guard's reason in the approval body instead.

## Next

- [Permissions & hooks](permissions-hooks.md) — where `Denial` and `Escalation` fit.
- [Extra tools reference](../reference/extratools.md).

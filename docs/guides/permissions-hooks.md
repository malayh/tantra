# Permissions and hooks

Each tool call resolves to `allow`, `ask`, or `deny`. The longest matching rule on the current actor's `Agent` wins; equal-length matches use the stricter verdict. A matching agent rule may widen or narrow the tool declaration. With no match, the tool's declared permission applies, then `Runtime(default_permission=...)`.

An `ask` verdict on the root appends `AskRaised` and suspends the live root turn. The current writable root connection answers it:

```python
await connection.answer(ask_id, ApprovalResponse(allow=True), command_id=uuid4())
```

A denied call becomes an error result the model can observe. Invalid verdicts fail during Runtime construction.

Child agents cannot use approval-gated tools. Runtime rejects a statically configured child tool whose effective permission is `ask`. A hook or other dynamic policy that escalates a child call to `ask` returns an error without raising a human ask; the child should use `send()` to report the blocker to its parent.

Hooks receive turn, sample, tool, and event lifecycle callbacks. Pass instances with `Runtime(..., hooks=[AuditHook()])`. A `before_tool` hook may transform the validated call, return `Denial`, or return `Escalation`; sibling calls still follow their own outcomes.

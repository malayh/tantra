# Typed asks

Tools call `await ctx.ask(request)` with `Approval`, `Choice`, or `FreeText`. Runtime appends `AskRaised` and suspends that live actor turn.

The current writable root connection answers a root or descendant ask:

```python
await connection.answer(
    ask_id,
    FreeTextResponse(text="release/2.1"),
    command_id=uuid4(),
)
```

The response kind must match the request. Permission asks require `ApprovalResponse`. An unknown, already answered, interrupted, or process-lost ask raises `AskExpired`.

Ordinary messages do not answer typed asks. Inputs accepted while an actor is asking remain behind the active turn in FIFO order.

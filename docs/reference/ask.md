# Typed asks

Root tools call `await ctx.ask(request)` with `Approval`, `Choice`, or `FreeText`. Runtime appends `AskRaised` and suspends that live root turn. Child asks are rejected as ordered tool errors and emit no `AskRaised`; children communicate with their direct parent through `send()`.

The current writable root connection answers the root ask:

```python
await connection.answer(
    ask_id,
    FreeTextResponse(text="release/2.1"),
    command_id=uuid4(),
)
```

The response kind must match the request. Permission asks require `ApprovalResponse`. An unknown, already answered, interrupted, or process-lost ask raises `AskExpired`.

Ordinary messages do not answer typed asks. Inputs accepted while the root is asking remain behind the active turn in FIFO order.

# Known limits

- One root tree must be active in one Runtime process. Shared storage does not provide distributed execution ownership.
- Typed asks are live futures and expire after process loss.
- Sync-tool cancellation cannot stop an operating-system thread or reverse an external side effect.
- Actor journals grow without retention or pruning in 1.0.
- Breadth is uncapped; `Runtime.max_depth` limits only recursion depth.
- Delivery across actor journals can survive in the recipient even when the sender turn is interrupted before recording its own completion.

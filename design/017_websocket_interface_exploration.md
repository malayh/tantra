# 017 — WebSocket interface exploration

**Status:** provisional reference draft. No transport implementation is scheduled or authorized by this document.

## Goal

Capture the evidence and constraints for an optional Tantra WebSocket interface. Refine it through future real applications before defining a public contract. The interface sketches below are hypotheses, not APIs available in Tantra 1.4.

## Scope

**In:** Sarathi/Osuite findings, shared transport responsibilities, application integration points, illustrative interfaces, and questions to revisit.

**Out:** extracting either bridge, introducing dependencies, changing authentication, publishing a client SDK, or scheduling implementation phases beyond this reference draft.

## Decisions and constraints

- Wrap existing Runtime operations: `connect`, `events`, `Connection.send`, `answer`, and `cancel`. Keep replay, recovery, command identity, fencing, and commit-before-delivery in Runtime rather than duplicating them in the adapter.
- Applications own authentication, session access, and tool permissions. A writer token controls concurrency; it does not grant application authorization.
- Preserve application input validation and immutable command payloads. Retrying an editor command must reuse its original snapshot rather than capturing a new draft.
- Preserve actor/sequence cursors through event filtering. Replaying history reconstructs state; it must not reopen completed proposals or answered approvals.
- Preserve accepted work across socket disconnects. Turn/tool authorization remains necessary because execution can outlive the authenticated connection.
- Keep server dependencies optional and bound subscriptions, pending commands, and outbound buffering. A slow client must not block unrelated clients.
- Judge extraction by the duplicated machinery it removes from real applications. Callbacks that recreate the bridge indicate a poor boundary.
- Another agent class alone provides little transport evidence. Use a genuinely different application integration when a real requirement arises.

## Current evidence

| Source | Shared mechanisms | Application behavior |
|---|---|---|
| [Sarathi bridge](../apps/sarathi/backend/src/sarathi/api/ws.py) | Subscription tasks, replay readiness, writer handling, commands, errors, cleanup | Attachments, child subscriptions, typed asks, title/header updates |
| [Osuite bridge](../../observability_ui/backend/app/api/agents.py) | Root subscription, replay readiness, writer handling, commands, errors, cleanup | Dashboard authorization, editor snapshots, proposal/display rules |

- Sarathi's [socket tests](../apps/sarathi/backend/tests/test_ws.py) cover cursor replay, writer takeover, read-only views, descendants, asks, slow clients, and execution after disconnect. Osuite's [API tests](../../observability_ui/backend/app/tests/api/test_dashboard_agent.py) cover permission rechecks, deleted sessions, and two-runtime proposal/replay behavior.
- The wire formats already differ: Sarathi supports `unsubscribe` and `ask_response`; Osuite restricts subscriptions to the root and includes `writable` in readiness frames. Extracting code is not automatically wire-compatible.
- Both clients send credentials in query strings. No explicit Origin check was found in the inspected routes; ordinary CORS middleware does not authorize WebSocket handshakes. A future adapter needs an explicit security contract rather than silently inheriting these choices.
- Osuite's [history service](../../observability_ui/backend/app/services/dashboard_history.py) projects the last 20 visible messages and hides skill bodies. Its client hydrates at a watermark before streaming the suffix. Generic event replay alone does not solve message-history loading or proposal eligibility.
- Both apps use similar Runtime flows. They establish a shared-mechanism opportunity, not a universal application model.

## Possible integration shape

This sketch tests a boundary; names, signatures, dependencies, and callback count remain open. Authentication runs in application code. Header and actor identities supplied to policy checks must come from authoritative server state.

```python
endpoint = SessionSocket(
    runtime,
    authorize=authorize,
    message_schema=DashboardMessage,
    encode_input=encode_input,
    event_view_factory=DashboardEventView,
    allowed_origins=application_origins,
)

await endpoint.serve(
    websocket,
    root_id=root_id,
    principal=identity.user,
    expires_at=identity.expires_at,
)
```

| Candidate integration point | Application responsibility |
|---|---|
| Authentication before serving | Validate cookie/ticket/token and provide trusted identity and expiry |
| Authorization | Check read access, writer acquisition, send, answer, and cancel; handle changed permissions |
| Message validation and encoding | Validate dashboard context or attachments and prepare stable durable input |
| Subscription-local event projection | Hide application content while preserving cursor advancement |

Potential shared server behavior includes subscriptions, caught-up state, command receipts, typed answers, stable errors, writer replacement, and cleanup. Potential browser behavior includes cursor tracking, exact-command outboxes, reconnects, and retries. Durable acceptance and turn completion are separate states; an uncertain timeout must retain the original command ID.

A possible browser call shape is:

```typescript
await chat.connect({ after: history.watermark, writable: false });
await chat.takeWriter();
const receipt = await chat.send(frozenMessage);
await chat.answer(askId, { kind: "approval", allow: true });
await chat.cancel();
```

Candidate companion contracts are versioned frames/types, connection and actor status, bounded history snapshots with watermarks and pending asks, and sanitized diagnostics. Each requires evidence before standardization. Cookie or connection-ticket authentication, a generic `Access` object, event-view factories, and a common history reducer are not committed design choices.

## Considered & rejected

- **Finalize a universal adapter now:** the two existing applications have similar integration patterns, so callback shapes and generality remain unproven.
- **Build artificial applications for evidence:** spend the effort on real application requirements instead.
- **Standardize identity or chat UI with the transport:** ownership, permissions, visible history, and actions differ between applications.
- **Extract immediately:** the user chose a reference draft for future work.

## Implementation phases

### P0 — Reference draft · deps: none · ✅ DONE (documentation only)

**Deliver**
- Record existing findings, preservation constraints, and illustrative interfaces in this document.
- Keep public API choices open; record revisit criteria without scheduling an extraction.

**Verify**
- Check current-code claims and source links against the inspected bridges and tests.
- Confirm examples are clearly hypothetical and open choices are not presented as frozen contracts.
- Confirm this work changes only the document, with no code, dependency, or application changes.

**Checklist**
- [x] Findings and constraints
- [x] Illustrative integration
- [x] Open decisions and revisit criteria

### Conventions

- This is documentation only; no runtime test campaign or independent code review is required.
- Future code work follows repository guidelines: Ponytail full, no code comments, focused verification, `just lint`, relevant tests, and one independent review per requested code phase.
- **Contract freeze:** preservation constraints only. Package layout, wire protocol, and public interfaces remain provisional.

### Keeping this spec current

- Update the phase marker and checklist after document verification.
- Add evidence when a real application exposes a requirement; distinguish observations from candidate design.
- Record only material changes or surprises, not routine implementation history.
- Before implementation, resolve the relevant open decisions and replace provisional sections with a decision-complete plan. Implement only a separately requested phase.

## Open Decisions

Defer package layout/framework dependency, policy and callback shapes, wire versioning, credential transport and revocation, browser client scope, history projection, event visibility, flow control, and compatibility rollout. No default authorizes implementing any of these.

Revisit when a real application needs integration. Compare its requirements against both existing bridges, identify repeated mechanisms, and test the smallest shared boundary. Resolve only the decisions required by that work; leave unsupported generality open.

## Risks

- Similar applications can make an overly narrow interface appear universal.
- Excessive callbacks can relocate duplication instead of removing it.
- Raw events can expose tool arguments/results, skill bodies, and application context.
- Public wire types create compatibility obligations across independently deployed clients and servers.
- Replay, projection, and revocation mistakes can expose content or make historical actions appear live.

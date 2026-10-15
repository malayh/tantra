# 018 — Upstream post drafts

- One draft per thread in `agentclientprotocol/agent-client-protocol`, each 150 words or fewer.
- Threads re-read 2026-10-15. The user posts; nothing here has been posted.
- Link target: `docs/acp/tantra-profile.md`, live once this branch is merged.

## #2208 — durable & resumable transport

- **State:** open RFD PR. Post as a PR comment.

The RFD replays from a bounded in-memory buffer of messages per stream, resumes on the same `Acp-Connection-Id`, and falls back to `session/resume` after a restart, a retention miss or another backend. It notes "a server is free to derive cursors from whatever it already tracks".

A data point from a server-run harness that journals every session: derive the cursor from the journal position and the second layer goes away. No buffer, no retention window, no affinity; any worker serves any resume.

```json
{"replayFrom": {"type": "_tantra_after", "cursor": "12.3"}}
```

Every journal-derived update carries `_meta.tantra.cursor`, and resume is exclusive, like `Last-Event-ID`. Tests cut a stream at every output and check that the resumed stream equals the uninterrupted one, also on a second runtime. An e2e run kills the owning backend; the client resumes from its cursor on the other.

Spec as implemented: https://github.com/malayh/tantra/blob/main/docs/acp/tantra-profile.md

## #2394 — a session's last change

- **State:** open RFD PR. Post as a PR comment.

An honest mismatch first: our `updatedAt` moves on every journal append and header patch, including model changes, so we don't meet part 1 today.

We argue clients may not need the comparison. Our cursor is a journal position, `<seq>.<index>`, on every journal-derived update. A client resumes with its last cursor; an empty replay means nothing changed, and otherwise it receives exactly the gap. We avoid the 27,765-update replay that way.

```json
{"type": "_tantra_after", "cursor": "12.3"}
```

It is exclusive, so nothing is re-sent or upserted, and it can sit between non-message updates. Compaction appends to the journal, so it never invalidates a cursor; a message cursor fails once compaction or rewind dropped its message.

Trade-off: the cursor is opaque and tied to the server's projection, the persistence question your FAQ raises.

Spec: https://github.com/malayh/tantra/blob/main/docs/acp/tantra-profile.md

## #2286 — current `state_update` after resume

- **State:** open issue. Post as an issue comment.

+1 to making it required. A data point from a server-run harness with shared, durable sessions: we send it on every resume, with and without `replayFrom`, and it is what lets clients tell "still running" from "ended while away".

After the `session/resume` response come `session_info_update`, then the `state_update` snapshot, then live updates. When idle, the snapshot carries the last stop reason; when blocked, `requires_action` with the ask id. Profile clients also get the queue and the subagent mirror before live traffic; a writer's pending permission requests follow and may interleave with it.

```json
{"sessionUpdate": "state_update", "state": "idle", "stopReason": "end_turn"}
```

Profile clients can tell the snapshot from a replayed state change because it carries no replay cursor.

Tests: `test_resume_without_replay_sends_only_the_current_snapshot`, `test_resume_in_write_mode_claims_before_responding_and_issues_asks_after`.

Spec: https://github.com/malayh/tantra/blob/main/docs/acp/tantra-profile.md

## #954 / #2175 — correlation token

- **State:** both closed (merged). Post as a new discussion: "A client correlation token for lost prompt responses", linking #954 and #2175.

#954 and #2175 leave a lost prompt response ambiguous and defer a client token, kept "separate from letting the client choose the canonical `messageId`".

Our profile adds a client UUID, `_meta.tantra.commandId`. A retry with the same id and content returns the same `messageId`; different content returns `-32602` with `command_conflict`. The dedupe record is the journal, so it holds on any worker and across restarts.

```json
{"_meta": {"tantra": {"commandId": "3d9e5b7a-1f2c-4d6e-8a9b-7c5d3e1f2a4b"}}}
```

We knowingly diverge from the Message ID RFD's "Only the Agent generates protocol message IDs": the `commandId` is also the `messageId`. One submission is one turn for us, so a separate token would map one-to-one onto the id; merging them drops the mapping. Plain clients keep agent ids.

Would a standard token, with or without that merge, be welcome in v2?

Spec: https://github.com/malayh/tantra/blob/main/docs/acp/tantra-profile.md

## #533 — multi-client session attach

- **State:** open RFD PR. Post as a PR comment.

Removing roles to avoid per-event filtering makes sense. We took the other side of that trade with one rule: notifications go to every connection, agent→client requests only to the current writer.

The writer is fenced, one per root, and claimed explicitly. A claim fences the old writer, cancels its open requests with `$/cancel_request`, and re-issues them to the new one. Readers see `requires_action` with the ask id, never the request.

```json
{"sessionUpdate": "_tantra_writer", "writer": false, "reason": "replaced"}
```

The cost lands on your core case: approving from a phone means claiming the writer, which fences the terminal until it reclaims. The turn keeps running. First-writer-wins avoids that.

It runs in the agent server, with no proxy. Tested across two runtimes on PostgreSQL and end to end across two backends behind Nginx.

Spec: https://github.com/malayh/tantra/blob/main/docs/acp/tantra-profile.md

## #1992 — subagents

- **State:** closed (merged 2026-09-30). Post as a new discussion: "Selective subagent delivery", linking #1992.

#1992 keeps automatic delivery and calls selective subscription worth revisiting, if hiding a transcript strands no interaction, keeps an unobserved child from looking stopped, and defines catch-up.

Our profile mirrors every descendant as `subagent_update {sessionId, title, state}` on its parent's id, so announcements and state always flow. A child's transcript flows only after `session/resume` on it, read-only, and stops on `session/close`; the child keeps running. Catch-up is a cursor or `start` replay plus a `state_update` snapshot.

```json
{"sessionUpdate": "subagent_update", "sessionId": "8a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d", "title": "scout", "state": {"state": "running"}}
```

One caveat: nothing strands for us only because our children cannot ask. A general design would still have to deliver child permission and elicitation requests while the transcript is hidden.

Tests: `test_children_are_mirrored_and_delivered_only_when_resumed`.

Spec: https://github.com/malayh/tantra/blob/main/docs/acp/tantra-profile.md

## #1261 — `session/inject`

- **State:** open RFD PR. Post as a PR comment.

A data point on the shared-queue question. The RFD adds no pending-queue list, but multi-client surfaces need one. Our profile lists every accepted root input that has not started in `_tantra_queue_update`, with its submitter, sent in full after each resume and on every change.

```json
{"sessionUpdate": "_tantra_queue_update", "entries": [{"commandId": "4c8d2e6f-0a1b-4c3d-9e5f-6a7b8c9d0e1f", "content": [{"type": "text", "text": "Then draft a reply."}], "submittedBy": "user-1"}]}
```

`session/prompt` while busy is our queue mode. It answers at durable acceptance with a `messageId`, and `user_message` appears at insertion under the same id, your delivery signal. We have no steer, revoke or replace, and unlike your recommendation, `session/cancel` drops queued inputs too.

Tests: `test_queue_updates_follow_the_runtime_queue`, `test_an_input_cancelled_while_queued_leaves_the_queue`.

Spec: https://github.com/malayh/tantra/blob/main/docs/acp/tantra-profile.md

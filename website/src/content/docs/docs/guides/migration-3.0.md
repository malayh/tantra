---
title: Migrate to 3.0
slug: docs/guides/migration-3.0
---

Tantra 3.0 removes the `finish` tool. Every completed child turn now delivers the child's final assistant message to its direct parent as a reply, so a parent never waits on a result that a child wrote but did not hand over. Journal bodies and sequences are unchanged, and no store has a new migration.

## What changes for models

- Child agents no longer get `finish`. A child delivers its result by ending its turn: the final assistant message becomes `[agent <child-uuid> replied] <text>` in the parent's queue. A child with an `output_schema` replies with the canonical JSON of its `submit_output`.
- The child stays reusable after every reply. The parent sends more work with `send` and stops work with `cancel_agent`. Nothing closes a child.
- `max_steps`, failed, cancelled and interrupted turns still queue `[agent <child-uuid> turn ended] <stable-json>`. A cancel by the parent still queues nothing.
- The `spawn`, `status` and `cancel_agent` descriptions and the child guidance say this. Remove prompt text that tells children to call `finish` or tells parents to wait for finished messages.

## What changes for applications

- A parent now receives child text. A `view` that hides child sessions still withholds their replies over ACP. Check any other path that shows a parent's inputs to people who must not see child output.
- A mid-level child may reply while its own children still run, and it replies again when they wake it. Use `tree_status` or `watch_tree` for the state of the whole tree.
- Children never reach the `finished` status in new sessions. They are `idle` after a reply. UIs that label `_tantra_finished` as done should treat an idle child with `end_turn` as replied. A child from a 2.x session that ended a plain turn also shows `end_turn`, though its parent got only a status notice.
- Replies over 32 KiB of UTF-8 are cut at a character boundary and end with `[reply truncated at 32 KiB]`. An empty final message becomes `(no text reply)`.
- `finish` remains a reserved tool name for child agents.
- Scripted test providers that call `finish` must end the child's turn with text instead.

## ACP and the TypeScript client

- A reply projects as an incoming `session_message` from the child with `_meta.tantra.reply: true`, on the parent's session. On the child's session it is the child's final `agent_message`, or its `submit_output` call.
- `tantra-acp-client` 2.1.0 adds `MessageEntry.reply`. The profile version stays 1, so older clients render replies as plain messages.

## Existing data

- 2.x journals load unchanged. A child that finished under 2.x stays closed, still rejects `send`, and its `[agent X finished]` input still projects with `_meta.tantra.finished`.
- A completed child turn that a 2.x Runtime already notified is not delivered again. The notice reuses the same command ID, and the 2.x status-only text counts as delivered.

## Deploy

Stop every 2.x worker before starting 3.0 workers. A mixed fleet works but behaves inconsistently: 2.x Runtimes still give children `finish`, and a 2.x ACP server hides replies.

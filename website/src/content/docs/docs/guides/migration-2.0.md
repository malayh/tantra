---
title: Migrate to 2.0
slug: docs/guides/migration-2.0
---

Tantra 2.0 adds `tantra.acp`, which serves a Runtime to any ACP client over an ACP v2–inspired protocol plus the `tantra` profile, and `tantra-acp-client`, its TypeScript client. It also adds `Runtime.watch_tree`, `Runtime.patch_header`, `Runtime.lookup_ask` and input attribution. Journal bodies and sequences are unchanged. PostgreSQL applies migrations 12 and 13; SQLite, memory and filesystem storage formats have no new migration.

## Upgrade PostgreSQL

Every worker must run 2.0 before migration 12 is applied. On the migrated schema an older worker writes no `journal_index` row for `AskRaised`, and `PostgresStore.lookup_ask` has no fallback, so `tantra.acp` never issues those asks.

1. Stop every application worker and other writer using the database. Mixed-version writers are unsupported during migration.
2. Install `tantra-harness[postgres]==2.0.0` in the application environment.
3. Run setup once against the existing database and schema before starting Runtime:

   ```python
   from tantra import PostgresStore

   store = PostgresStore(DATABASE_URL, schema=EXISTING_SCHEMA)
   await store.setup()
   await store.close()
   ```

4. Start only workers running 2.0. Preserve the startup order: Store setup, then `await runtime.start()`. Fail readiness if either operation fails.

Setup applies pending migrations through version 13.

- **Migration 12** adds `journal_index.ask_id` and the partial index `journal_ask_idx`, then backfills ask entries by decoding every stored event in pages of 1,000. It runs in the setup transaction and holds an exclusive lock on `journal_index` until it commits, so its time grows with total journal size and every append and indexed read waits for it. Allow a maintenance window.
- **Migration 13** only adds the partial index `coordinator_changes_tree_idx`. It is built in the setup transaction, so change inserts wait for it briefly. Older workers ignore it.

Corrupt rows or migration failures roll back, and the schema version is published only after completion. Pending migrations from earlier releases also run, in order; see the [1.4](/tantra/docs/guides/migration-1.4/) and [1.3](/tantra/docs/guides/migration-1.3/) guides.

## Coordinated deployments

2.0 records `sender_id` and `origin` on `InputQueued`. A retry compared with an event written before `origin` existed ignores both fields, so recovery and command retries over older journals stay duplicates. An older owner rejects retries of inputs that carry the new fields: drain older coordinated workers before rolling out 2.0.

`CoordinatedStore` now publishes a `header` change when an append changes an actor's status, current turn or last turn.

## Input attribution

- `InputQueued.origin` is `user`, `spawn`, `message`, `finished` or `turn_ended`, and agent inputs carry the sending actor's journal id in `sender_id`. The input text and its prefixes are unchanged. Events written before these fields existed read both as None. See [Events](/tantra/docs/reference/events/).
- `Connection.send`, `prompt`, `answer` and `cancel` accept `submitted_by`, a string of 1–256 characters. It is recorded on `InputQueued`, `AskAnswered` and `CancellationRequested`, and turns expose it as `TurnContext.submitted_by` and tool `Context.submitted_by`. It grants no access.

## Tree state and headers

- `Runtime.watch_tree(root_id)` yields `TreeChange` snapshots, the root's title and model plus every actor status, on each tree change without reading journals. Applications that poll `tree_status()` can follow it instead.
- Route application title and model changes through `Runtime.patch_header(agent_id, title=..., model=...)`. It needs no execution ownership, so any Runtime may call it, and it signals tree watchers before it returns. A direct `store.patch_header` reaches watchers only through a coordinator. On PostgreSQL, a model change raises `ModelChangeBusy` while the tree has work; a title-only patch never does.

See [Runtime & Connection](/tantra/docs/reference/runtime/).

## Serve clients over ACP

Applications that stream a Runtime to their own clients over an application-owned socket can serve it with `tantra.acp` instead. `AcpServer(runtime, *, agent, scope, policy, ...)` offers `websocket(*, bind, allowed_origins, allow_missing_origin=False)`, an ASGI `AcpWebSocket`, plus `serve_stdio(auth)` and `connect(transport, auth)`. The `tantra` profile adds journal cursors, client command IDs, one fenced writer per root, lazy child transcripts and a shared input queue. `tantra-acp-client` is the TypeScript client for the WebSocket endpoint.

- On shutdown, call `await endpoint.aclose()` before `await runtime.aclose()`.
- `Runtime.lookup_ask(root_id, ask_id)` returns the original `AskRaised` as a `LocatedAsk`, and the `answer` policy check receives it in `AcpAccess.ask`. Never trust approval details supplied by a client.

See the [ACP server reference](/tantra/docs/reference/acp/) and the [`tantra` profile specification](/tantra/docs/acp/tantra-profile/).

## Custom stores

- `tantra.testing.store_conformance` now requires a store to detect duplicate inputs by comparing events with `==`, so that a legacy `InputQueued` equals its structured retry. Custom stores that compare serialized events fail it.
- `lookup_ask(root_id, ask_id)` is an optional store capability. Runtime falls back to reading history when it is absent; required Store and Coordinator protocols are unchanged.

## Behavior changes

- Tool approval asks carry the call's effective arguments in `extra["arguments"]`.
- `bash` runs commands with stdin set to `/dev/null`.
- Tool call IDs are unique within a session. A provider ID already used in the turn or the loaded history, including the `OpenAICompatible` fallback `call_{index}`, is rewritten to a deterministic `call_<24 hex>` before it is journaled. IDs without a collision are unchanged.
- `pg_notify` change payloads are now built by PostgreSQL: the same keys, with different whitespace.

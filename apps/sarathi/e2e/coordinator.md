# Coordinator failure verification

This runbook exercises the normal Sarathi auth, WebSocket, Runtime, Store, and UI against a deterministic external OpenAI-compatible provider and a gated test tool. The controls exist only in the isolated Compose override; Sarathi exposes no fault-injection HTTP routes.

## Start and reset

Create `apps/sarathi/.env` as usual so secrets and database settings exist, then run from `apps/sarathi/`:

```bash
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml up --build -d
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml ps
```

Use `http://localhost:13001` in Brave. Nginx is `http://localhost:18000`, backend A is `http://localhost:18001`, backend B is `http://localhost:18002`, and the gate control API is `http://localhost:18090`. The override forces model `e2e-model`, disables embeddings, and uses a six-second coordinator lease unless `E2E_COORDINATOR_LEASE_TTL` overrides it.

The project name, database, uploads, and network are disposable and isolated. Verify the project name before the destructive reset:

```bash
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml down -v
```

Never run `down -v` against the default project. Restore every stopped or paused service before leaving the stack.

## Gate controls

A gate starts open. Close it before sending a matching prompt, wait until `/control/state` reports a waiter, then open it. Reset releases all current waiters and clears request evidence.

```bash
curl -fsS -X POST http://localhost:18090/control/reset
curl -fsS -X POST http://localhost:18090/control/gates/model/close
curl -fsS http://localhost:18090/control/state
curl -fsS -X POST http://localhost:18090/control/gates/model/open
curl -fsS -X POST http://localhost:18090/control/command-replies/drop-next-send
```

| Prompt marker | Deterministic behavior |
|---|---|
| `[gate:provider=model]` | Provider request waits at `model`, then streams a fixed answer. |
| `[gate:tool=io]` | Model calls the opt-in `e2e_gate` tool, which waits at `io`. |
| `[gate:ask=approval]` | Model calls `memory_write`, producing the real approval card. |
| `[gate:child=worker]` | Root spawns `Gate worker`; the child provider waits at `worker`, then finishes. |
| `[gate:child-tool=worker]` | Child calls the gated tool and finishes after release. |
| `[gate:drop=stream]` | Provider waits at `stream`, emits partial text, then drops its SSE response. |

Markers may be surrounded by ordinary text. Close `io`, `worker`, or `stream` before sending those prompts. `[gate:drop=stream]` tests provider-stream loss only. The command-reply control arms one successful `send`: the E2E coordinator waits for the durable reply, records its transport and command UUIDs in `/control/state`, then raises a timeout to its caller. It never affects the production coordinator.

## Evidence

Write screenshots and notes under the ignored directory `apps/sarathi/e2e/reports/011-coordinator-2026-09-27/`. For each case record the root ID from the URL, command UUIDs and journal cursors from WebSocket frames, the gate request ID/source, ownership generation before and after, Nginx upstream, and the final journal rows.

Backend Uvicorn access logs are disabled because WebSocket URLs contain authentication query parameters. Use the sanitized Nginx access log for backend routing evidence.

Gate sources identify which container made each provider request. Map them to containers with `docker inspect`; close a WebSocket before reading its completed Nginx access-log line. Direct probes can force A or B through ports 18001 and 18002. Brave UI traffic must continue through Nginx on 18000.

```bash
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml logs --tail=200 nginx
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml ps -q backend_a)
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml ps -q backend_b)
```

Replace `<root>` in these evidence queries:

```bash
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml exec -T db psql -U sarathi -d sarathi -P pager=off -c "SELECT root_id, owner_instance, generation, expires_at, writer_connection, recovery FROM tantra.coordinator_roots WHERE root_id = '<root>';"
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml exec -T db psql -U sarathi -d sarathi -P pager=off -c "SELECT actor_id, active, updated_at FROM tantra.coordinator_activity WHERE root_id = '<root>' ORDER BY actor_id;"
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml exec -T db psql -U sarathi -d sarathi -P pager=off -c "SELECT session_id, seq, convert_from(decode(stamped #>> '{tantra_stamped,payload}', 'base64'), 'UTF8')::jsonb AS event FROM tantra.events WHERE session_id IN (SELECT id FROM tantra.sessions WHERE id = '<root>' OR header->>'root_id' = '<root>') ORDER BY session_id, seq;"
```

To inject a real missed PostgreSQL notification, first hold the owner on a provider gate and identify the observer backend's container IP. Pause that observer so it cannot reconnect, then terminate only its dedicated `LISTEN` connection in the disposable database:

```bash
OBSERVER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml ps -q backend_b))
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml pause backend_b
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml exec -T db psql -U sarathi -d sarathi -v observer_ip="$OBSERVER_IP" -P pager=off -c "SELECT pid, client_addr, query, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'sarathi' AND backend_type = 'client backend' AND client_addr = :'observer_ip'::inet AND query ~ '^LISTEN ';"
curl -fsS -X POST http://localhost:18090/control/gates/model/open
docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml unpause backend_b
```

Confirm the termination query returned exactly one row. The owner must publish the gated journal change while B is paused. After unpausing B, verify its viewer advances from the old scalar cursor through the durable page without duplicates; that proves catch-up rather than notification delivery. Restore B even if an assertion fails.

## Deterministic matrix

Use three Brave windows on the same session: writer A, writer B, and a viewer opened with `?view=readonly`. Nginx round-robins them; confirm actual routing from its logs. Run each destructive case on a fresh chat, and reset the gate between cases.

1. **Reconnect through B while A works:** close `model`, send `[gate:provider=model]`, close or reload writer A, reconnect a writer routed to B, open `model`, and verify one turn completes with no duplicate UUID or sequence.
2. **Writer takeover and viewer:** open writer B and confirm writer A closes with 4009/reload banner. Open the read-only URL and confirm it observes without replacing B or enabling mutations.
3. **Active child while root idle:** close `worker`, send `[gate:child=worker]`, wait for the root turn to become idle while the child stays active, disconnect all writers, and verify ownership/activity remain. Open `worker` and refresh the child drawer.
4. **FIFO while working:** close `model`, send `[gate:provider=model]`, enqueue a second UUID with a direct authenticated WebSocket probe, open `model`, and verify journal acceptance and execution order.
5. **Remote answer and Stop:** route the owner to A, take writer authority through B, answer `[gate:ask=approval]` from B, then repeat with a closed provider/tool gate and Stop from B. Release the gate and verify the durable terminal.
6. **Committed reply loss:** arm `drop-next-send`, submit a fixed command UUID through non-owner B, observe `command_timeout`, then resend the identical frame UUID. `/control/state` must show one dropped transport reply and the journal must contain exactly one matching `input_queued`.
7. **Notification catch-up:** hold the owner on `model`, terminate the paused observer's exact `LISTEN` connection with the procedure above, publish while it is absent, then unpause it. Verify the viewer catches up from its old cursor without duplicates.
8. **Backend B death:** with A owning a closed gate, stop B and verify A continues. Restore B and reconnect through it before the next case.
9. **Owner A death and expiry:** kill A while `[gate:provider=model]` waits. Before six seconds, B must not start replacement work. After expiry, claim from B and verify the abandoned started turn is interrupted rather than replayed.
10. **Paused stale owner:** pause A at a provider gate, wait past expiry, claim/recover through B, unpause A, then open the gate. Verify A cannot append and the ownership generation increased once.
11. **Ask expiry:** leave `[gate:ask=approval]` unanswered, kill its owner, wait past expiry, reconnect through the other backend, and verify the old ask is expired and cannot be answered.
12. **Cancellation acceptance crash boundary:** with `[gate:tool=io]` blocked, run one crash before cancellation acceptance and one after. Before acceptance there is no `cancellation_requested`; after acceptance the request, frozen targets, and matching turn terminals commit atomically. Verify no journal state has persisted targets without its terminals, and do not expect recovery to repair such a partial state.
13. **Database outage:** stop `db` while a viewer and writer are connected. Unavailability must not render as inactive. Start `db`, wait for health, restore backends if needed, and verify replay/catch-up.
14. **Slow reader:** throttle or suspend one read-only page while turns complete elsewhere, resume it, and verify scalar cursors catch up without blocking the owner.
15. **Graceful shutdown:** stop the owner with Compose rather than kill/pause and verify it writes interruption terminals, clears activity, and releases ownership immediately. The other backend must claim without waiting for lease expiry.
16. **Child refresh:** complete `[gate:child=worker]`, refresh, open the child drawer, and verify its independent journal and display name.
17. **Cross-backend upload:** upload through one backend, read through the other, and verify the shared path/content without using the user's default uploads volume.

For kill/pause cases identify the current owner from the gate source, activity, and recent Nginx evidence before acting. If placement is ambiguous, stop and rerun the fresh case rather than guessing.

## Real-provider smoke

The deterministic override is not a substitute for the existing real-provider runbook. After this matrix, use `e2e/runbook.md` against the normal stack for streaming, delegation, child communication, drawer history, approval, cancellation, refresh, reconnect, and upload behavior. Missing external credentials or services leaves verification pending; it is not a deterministic-gate pass.

## Failure interpretation

- A provider or tool gate with a waiter is intentionally blocked. No waiter means the marker was not selected or the gate was closed too late.
- A provider timeout or dropped SSE body that exhausts retries must end in a durable `turn_failed`; a successful retry may complete normally. Inactive activity with only nonterminal turn events is a failure.
- A stopped database is an availability failure; never report `active=false` as the expected result.
- A process killed after `turn_started` must yield interruption on takeover and must not replay model/tool work.
- A paused stale process may finish an external request, but fencing must reject its later durable writes.
- A timed-out transport command has unknown acceptance. Retry only its original UUID.
- Restore with `unpause` or `start`, wait for health, and call `/control/reset` after every destructive case.

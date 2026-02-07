# LakeSync Todo App

A minimal reference implementation demonstrating the LakeSync sync pipeline with offline-first SQLite storage and automatic column-level delta extraction.

## Quick Start

```bash
# From the repository root
bun install

# Start the dev server (local mode — in-process gateway, no external services needed)
cd apps/examples/todo-app
bun run dev
```

Open http://localhost:5173

### Remote gateway mode

To connect to a deployed gateway instead of the in-process one:

```bash
VITE_GATEWAY_URL=https://your-gateway.workers.dev bun run dev
```

## How It Works

1. **Add a todo** -- creates an INSERT delta
2. **Toggle completed** -- creates an UPDATE delta (only the `completed` column)
3. **Delete a todo** -- creates a DELETE delta

Each change:
- Is applied to a local SQLite database (sql.js with IndexedDB persistence)
- Has a column-level delta automatically extracted by `SyncTracker`
- Is queued in the `IDBQueue` for eventual delivery
- Is pushed to the gateway (local or remote) on the next sync cycle
- The gateway resolves conflicts via column-level LWW and buffers deltas

## Architecture

```
User Action -> SyncCoordinator.tracker (SyncTracker)
                  -> LocalDB (sql.js + IDB)
                  -> extractDelta() -> IDBQueue
                  -> SyncTransport -> SyncGateway
```

### Transport modes

- **Local mode** (default): `LocalTransport` wraps an in-process `SyncGateway` -- no network calls, everything runs in the browser tab
- **Remote mode** (`VITE_GATEWAY_URL`): `HttpTransport` connects to a deployed gateway-worker via HTTP with JWT authentication

## Key files

| File | Purpose |
|------|---------|
| `src/main.ts` | Entry point: initialises DB, transport, coordinator, UI |
| `src/db.ts` | Opens `LocalDB` and registers the todo schema |
| `src/auth.ts` | Dev JWT helper for remote gateway mode |
| `src/ui.ts` | Vanilla TS UI wired to `SyncCoordinator` |

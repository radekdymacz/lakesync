# @lakesync/client

Client SDK for LakeSync -- offline-first sync coordination with local SQLite storage, automatic column-level delta extraction, and pluggable transports for communicating with a remote or in-process gateway. Deltas are queued locally, pushed/pulled via the transport layer, and applied to the local database with conflict resolution.

## Install

```bash
bun add @lakesync/client
```

Peer dependencies: `@lakesync/core`, `sql.js`

## Quick usage

### Full sync setup

```ts
import { LocalDB, SyncCoordinator, HttpTransport } from "@lakesync/client";

// Open a local SQLite database (persisted to IndexedDB)
const dbResult = await LocalDB.open({ name: "my-app", backend: "idb" });
const db = dbResult.ok ? dbResult.value : throw dbResult.error;

// Connect to a remote gateway
const transport = new HttpTransport({
  baseUrl: "https://your-gateway.workers.dev",
  gatewayId: "my-gateway",
  token: "your-jwt-token",
});

// Create the coordinator (wires up tracker, queue, and HLC automatically)
const coordinator = new SyncCoordinator(db, transport);
coordinator.startAutoSync(); // push/pull every 10s + on tab focus

// Track mutations — deltas are extracted and queued automatically
await coordinator.tracker.insert("todos", "row-1", {
  title: "Buy milk",
  completed: 0,
});

await coordinator.tracker.update("todos", "row-1", { completed: 1 });

await coordinator.tracker.delete("todos", "row-1");

// Query the local database
const rows = await coordinator.tracker.query("SELECT * FROM todos");
```

### LocalDB -- SQLite WASM with IndexedDB persistence

```ts
import { LocalDB } from "@lakesync/client";

const result = await LocalDB.open({ name: "my-app", backend: "idb" });
if (result.ok) {
  const db = result.value;

  // Execute statements
  await db.exec("CREATE TABLE IF NOT EXISTS todos (_rowId TEXT PRIMARY KEY, title TEXT)");

  // Query with typed results
  const rows = await db.query<{ _rowId: string; title: string }>("SELECT * FROM todos");

  // Run a transaction
  await db.transaction((tx) => {
    tx.exec("INSERT INTO todos VALUES (?, ?)", ["row-1", "Buy milk"]);
    tx.exec("INSERT INTO todos VALUES (?, ?)", ["row-2", "Walk dog"]);
  });

  // Persist snapshot to IndexedDB
  await db.save();

  // Close (auto-saves for idb backend)
  await db.close();
}
```

### LocalTransport -- in-process gateway (testing / offline demos)

```ts
import { LocalDB, SyncCoordinator, LocalTransport } from "@lakesync/client";
import { SyncGateway } from "@lakesync/gateway";

const gateway = new SyncGateway({ gatewayId: "test-gw" });
const transport = new LocalTransport(gateway);
const coordinator = new SyncCoordinator(db, transport);
```

### MemoryQueue / IDBQueue -- outbox implementations

```ts
import { MemoryQueue, IDBQueue } from "@lakesync/client";

// In-memory queue (testing / server-side)
const memQueue = new MemoryQueue();

// IndexedDB-backed queue (browser — persists across page reloads)
const idbQueue = new IDBQueue("my-app-sync-queue");

// Both implement the same SyncQueue interface
const result = await memQueue.push(delta);
const entries = await memQueue.peek(10);
await memQueue.markSending(entries.ok ? entries.value.map(e => e.id) : []);
await memQueue.ack(["entry-id"]);
```

### Schema registry

```ts
import { LocalDB, registerSchema, getSchema, migrateSchema } from "@lakesync/client";
import type { TableSchema } from "@lakesync/core";

const schema: TableSchema = {
  table: "todos",
  columns: [
    { name: "title", type: "string" },
    { name: "completed", type: "number" },
  ],
};

// Register a schema (creates the table + _lakesync_meta entry)
await registerSchema(db, schema);

// Retrieve the registered schema
const result = await getSchema(db, "todos");

// Migrate to a new schema version (ALTER TABLE ... ADD COLUMN)
const newSchema: TableSchema = {
  table: "todos",
  columns: [
    { name: "title", type: "string" },
    { name: "completed", type: "number" },
    { name: "priority", type: "number" },
  ],
};
await migrateSchema(db, schema, newSchema);
```

## API surface

### Sync coordination

| Export | Description |
|---|---|
| `SyncCoordinator` | Orchestrates push/pull cycles, auto-sync (interval + visibility), dead-lettering |
| `SyncCoordinatorConfig` | Optional config: `queue`, `hlc`, `clientId`, `maxRetries` |
| `SyncTracker` | Wraps LocalDB + SyncQueue + HLC; `insert()`, `update()`, `delete()`, `query()` |
| `SchemaSynchroniser` | Compares local/server schema versions and applies additive migrations |
| `applyRemoteDeltas(db, deltas, resolver, queue)` | Apply pulled deltas to SQLite with conflict resolution |

### Transport

| Export | Description |
|---|---|
| `SyncTransport` | Interface: `push(msg)`, `pull(msg)` returning `Result` |
| `HttpTransport` | HTTP transport with BigInt-safe JSON serialisation and JWT auth |
| `HttpTransportConfig` | Config: `baseUrl`, `gatewayId`, `token`, optional `fetch` |
| `LocalTransport` | In-process transport wrapping a `LocalGateway` interface |
| `LocalGateway` | Interface matching `SyncGateway.handlePush` / `handlePull` |

### Local database

| Export | Description |
|---|---|
| `LocalDB` | SQLite WASM wrapper: `open()`, `exec()`, `query()`, `transaction()`, `save()`, `close()` |
| `DbConfig` | Config: `name`, `backend` (`"idb"` / `"memory"` / auto-detect) |
| `DbError` | Error class for database operations |
| `Transaction` | Transaction object with `exec()` and `query()` methods |
| `registerSchema(db, schema)` | Register a `TableSchema` (creates table + metadata) |
| `getSchema(db, table)` | Retrieve the registered schema for a table |
| `migrateSchema(db, old, new)` | Apply additive schema migration (ALTER TABLE ADD COLUMN) |
| `saveSnapshot(name, data)` | Persist database bytes to IndexedDB |
| `loadSnapshot(name)` | Load database bytes from IndexedDB |
| `deleteSnapshot(name)` | Remove persisted snapshot |

### Queue

| Export | Description |
|---|---|
| `SyncQueue` | Interface: `push`, `peek`, `markSending`, `ack`, `nack`, `depth`, `clear` |
| `QueueEntry` | Entry: `{ id, delta, status, createdAt, retryCount }` |
| `QueueEntryStatus` | `'pending' \| 'sending' \| 'acked'` |
| `MemoryQueue` | In-memory `SyncQueue` for testing and server-side use |
| `IDBQueue` | IndexedDB-backed `SyncQueue` with BigInt serialisation |

## Testing

```bash
bun test --filter client
```

Or from the package directory:

```bash
cd packages/client
bun test
```

Tests use [Vitest](https://vitest.dev/) with `fake-indexeddb` for IDB tests.

# @lakesync/client

Client-side sync queue implementing the outbox pattern. Provides in-memory and IndexedDB-backed queue implementations for offline-first delta synchronisation. Deltas are queued locally, marked as sending during transit, and acknowledged or retried on failure -- ensuring no data is lost even when the network is unavailable.

## Install

```bash
bun add @lakesync/client
```

## Quick usage

### MemoryQueue -- in-memory outbox (testing / server-side)

```ts
import { MemoryQueue } from "@lakesync/client";
import { HLC, extractDelta } from "@lakesync/core";

const queue = new MemoryQueue();
const clock = new HLC();

// Extract a delta from a row change
const delta = await extractDelta(null, { name: "Alice" }, {
  table: "users",
  rowId: "u1",
  clientId: "client-1",
  hlc: clock.now(),
});

// Push the delta into the outbox
const pushResult = await queue.push(delta!);
if (pushResult.ok) {
  console.log("Queued entry:", pushResult.value.id);
}

// Peek at pending entries (ordered by creation time)
const peekResult = await queue.peek(10);
if (peekResult.ok) {
  const entries = peekResult.value;
  const ids = entries.map((e) => e.id);

  // Mark entries as in-flight
  await queue.markSending(ids);

  // On successful delivery, acknowledge and remove
  await queue.ack(ids);

  // Or on failure, return to pending with incremented retryCount
  // await queue.nack(ids);
}

// Check queue depth (pending + sending entries)
const depthResult = await queue.depth();
if (depthResult.ok) {
  console.log("Queue depth:", depthResult.value);
}
```

### IDBQueue -- IndexedDB-backed outbox (browser)

```ts
import { IDBQueue } from "@lakesync/client";

// Create an IDB-backed queue (persists across page reloads)
const queue = new IDBQueue();

// Optionally provide a custom database name
const testQueue = new IDBQueue("my-app-sync-queue");

// The API is identical to MemoryQueue
const result = await queue.push(delta);
if (result.ok) {
  console.log("Persisted to IndexedDB:", result.value.id);
}
```

The `IDBQueue` automatically serialises `HLCTimestamp` (branded `bigint`) values to strings for IndexedDB storage, since `structuredClone` cannot handle `bigint`. Deserialisation back to `bigint` is transparent.

## API surface

### Interfaces

| Export | Description |
|---|---|
| `SyncQueue` | Outbox queue interface with `push`, `peek`, `markSending`, `ack`, `nack`, `depth`, `clear` |
| `QueueEntry` | Queue entry: `{ id, delta, status, createdAt, retryCount }` |
| `QueueEntryStatus` | `'pending' \| 'sending' \| 'acked'` |

### Implementations

| Export | Description |
|---|---|
| `MemoryQueue` | In-memory `SyncQueue` -- suitable for testing and server-side use |
| `IDBQueue` | IndexedDB-backed `SyncQueue` -- persistent browser storage with BigInt serialisation |

### SyncQueue methods

| Method | Description |
|---|---|
| `push(delta)` | Add a `RowDelta` to the queue; returns `Result<QueueEntry>` |
| `peek(limit)` | Return up to `limit` pending entries ordered by `createdAt` |
| `markSending(ids)` | Transition entries from `pending` to `sending` |
| `ack(ids)` | Acknowledge delivery and remove entries from the queue |
| `nack(ids)` | Return entries to `pending` with incremented `retryCount` |
| `depth()` | Count of non-acked entries (pending + sending) |
| `clear()` | Remove all entries from the queue |

## Testing

```bash
bun test --filter client
```

Or from the package directory:

```bash
cd packages/client
bun test
```

Tests use [Vitest](https://vitest.dev/) with `fake-indexeddb` for IDBQueue tests. Test files are located in `src/queue/__tests__/`.

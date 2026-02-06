# @lakesync/gateway

Sync gateway coordinating delta ingestion, conflict resolution, and flush to object storage. Maintains a dual-structure `DeltaBuffer` (append-only log + row-level index) for efficient event streaming and conflict resolution. Incoming deltas are validated for HLC clock drift, deduplicated by `deltaId`, and merged via column-level Last-Write-Wins before being buffered for periodic flush.

## Install

```bash
bun add @lakesync/gateway
```

## Quick usage

### Create and use a SyncGateway

```ts
import { SyncGateway, type GatewayConfig } from "@lakesync/gateway";
import type { LakeAdapter } from "@lakesync/adapter";
import { HLC } from "@lakesync/core";

// Configure the gateway
const config: GatewayConfig = {
  gatewayId: "gw-1",
  maxBufferBytes: 1_048_576,  // 1 MiB
  maxBufferAgeMs: 30_000,     // 30 seconds
};

// Create a gateway (adapter is optional for testing)
const gateway = new SyncGateway(config);

// Or with a lake adapter for flushing
// const gateway = new SyncGateway(config, adapter);

// Handle a push from a client
const clientClock = new HLC();
const pushResult = gateway.handlePush({
  clientId: "client-1",
  deltas: [/* RowDelta[] */],
  lastSeenHlc: clientClock.now(),
});

if (pushResult.ok) {
  console.log("Server HLC:", pushResult.value.serverHlc);
  console.log("Accepted:", pushResult.value.accepted);
} else {
  console.error("Clock drift error:", pushResult.error.message);
}

// Handle a pull from a client
const pullResult = gateway.handlePull({
  clientId: "client-1",
  sinceHlc: HLC.encode(0, 0),
  maxDeltas: 100,
});

if (pullResult.ok) {
  console.log("Deltas:", pullResult.value.deltas.length);
  console.log("Has more:", pullResult.value.hasMore);
}

// Flush buffered deltas to the lake adapter
if (gateway.shouldFlush()) {
  const flushResult = await gateway.flush();
  if (!flushResult.ok) {
    console.error("Flush failed:", flushResult.error.message);
  }
}

// Monitor buffer statistics
console.log(gateway.bufferStats);
// { logSize: 0, indexSize: 0, byteSize: 0 }
```

### DeltaBuffer -- dual-structure buffer

```ts
import { DeltaBuffer } from "@lakesync/gateway";
import { rowKey, HLC, type RowDelta } from "@lakesync/core";

const buffer = new DeltaBuffer();

// Append a delta (post-conflict-resolution)
buffer.append(delta);

// Look up the current merged state for a row
const key = rowKey("users", "u1");
const existing = buffer.getRow(key);

// Check for idempotent re-push
const seen = buffer.hasDelta("some-delta-id");

// Retrieve events since a given HLC (for pull)
const { deltas, hasMore } = buffer.getEventsSince(sinceHlc, 100);

// Check flush conditions
const shouldFlush = buffer.shouldFlush({
  maxBytes: 1_048_576,
  maxAgeMs: 30_000,
});

// Drain and reset for flush
const entries = buffer.drain();
```

## API surface

### Classes

| Export | Description |
|---|---|
| `SyncGateway` | Main gateway class: `handlePush`, `handlePull`, `flush`, `shouldFlush`, `bufferStats` |
| `DeltaBuffer` | Dual-structure buffer: append-only log + row-level index |

### Types

| Export | Description |
|---|---|
| `SyncPush` | Push message: `{ clientId, deltas, lastSeenHlc }` |
| `SyncPull` | Pull request: `{ clientId, sinceHlc, maxDeltas }` |
| `SyncResponse` | Pull response: `{ deltas, serverHlc, hasMore }` |
| `GatewayConfig` | Configuration: `{ gatewayId, maxBufferBytes, maxBufferAgeMs }` |
| `GatewayState` | Runtime state: `{ hlc, flushing }` |
| `FlushEnvelope` | Versioned envelope written to object storage on flush |

### SyncGateway methods

| Method | Description |
|---|---|
| `handlePush(msg)` | Ingest client deltas with drift validation and LWW conflict resolution |
| `handlePull(msg)` | Return buffered deltas since a given HLC cursor |
| `flush()` | Write buffered deltas to the lake adapter as a `FlushEnvelope` |
| `shouldFlush()` | Check whether byte-size or age thresholds have been exceeded |
| `bufferStats` | Getter returning `{ logSize, indexSize, byteSize }` |

### DeltaBuffer methods

| Method | Description |
|---|---|
| `append(delta)` | Add a delta to the log and upsert the row index |
| `getRow(key)` | Look up the current merged state for a `RowKey` |
| `hasDelta(deltaId)` | Check if a delta ID is already in the log (idempotency) |
| `getEventsSince(hlc, limit)` | Return change events after a given HLC with pagination |
| `shouldFlush(config)` | Check byte-size and age thresholds |
| `drain()` | Drain and reset the buffer, returning all log entries |

## Testing

```bash
bun test --filter gateway
```

Or from the package directory:

```bash
cd packages/gateway
bun test
```

Tests use [Vitest](https://vitest.dev/).

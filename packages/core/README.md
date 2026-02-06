# @lakesync/core

Core primitives for LakeSync -- Hybrid Logical Clock (HLC), column-level delta extraction and application, Last-Write-Wins conflict resolution, and the `Result<T, E>` error handling pattern. All other LakeSync packages depend on these foundational types and utilities.

## Install

```bash
bun add @lakesync/core
```

## Quick usage

### HLC -- Hybrid Logical Clock

```ts
import { HLC } from "@lakesync/core";

// Create a clock (optionally inject a wall-clock source for testing)
const clock = new HLC();

// Generate a monotonically increasing timestamp
const ts1 = clock.now();
const ts2 = clock.now();
console.log(ts1 < ts2); // true

// Receive a remote timestamp and advance the local clock
const remoteTs = HLC.encode(Date.now(), 0);
const result = clock.recv(remoteTs);
if (result.ok) {
  console.log("Advanced to", result.value);
} else {
  console.error("Clock drift:", result.error.message);
}

// Decode a timestamp into wall clock (ms) and logical counter
const { wall, counter } = HLC.decode(ts1);
```

### Delta -- column-level change extraction and application

```ts
import { extractDelta, applyDelta, type RowDelta } from "@lakesync/core";
import { HLC } from "@lakesync/core";

const clock = new HLC();

// Extract a delta between two row states
const before = { name: "Alice", age: 30 };
const after = { name: "Alice", age: 31 };

const delta = await extractDelta(before, after, {
  table: "users",
  rowId: "u1",
  clientId: "client-1",
  hlc: clock.now(),
});

if (delta) {
  console.log(delta.op);      // "UPDATE"
  console.log(delta.columns);  // [{ column: "age", value: 31 }]

  // Apply the delta to a row
  const merged = applyDelta(before, delta);
  console.log(merged); // { name: "Alice", age: 31 }
}

// INSERT: before is null
const insertDelta = await extractDelta(null, { name: "Bob" }, {
  table: "users",
  rowId: "u2",
  clientId: "client-1",
  hlc: clock.now(),
});

// DELETE: after is null
const deleteDelta = await extractDelta({ name: "Bob" }, null, {
  table: "users",
  rowId: "u2",
  clientId: "client-1",
  hlc: clock.now(),
});
```

### Conflict -- Last-Write-Wins resolution

```ts
import { resolveLWW, LWWResolver } from "@lakesync/core";
import type { RowDelta } from "@lakesync/core";

// Resolve two conflicting deltas for the same row
const result = resolveLWW(localDelta, remoteDelta);
if (result.ok) {
  console.log("Resolved delta:", result.value);
} else {
  console.error("Conflict error:", result.error.message);
}

// Or use the class-based resolver directly
const resolver = new LWWResolver();
const resolved = resolver.resolve(localDelta, remoteDelta);
```

### Result -- Ok/Err error handling pattern

```ts
import {
  Ok,
  Err,
  mapResult,
  flatMapResult,
  unwrapOrThrow,
  fromPromise,
  type Result,
} from "@lakesync/core";

// Create success and failure results
const success: Result<number, Error> = Ok(42);
const failure: Result<number, Error> = Err(new Error("something went wrong"));

// Check and extract values
if (success.ok) {
  console.log(success.value); // 42
}

// Transform the success value
const doubled = mapResult(success, (n) => n * 2); // Ok(84)

// Chain Result-returning operations
const chained = flatMapResult(success, (n) =>
  n > 0 ? Ok(n.toString()) : Err(new Error("negative")),
);

// Unwrap or throw
const value = unwrapOrThrow(success); // 42

// Wrap a Promise into a Result
const result = await fromPromise(fetch("/api/data"));
```

## API surface

### HLC

| Export | Description |
|---|---|
| `HLC` | Hybrid Logical Clock class with `now()`, `recv()`, `encode()`, `decode()`, `compare()` |
| `HLCTimestamp` | Branded `bigint` type: `[48-bit wall ms][16-bit counter]` |

### Delta

| Export | Description |
|---|---|
| `extractDelta(before, after, opts)` | Extract a column-level delta between two row states (async) |
| `applyDelta(row, delta)` | Apply a delta to a row, returning the merged result |
| `RowDelta` | Row-level delta with `op`, `table`, `rowId`, `clientId`, `columns`, `hlc`, `deltaId` |
| `ColumnDelta` | Single column change: `{ column, value }` |
| `DeltaOp` | `'INSERT' \| 'UPDATE' \| 'DELETE'` |
| `TableSchema` | Column allow-list with type hints |
| `RowKey` | Branded composite key type (`table:rowId`) |
| `rowKey(table, rowId)` | Create a composite `RowKey` |

### Conflict

| Export | Description |
|---|---|
| `resolveLWW(local, remote)` | Convenience function for column-level LWW resolution |
| `LWWResolver` | Class implementing `ConflictResolver` with LWW semantics |
| `ConflictResolver` | Strategy interface: `resolve(local, remote) => Result<RowDelta, ConflictError>` |

### Result

| Export | Description |
|---|---|
| `Result<T, E>` | Discriminated union: `{ ok: true, value: T } \| { ok: false, error: E }` |
| `Ok(value)` | Create a successful `Result` |
| `Err(error)` | Create a failed `Result` |
| `mapResult(result, fn)` | Transform the success value |
| `flatMapResult(result, fn)` | Chain `Result`-returning operations |
| `unwrapOrThrow(result)` | Extract the value or throw the error |
| `fromPromise(promise)` | Wrap a `Promise` into a `Result` |
| `LakeSyncError` | Base error class with `code` and optional `cause` |
| `ClockDriftError` | HLC drift exceeded maximum threshold |
| `ConflictError` | Conflict resolution failure |
| `FlushError` | Flush operation failure |
| `SchemaError` | Schema mismatch or validation failure |
| `AdapterError` | Lake adapter operation failure |

## Testing

```bash
bun test --filter core
```

Or from the package directory:

```bash
cd packages/core
bun test
```

Tests use [Vitest](https://vitest.dev/) and are located in `src/*/__tests__/`.

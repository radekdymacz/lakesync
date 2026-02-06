# Delta Format & Guarantees Specification

This document defines the wire and storage contract for LakeSync deltas — the atomic unit of change that flows through the entire system.

## Delta Schema

A **RowDelta** represents a single mutation to one row in one table:

```typescript
interface RowDelta {
  op: "INSERT" | "UPDATE" | "DELETE";
  table: string;
  rowId: string;
  clientId: string;
  columns: ColumnDelta[];
  hlc: HLCTimestamp;
  deltaId: string;
}

interface ColumnDelta {
  column: string;
  value: unknown;  // JSON-serialisable, never undefined (use null)
}
```

### Field Definitions

| Field | Type | Description |
|-------|------|-------------|
| `op` | `"INSERT" \| "UPDATE" \| "DELETE"` | The operation type. INSERT creates a new row, UPDATE modifies existing columns, DELETE tombstones the row. |
| `table` | `string` | Table name. Must match a registered schema. |
| `rowId` | `string` | Unique row identifier within the table. Typically a UUID. |
| `clientId` | `string` | Identifier of the client that produced this delta. Used for LWW tiebreak and audit. |
| `columns` | `ColumnDelta[]` | Changed columns. Empty array for DELETE operations. Only changed columns are included in UPDATE. |
| `hlc` | `HLCTimestamp` | Hybrid Logical Clock timestamp (branded `bigint`). Encodes both wall time and logical ordering. |
| `deltaId` | `string` | Deterministic identifier: SHA-256 hex digest of `(clientId, hlc, table, rowId, columns)`. |

### Column Value Types

Column values must be JSON-serialisable. Supported types:

| Type | JSON Representation | Notes |
|------|-------------------|-------|
| `string` | `"hello"` | UTF-8 string |
| `number` | `42`, `3.14` | IEEE 754 double |
| `boolean` | `true`, `false` | Stored as 0/1 in SQLite |
| `null` | `null` | Represents absent/cleared value |
| `json` | `{...}`, `[...]` | Nested objects — see conflict semantics caveat |

**Never use `undefined`** as a column value. Use `null` to represent absence.

## HLC Timestamp Format

```
┌──────────────────────────────────────────────────┬──────────────────┐
│              48-bit wall clock (ms)               │  16-bit counter  │
└──────────────────────────────────────────────────┴──────────────────┘
```

- **Encoding**: `(BigInt(wallMs) << 16n) | BigInt(counter)` — a branded `bigint`
- **Wall clock**: Milliseconds since Unix epoch, 48 bits (valid until year 10889)
- **Counter**: Logical counter, 16 bits (0–65535), resets when wall clock advances
- **Comparison**: Standard bigint comparison (`a < b`) gives correct temporal ordering
- **Maximum drift**: 5000ms — the gateway rejects deltas with HLC wall clock more than 5 seconds ahead of its own physical clock
- **Counter overflow**: When counter exceeds 65535, wall clock advances by 1ms and counter resets to 0

### HLC Guarantees

1. **Monotonicity**: `hlc.now()` always returns a value strictly greater than any previous call on the same instance
2. **Causality**: `hlc.recv(remote)` advances the local clock to be strictly after `remote`, preserving causal ordering
3. **Bounded drift**: Remote timestamps more than 5s ahead of the gateway's physical clock are rejected with a `ClockDriftError`

## Delta ID (Idempotency Key)

The `deltaId` is a deterministic SHA-256 hash computed from:

```
SHA-256(stableStringify({
  clientId,
  hlc: hlc.toString(),
  table,
  rowId,
  columns
}))
```

- Uses `fast-json-stable-stringify` for key-order-independent serialisation
- The HLC bigint is converted to its string representation before hashing
- Output is a lowercase hex string (64 characters)

### Idempotency Rules

1. **Same deltaId = same mutation**: If a client pushes a delta with a `deltaId` that already exists in the gateway's buffer, the delta is silently accepted (idempotent re-push) without re-applying conflict resolution
2. **Determinism**: Two clients producing identical `(clientId, hlc, table, rowId, columns)` will generate the same `deltaId` — this is by design, since identical inputs represent the same logical mutation
3. **Push deduplication**: The gateway checks `buffer.hasDelta(deltaId)` before processing each delta in a push batch

## Delta Extraction Rules

Deltas are extracted by comparing row snapshots (before/after):

| Before | After | Result |
|--------|-------|--------|
| `null` | `{...}` | `INSERT` with all columns from `after` |
| `{...}` | `null` | `DELETE` with empty columns |
| `{...}` | `{...}` | `UPDATE` with only changed columns |
| `null` | `null` | No delta (no-op) |
| `{...}` | `{...}` (identical) | No delta (no-op) |

### Column Diff Semantics

- Primitive equality uses `Object.is()` (handles `NaN`, `+0`/`-0`)
- Object/array equality uses deep structural comparison (key-order-agnostic via `fast-deep-equal`)
- Columns present in `after` but not in `before` are included (new column)
- Columns with `undefined` value in `after` are skipped (treated as absent)
- If a `TableSchema` is registered, only columns in the schema's allow-list are diffed

## Ordering Guarantees

### Per-Client Ordering

Deltas from a single client are strictly ordered by HLC. Since each client has its own HLC instance, `hlc.now()` guarantees monotonic timestamps within that client.

### Cross-Client Ordering

Deltas from different clients are ordered by HLC comparison. When two clients produce deltas in the same millisecond:
1. The logical counter differentiates them if they're on the same HLC instance
2. If HLCs are equal (different clients, same wall time, counter 0), the `clientId` string is used as a deterministic tiebreak (lexicographically higher wins)

### Gateway Ordering

The gateway's `DeltaBuffer` maintains an append-only log. Deltas are appended in the order they are accepted by `handlePush()`. The log order is the gateway's arrival order, which may differ from HLC order across clients.

`handlePull()` returns deltas from the log with HLC strictly after the requested `sinceHlc` cursor, preserving log order within that window.

### No Global Total Order

LakeSync does **not** guarantee a global total order across all clients. It guarantees:
- Causal ordering per client
- Deterministic conflict resolution (same inputs always produce same output)
- Eventually consistent state (all clients converge after syncing the same deltas)

## Flush & Storage Format

### Parquet Layout

When flushed to Parquet, each delta becomes one row with columns mapped from the `TableSchema`:

| Parquet Column | Source |
|---------------|--------|
| `_op` | `delta.op` |
| `_row_id` | `delta.rowId` |
| `_client_id` | `delta.clientId` |
| `_hlc` | `delta.hlc` (as int64) |
| `_delta_id` | `delta.deltaId` |
| `<column_name>` | Each column from `delta.columns` |

File path: `deltas/{date}/{gatewayId}/{minHlc}-{maxHlc}.parquet`

### JSON Flush Envelope

When flushed as JSON (e.g. when no `tableSchema` is configured):

```json
{
  "version": 1,
  "gatewayId": "my-gateway",
  "createdAt": "2026-02-06T12:00:00.000Z",
  "hlcRange": { "min": "111...", "max": "222..." },
  "deltaCount": 42,
  "byteSize": 8192,
  "deltas": [...]
}
```

File path: `deltas/{date}/{gatewayId}/{minHlc}-{maxHlc}.json`

### Compaction Invariants

- **Base files**: Compacted Parquet files contain the materialised state (one row per `rowId`, latest column values applied)
- **Equality delete files**: List `rowId` values that have been deleted; readers must exclude matching rows from base files
- **Rebuild guarantee**: The full state can always be reconstructed by replaying all delta files in HLC order. Compacted files are an optimisation, not the source of truth
- **File lifecycle**: `data` files contain deltas or materialised rows; `equality-deletes` files contain tombstone lists. The compactor merges small files and produces new snapshots

## Wire Protocol (HTTP)

### Push

```
POST /sync/:gatewayId/push
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "clientId": "client-abc",
  "deltas": [...],
  "lastSeenHlc": "12345678901234"  // string — bigint serialised
}
```

### Pull

```
GET /sync/:gatewayId/pull?clientId=...&since=...&limit=...
Authorization: Bearer <jwt>
```

### BigInt Serialisation

HLC timestamps are `bigint` values that cannot be represented in standard JSON. On the wire:
- **Request**: A custom replacer converts `bigint` → `string` during `JSON.stringify()`
- **Response**: A custom reviver converts `string` → `bigint` for fields ending in `hlc` (case-insensitive) and the `hlc` field on delta objects

## Forward/Backward Compatibility

### Adding Columns

Adding a column to a `TableSchema` is safe:
- Existing deltas will not have the new column — readers should treat missing columns as `null`
- New deltas will include the column when it's changed
- Parquet files from before the column was added will have `null` values for the new column

### Removing Columns

Removing a column from a `TableSchema` is safe:
- New deltas will stop tracking the column
- Existing Parquet files retain the column data (immutable)
- Compaction will drop the column from materialised base files

### Renaming Columns

Column renames are **not supported** as an atomic operation. Use add + migrate + remove instead.

### Delta Format Versioning

The current delta format is **version 1** (implicit — no version field on individual deltas). The JSON flush envelope includes an explicit `version: 1` field. Future breaking changes to the delta schema will increment this version.

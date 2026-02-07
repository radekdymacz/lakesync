# @lakesync/parquet

Parquet serialisation and deserialisation for LakeSync deltas. Converts `RowDelta` arrays to Snappy-compressed Parquet bytes via Apache Arrow IPC and parquet-wasm, and reads them back. All public functions return `Result<T, E>` and never throw.

## Install

```bash
bun add @lakesync/parquet
```

Peer dependency: `@lakesync/core`

## Quick usage

### Write deltas to Parquet

```ts
import { writeDeltasToParquet } from "@lakesync/parquet";
import type { RowDelta, TableSchema } from "@lakesync/core";

const schema: TableSchema = {
  columns: { name: "string", age: "number", active: "boolean" },
};

const deltas: RowDelta[] = [
  /* ... your deltas ... */
];

const result = await writeDeltasToParquet(deltas, schema);
if (result.ok) {
  const parquetBytes: Uint8Array = result.value;
  // Upload to object storage, write to disk, etc.
} else {
  console.error("Write failed:", result.error.message);
}
```

### Read deltas from Parquet

```ts
import { readParquetToDeltas } from "@lakesync/parquet";

const parquetBytes: Uint8Array = /* ... loaded from storage ... */;

const result = await readParquetToDeltas(parquetBytes);
if (result.ok) {
  for (const delta of result.value) {
    console.log(delta.op, delta.table, delta.rowId, delta.columns);
  }
} else {
  console.error("Read failed:", result.error.message);
}
```

## API surface

| Export | Description |
|---|---|
| `writeDeltasToParquet(deltas, schema)` | Serialise `RowDelta[]` to Snappy-compressed Parquet bytes; returns `Result<Uint8Array, FlushError>` |
| `readParquetToDeltas(data)` | Deserialise Parquet bytes back to `RowDelta[]`; returns `Result<RowDelta[], FlushError>` |

## Implementation notes

- Boolean columns are stored as Int8 (1/0/null) to work around an Arrow JS IPC serialisation bug with all-null boolean buffers. The original types are preserved in Parquet metadata (`lakesync:bool_columns`) and restored on read.
- JSON-serialised values (objects and arrays stored as Utf8 strings) are automatically parsed back during deserialisation.
- HLC timestamps are stored as Int64 and cast back to branded `HLCTimestamp` bigints on read.

## Testing

```bash
bun test --filter parquet
```

Or from the package directory:

```bash
cd packages/parquet
bun test
```

Tests use [Vitest](https://vitest.dev/) and are located in `src/__tests__/`.

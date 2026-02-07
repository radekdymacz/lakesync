# @lakesync/catalogue

Iceberg REST catalogue client for LakeSync. Wraps the Nessie-backed Iceberg REST API v1 to manage namespaces, tables, schemas, and snapshot commits -- returning `Result<T, CatalogueError>` from every public method.

## Install

```bash
bun add @lakesync/catalogue
```

## Quick usage

### Connect to a Nessie catalogue

```ts
import { NessieCatalogueClient } from "@lakesync/catalogue";

const client = new NessieCatalogueClient({
  nessieUri: "http://localhost:19120/iceberg",
  warehouseUri: "s3://lakesync-warehouse",
});

// Create a namespace (idempotent -- ignores 409 Conflict)
await client.createNamespace(["lakesync"]);

// List all namespaces
const ns = await client.listNamespaces();
if (ns.ok) console.log(ns.value); // [["lakesync"]]
```

### Convert a LakeSync schema to Iceberg and create a table

```ts
import { tableSchemaToIceberg, buildPartitionSpec } from "@lakesync/catalogue";
import type { TableSchema } from "@lakesync/core";

const schema: TableSchema = {
  table: "todos",
  columns: [
    { name: "title", type: "string" },
    { name: "done", type: "boolean" },
  ],
};

const icebergSchema = tableSchemaToIceberg(schema);
const partitionSpec = buildPartitionSpec(icebergSchema);

await client.createTable(["lakesync"], "todos", icebergSchema, partitionSpec);
```

### Append data files and inspect snapshots

```ts
await client.appendFiles(["lakesync"], "todos", [
  {
    content: "data",
    "file-path": "s3://lakesync-warehouse/lakesync/todos/data/00001.parquet",
    "file-format": "PARQUET",
    "record-count": 50,
    "file-size-in-bytes": 4096,
  },
]);

const snap = await client.currentSnapshot(["lakesync"], "todos");
if (snap.ok && snap.value) {
  console.log("Snapshot ID:", snap.value["snapshot-id"]);
}
```

## API surface

| Export | Description |
|---|---|
| `NessieCatalogueClient` | Typed client for the Nessie Iceberg REST Catalogue API v1 |
| `tableSchemaToIceberg(schema)` | Convert a LakeSync `TableSchema` to an `IcebergSchema` with system columns |
| `buildPartitionSpec(schema)` | Build a day-partitioned `PartitionSpec` from the `hlc` field |
| `lakeSyncTableName(table)` | Map a table name to the `["lakesync"]` namespace |
| `CatalogueError` | Error class with `statusCode` for catalogue operation failures |
| `CatalogueConfig` | Connection config: `nessieUri`, `warehouseUri`, `defaultBranch?` |
| `IcebergSchema` / `IcebergField` | Iceberg schema and field type definitions |
| `PartitionSpec` | Iceberg partition specification type |
| `DataFile` | Data file reference for table commits |
| `Snapshot` / `TableMetadata` | Snapshot and full table metadata types |

## Testing

```bash
bun test --filter catalogue
```

Tests use [Vitest](https://vitest.dev/) and are located in `src/__tests__/`.

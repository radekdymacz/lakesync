# lakesync

Unified npm package for LakeSync -- a single install that provides access to all LakeSync sub-packages via subpath exports.

## Install

```bash
npm install lakesync
```

## Usage

The default export re-exports `@lakesync/core`. Each sub-package is available via a subpath:

```ts
// Core types and utilities (default export)
import { HLC, extractDelta, Ok, Err } from "lakesync";

// Client SDK
import { LocalDB, SyncCoordinator, HttpTransport } from "lakesync/client";

// Gateway
import { SyncGateway, DeltaBuffer } from "lakesync/gateway";

// Storage adapter
import { MinIOAdapter } from "lakesync/adapter";

// Protobuf codec
import { encodeSyncPush, decodeSyncPush } from "lakesync/proto";

// Parquet read/write
import { writeDeltasToParquet, readParquetToDeltas } from "lakesync/parquet";

// Iceberg catalogue
import { NessieCatalogueClient } from "lakesync/catalogue";

// Compaction
import { Compactor, MaintenanceRunner } from "lakesync/compactor";

// Analytics
import { DuckDBClient, UnionReader, TimeTraveller } from "lakesync/analyst";
```

## Subpath exports

| Subpath | Maps to | Description |
|---------|---------|-------------|
| `lakesync` | `@lakesync/core` | HLC, delta types, conflict resolution, Result type |
| `lakesync/client` | `@lakesync/client` | SyncCoordinator, LocalDB, transports, queues |
| `lakesync/gateway` | `@lakesync/gateway` | Sync gateway with delta buffer and flush |
| `lakesync/adapter` | `@lakesync/adapter` | Storage adapter interface + S3/MinIO implementation |
| `lakesync/proto` | `@lakesync/proto` | Protobuf codec for the wire protocol |
| `lakesync/parquet` | `@lakesync/parquet` | Parquet read/write via parquet-wasm |
| `lakesync/catalogue` | `@lakesync/catalogue` | Iceberg REST catalogue client |
| `lakesync/compactor` | `@lakesync/compactor` | Parquet compaction and maintenance |
| `lakesync/analyst` | `@lakesync/analyst` | DuckDB-WASM analytics and time-travel queries |

## Peer dependencies

Heavy runtime dependencies are optional peer dependencies -- install only what you need:

| Peer dependency | Required for |
|-----------------|-------------|
| `sql.js` | `lakesync/client` (LocalDB) |
| `idb` | `lakesync/client` (IDBQueue, IDB persistence) |
| `@aws-sdk/client-s3` | `lakesync/adapter` (MinIOAdapter) |
| `@bufbuild/protobuf` | `lakesync/proto` |
| `parquet-wasm`, `apache-arrow` | `lakesync/parquet` |
| `@duckdb/duckdb-wasm` | `lakesync/analyst` |

## When to use this vs `@lakesync/*`

- **Use `lakesync`** for quick prototyping or when you want a single dependency
- **Use individual `@lakesync/*` packages** for production to minimise bundle size and control peer dependencies precisely

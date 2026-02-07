# Phase 1 Retrospective — LakeSync Foundation

## What Was Built

Phase 1 delivered the core synchronisation primitives: a complete column-level delta sync engine with conflict resolution, durable queuing, gateway buffering, object storage flush, and a binary wire protocol — all wrapped in the `Result<T, E>` pattern with zero thrown exceptions.

## Package Inventory

| Package | Purpose | Key Exports |
|---------|---------|-------------|
| `@lakesync/core` | Delta types, HLC, Result, Conflict resolution, Parquet schema mapping | `RowDelta`, `HLC`, `Ok`/`Err`, `LWWResolver`, `extractDelta`, `applyDelta`, `TableSchema` |
| `@lakesync/client` | Client SDK: local DB, sync coordination, transports, queues | `LocalDB`, `SyncCoordinator`, `SyncTracker`, `HttpTransport`, `LocalTransport`, `MemoryQueue`, `IDBQueue`, `registerSchema` |
| `@lakesync/gateway` | Server-side buffer + flush (JSON/Parquet) + schema management | `SyncGateway`, `DeltaBuffer`, `SchemaManager`, `FlushEnvelope` |
| `@lakesync/adapter` | S3-compatible object storage | `LakeAdapter`, `MinIOAdapter` |
| `@lakesync/proto` | Protobuf wire protocol | `encode*`/`decode*` for RowDelta, SyncPush, SyncPull, SyncResponse |
| `@lakesync/parquet` | Parquet read/write via parquet-wasm | `writeDeltasToParquet`, `readParquetToDeltas` |
| `@lakesync/catalogue` | Iceberg REST catalogue (Nessie) | `NessieCatalogueClient`, `tableSchemaToIceberg` |
| `@lakesync/compactor` | Parquet compaction + equality deletes + maintenance | `Compactor`, `MaintenanceRunner`, `CompactionScheduler` |
| `@lakesync/analyst` | DuckDB-WASM analytics + time-travel queries | `DuckDBClient`, `UnionReader`, `TimeTraveller` |
| `lakesync` | Unified npm package with subpath exports | Re-exports all packages |
| `todo-app` | Example Vite app | `LocalDB` + `SyncCoordinator` with local/remote transport |
| `gateway-worker` | Cloudflare Workers deployment | `SyncGatewayDO`, `R2Adapter`, JWT auth |

## Architecture Decisions (ADRs)

1. **ADR-001** Column-level delta granularity — only changed columns transmitted
2. **ADR-002** Column-level LWW with HLC + clientId tiebreak
3. **ADR-003** Merge-on-read with equality deletes (MOR strategy)
4. **ADR-004** Iceberg REST catalogue for metadata (future)
5. **ADR-005** No multi-table transactions in v1
6. **ADR-006** 64-bit HLC (48-bit wall + 16-bit counter), 5s max drift
7. **ADR-007** Result pattern over exceptions
8. **ADR-008** Protobuf v3 wire protocol
9. **ADR-009** Deterministic deltaId via SHA-256

## Key Design Properties

- **HLCTimestamp** = branded bigint (`(wall << 16) | counter`)
- **DeltaId** = SHA-256 of `{clientId, hlc, table, rowId, columns}` — idempotent push
- **DeltaBuffer** = dual structure: append-only log (ordered) + row-key index (latest state)
- **Flush envelope** = JSON at `deltas/{YYYY-MM-DD}/{gatewayId}/{minHlc}-{maxHlc}.json`
- **IDBQueue** serialises HLC to string (IndexedDB structuredClone cannot handle bigint)

## What Works

- Full push/pull cycle: client → queue → gateway → buffer → flush to MinIO
- Column-level conflict resolution with deterministic winner
- Protobuf encode/decode roundtrip with HLC preservation
- Integration tests: multi-client sync, conflict resolution, end-to-end flush
- CI: lint + typecheck + unit tests + integration tests (Docker MinIO + Nessie)

## Post-Phase-1 Status

All phases through 4A have been completed. The items originally listed as "Phase 2 must address" have all been implemented:

1. **Flush format** — Parquet flush fully supported via `@lakesync/parquet` (Phase 2A)
2. **Iceberg catalogue** — `NessieCatalogueClient` with snapshot commits (Phase 2B)
3. **Local persistence** — `LocalDB` (sql.js + IndexedDB snapshots) with schema registry (Phase 2C)
4. **Change tracking** — `SyncTracker` with automatic delta extraction on insert/update/delete (Phase 2C)
5. **Production runtime** — Cloudflare Workers with Durable Objects, R2, JWT auth (Phase 2D)
6. **Compaction** — `Compactor` + `MaintenanceRunner` + `CompactionScheduler` with equality deletes (Phase 3A)
7. **Schema evolution** — `SchemaSynchroniser` for client-side + `SchemaManager` for server-side (Phase 3B)
8. **Query layer** — `DuckDBClient` + `UnionReader` + `TimeTraveller` for analytics and time-travel (Phase 4A)

# Phase 1 Retrospective — LakeSync Foundation

## What Was Built

Phase 1 delivered the core synchronisation primitives: a complete column-level delta sync engine with conflict resolution, durable queuing, gateway buffering, object storage flush, and a binary wire protocol — all wrapped in the `Result<T, E>` pattern with zero thrown exceptions.

## Package Inventory

| Package | Purpose | LOC (approx) | Key Exports |
|---------|---------|--------------|-------------|
| `@lakesync/core` | Delta types, HLC, Result, Conflict resolution | ~800 | `RowDelta`, `HLC`, `Ok`/`Err`, `LWWResolver`, `extractDelta`, `applyDelta` |
| `@lakesync/client` | Client-side sync queue | ~400 | `SyncQueue`, `MemoryQueue`, `IDBQueue` |
| `@lakesync/gateway` | Server-side buffer + flush | ~500 | `SyncGateway`, `DeltaBuffer`, `FlushEnvelope` |
| `@lakesync/adapter` | S3-compatible object storage | ~300 | `LakeAdapter`, `MinIOAdapter` |
| `@lakesync/proto` | Protobuf wire protocol | ~600 | `encode*`/`decode*` for RowDelta, SyncPush, SyncPull, SyncResponse |
| `@lakesync/todo-app` | Example Vite app | ~250 | TodoDB (in-memory), SyncManager, UI |

**Total:** ~2,850 lines of implementation + ~1,700 lines of tests across 5 packages.

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

## What Phase 2 Must Address

1. **Flush format** — JSON envelopes are readable but inefficient; Parquet needed for Iceberg compatibility
2. **No catalogue** — flushed files are orphaned blobs; need Iceberg table registration via Nessie
3. **No local persistence** — client uses in-memory or IndexedDB queue only; need SQLite WASM for proper local state
4. **No change tracking** — todo-app manually calls `extractDelta()`; need automatic change interception
5. **No production runtime** — need Cloudflare Workers (Durable Objects) for serverless gateway
6. **No compaction** — delta files accumulate without bound; need MOR compaction
7. **No schema evolution** — column additions require manual coordination
8. **No query layer** — no way to read historical or aggregated data from the lake

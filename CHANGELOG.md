# Changelog

All notable changes to LakeSync are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-02-06

### Added

#### Core (`@lakesync/core`)
- Hybrid Logical Clock (HLC) with branded bigint timestamps
- Column-level delta extraction from SQL row diffs
- Last-Write-Wins (LWW) conflict resolution at the column level
- `Result<T, E>` type for error handling without exceptions
- Parquet schema mapping from `TableSchema` definitions
- Sync protocol types (`SyncPush`, `SyncPull`, `SyncResponse`)

#### Client SDK (`@lakesync/client`)
- `SyncCoordinator` — orchestrates push/pull sync cycles with configurable transport
- `SyncTracker` — tracks local mutations and extracts deltas automatically
- `HttpTransport` — HTTP-based sync transport with BigInt-safe JSON serialisation
- `LocalTransport` — in-process transport for testing and offline demos
- `LocalDB` — sql.js wrapper with IndexedDB snapshot persistence
- `IDBQueue` — IndexedDB-backed sync queue with exponential backoff
- `MemoryQueue` — in-memory queue for testing and server-side use
- Schema registry for table definition management
- Automatic retry with exponential backoff and dead-lettering

#### Gateway (`@lakesync/gateway`)
- `SyncGateway` — handles delta ingestion, LWW conflict resolution, and flush
- `DeltaBuffer` — dual-structure buffer (append log + row index)
- Configurable flush to Parquet or JSON format
- Iceberg catalogue integration for snapshot commits via Nessie

#### Parquet (`@lakesync/parquet`)
- Parquet write/read using `parquet-wasm`
- Arrow schema mapping from LakeSync `TableSchema`

#### Catalogue (`@lakesync/catalogue`)
- Iceberg REST catalogue client (Nessie-compatible)
- Table creation, namespace management, and file append operations
- Iceberg schema and partition spec mapping

#### Adapter (`@lakesync/adapter`)
- `LakeAdapter` interface for object store abstraction
- MinIO/S3 adapter implementation using AWS SDK v3

#### Proto (`@lakesync/proto`)
- Protobuf schema for delta wire protocol
- Encode/decode codec with BigInt HLC support

#### Compactor (`@lakesync/compactor`)
- Parquet file compaction logic
- Equality delete file support for Iceberg v2
- Maintenance operations (expire snapshots, rewrite data files)

#### Gateway Worker (`gateway-worker`)
- Cloudflare Workers deployment with Durable Object sync gateway
- R2 adapter for Cloudflare R2 object storage
- JWT (HS256) authentication middleware
- Alarm-based automatic flush
- Admin schema registration endpoint

#### Todo App (`todo-app`)
- Reference implementation: offline-first todo list
- Column-level sync with automatic conflict resolution
- IndexedDB persistence across page reloads
- Online/offline status indicator

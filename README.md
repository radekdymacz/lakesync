# LakeSync

[![CI](https://github.com/radekdymacz/lakesync/actions/workflows/ci.yml/badge.svg)](https://github.com/radekdymacz/lakesync/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Column-level delta sync engine over an Iceberg-style lakehouse.**

LakeSync is an open-source sync engine that tracks changes at the column level, resolves conflicts using last-write-wins with hybrid logical clocks, and persists data to an Iceberg-compatible object store. It is designed for offline-first applications that need reliable, low-latency synchronisation between clients and a shared lakehouse backend.

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌───────────────┐      ┌───────────┐
│  Client SDK  │─────▶│ Sync Gateway │─────▶│ Object Store  │      │ Catalogue │
│  (browser)   │◀─────│ (CF Workers) │◀─────│  (R2/MinIO)   │─────▶│ (Nessie)  │
└─────────────┘      └──────────────┘      └───────────────┘      └───────────┘
       │                     │                      │
  SyncCoordinator      Delta Buffer            Parquet Files
  SyncTracker         (Log + Index)           (Iceberg snapshots)
  IDB Queue/Store     LWW Resolution
  HTTP/Local Transport
```

- **Column-level deltas** — only changed fields are synced, not entire rows
- **Hybrid Logical Clocks** — monotonic ordering across distributed clients
- **Last-Write-Wins** — deterministic conflict resolution at the column level
- **Protobuf wire protocol** — compact, typed serialisation
- **Iceberg-compatible storage** — data lands in Parquet on S3/MinIO/R2
- **Iceberg catalogue** — Nessie-compatible REST catalogue for metadata management

## Packages

| Package | Description |
|---------|-------------|
| `@lakesync/core` | HLC, delta types, conflict resolution, Result type, Parquet schema mapping |
| `@lakesync/client` | Client SDK: SyncCoordinator, SyncTracker, transports (HTTP + local), IndexedDB queue & persistence |
| `@lakesync/gateway` | Sync gateway with delta buffer, LWW conflict resolution, and Parquet/JSON flush |
| `@lakesync/adapter` | Lake adapter interface + MinIO/S3 implementation |
| `@lakesync/proto` | Protobuf schema and codec for wire protocol |
| `@lakesync/parquet` | Parquet read/write using parquet-wasm |
| `@lakesync/catalogue` | Iceberg REST catalogue client (Nessie-compatible) |
| `@lakesync/compactor` | Parquet file compaction and equality delete support |
| `@lakesync/analyst` | Placeholder for analytics queries (future) |

## Apps

| App | Description |
|-----|-------------|
| `gateway-worker` | Cloudflare Workers deployment with Durable Object gateway, R2 adapter, JWT auth |
| `todo-app` | Reference implementation: offline-first todo list with column-level sync |

## Getting Started

### As a Library Consumer

```bash
npm install @lakesync/client @lakesync/core
```

```typescript
import { LocalDB, SyncCoordinator, HttpTransport } from "@lakesync/client";

// Open a local database with IndexedDB persistence
const db = await LocalDB.open({ name: "my-app", backend: "idb" });

// Connect to a remote gateway
const transport = new HttpTransport({
  baseUrl: "https://your-gateway.workers.dev",
  gatewayId: "my-gateway",
  token: "your-jwt-token",
});

// Start syncing
const coordinator = new SyncCoordinator(db, transport);
coordinator.startAutoSync();
```

### Development

```bash
git clone https://github.com/radekdymacz/lakesync.git
cd lakesync
bun install
bun run build
bun run test
```

See the [Todo App example](apps/examples/todo-app/) for a working reference implementation.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

## Licence

Licensed under the [Apache Licence 2.0](LICENSE).

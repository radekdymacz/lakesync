# LakeSync

[![CI](https://github.com/radekdymacz/lakesync/actions/workflows/ci.yml/badge.svg)](https://github.com/radekdymacz/lakesync/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Column-level delta sync engine over an Iceberg-style lakehouse.**

LakeSync is an open-source sync engine that tracks changes at the column level, resolves conflicts using last-write-wins with hybrid logical clocks, and persists data to an Iceberg-compatible object store. It is designed for offline-first applications that need reliable, low-latency synchronisation between clients and a shared lakehouse backend.

## Quick Start

```bash
git clone https://github.com/radekdymacz/lakesync.git
cd lakesync
bun install
docker compose -f docker/docker-compose.yml up -d
bun run build
bun run test
```

See the [Todo App example](apps/examples/todo-app/) for a working reference implementation.

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌───────────────┐
│  Client SDK  │─────▶│ Sync Gateway │─────▶│ Object Store  │
│  (browser)   │◀─────│  (Bun/CF DO) │◀─────│ (MinIO/S3)    │
└─────────────┘      └──────────────┘      └───────────────┘
       │                     │
  Offline Queue         Delta Buffer
  (IndexedDB)          (Log + Index)
```

- **Column-level deltas** — only changed fields are synced, not entire rows
- **Hybrid Logical Clocks** — monotonic ordering across distributed clients
- **Last-Write-Wins** — deterministic conflict resolution at the column level
- **Protobuf wire protocol** — compact, typed serialisation
- **Iceberg-compatible storage** — data lands in Parquet on S3/MinIO

## Packages

| Package | Description |
|---------|-------------|
| `@lakesync/core` | HLC, delta extraction, conflict resolution, Result type |
| `@lakesync/client` | Client sync queue (memory + IndexedDB) |
| `@lakesync/gateway` | Sync gateway with delta buffer and flush logic |
| `@lakesync/adapter` | Lake adapter interface + MinIO implementation |
| `@lakesync/proto` | Protobuf schema and codec |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

## Licence

Licensed under the [Apache Licence 2.0](LICENSE).

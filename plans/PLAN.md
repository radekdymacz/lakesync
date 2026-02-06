# LakeSync — Master Plan

## Overview

LakeSync is a distributed sync engine that writes column-level deltas to an Apache Iceberg data lake. Phase 1 built the foundation (HLC, deltas, gateway, queue, proto, adapter). Phases 2–4 add Parquet output, Iceberg catalogue integration, SQLite WASM client, Cloudflare Workers runtime, compaction, schema evolution, and analytical queries.

## Phase Plans

| Phase | File | Summary |
|-------|------|---------|
| 1 (done) | [phase-1-retrospective.md](./phase-1-retrospective.md) | HLC, Delta, Result, Conflict, Queue, Gateway, Proto, Adapter |
| 2A (done) | [phase-2a-parquet-writer.md](./phase-2a-parquet-writer.md) | Parquet write/read + gateway flush upgrade |
| 2B (done) | [phase-2b-iceberg-catalogue.md](./phase-2b-iceberg-catalogue.md) | Nessie REST client + snapshot commit |
| 2C (done) | [phase-2c-sqlite-client.md](./phase-2c-sqlite-client.md) | SQLite WASM + change tracking + todo upgrade |
| 2D (done) | [phase-2d-cloudflare-workers.md](./phase-2d-cloudflare-workers.md) | CF Workers + Durable Objects + R2 + JWT |
| 3A (done) | [phase-3a-compaction.md](./phase-3a-compaction.md) | MOR compaction + equality deletes + maintenance |
| 3B (done) | [phase-3b-schema-evolution.md](./phase-3b-schema-evolution.md) | Server schema versioning + client ALTER TABLE |
| 4A (done) | [phase-4a-analyst.md](./phase-4a-analyst.md) | DuckDB-Wasm + Union Read + time travel |

## Dependency Graph

```
Phase 1 (done)
  │
  ├──▶ PARALLEL GROUP A ◀────────────────────────────────┐
  │    ├── 2A: Parquet Writer (packages/parquet/)         │
  │    │    Tasks: 2A.1 ∥ 2A.2 → 2A.3 → 2A.4            │
  │    │                                                   │
  │    └── 2C.1–2C.2: SQLite WASM basics                  │
  │         Tasks: 2C.1 ∥ 2C.2                            │
  │                                                        │
  ├──▶ SEQUENTIAL (after 2A) ─────────────────────────────┤
  │    └── 2B: Iceberg Catalogue (packages/catalogue/)    │
  │         Tasks: 2B.1 → 2B.2 → 2B.3 → 2B.4            │
  │                                                        │
  ├──▶ SEQUENTIAL (after 2C.1 + 2C.2) ───────────────────┤
  │    └── 2C.3–2C.5: Change tracking + todo upgrade      │
  │         Tasks: 2C.3 → 2C.4 → 2C.5                    │
  │                                                        │
  ├──▶ SEQUENTIAL (after 2A + 2B) ────────────────────────┤
  │    └── 2D: Cloudflare Workers (apps/gateway-worker/)  │
  │         Tasks: 2D.1 → 2D.2 → 2D.3 → 2D.4 → 2D.5    │
  │                                                        │
  ├──▶ SEQUENTIAL (after 2B) ─────────────────────────────┤
  │    └── 3A: Compaction (packages/compactor/)           │
  │         Tasks: 3A.1 → 3A.2 → 3A.3 → 3A.4            │
  │                                                        │
  ├──▶ SEQUENTIAL (after 2B + 2C) ────────────────────────┤
  │    └── 3B: Schema Evolution                            │
  │         Tasks: 3B.1 → 3B.2                            │
  │                                                        │
  └──▶ SEQUENTIAL (after 3A) ─────────────────────────────┘
       └── 4A: Analyst / Union Read (packages/analyst/)
            Tasks: 4A.1 → 4A.2 → 4A.3
```

## Execution Order

For a solo developer, the optimal execution order maximises parallelism where possible:

| Step | Phase | Parallel? | Blocks |
|------|-------|-----------|--------|
| 1 | **2A** Parquet Writer | PARALLEL GROUP A with 2C.1–2C.2 | 2B, 2D |
| 1 | **2C.1–2C.2** SQLite basics | PARALLEL GROUP A with 2A | 2C.3 |
| 2 | **2B** Iceberg/Nessie | SEQUENTIAL after 2A | 2D, 3A, 3B, 4A |
| 2 | **2C.3–2C.5** Change tracking | SEQUENTIAL after 2C.2 | 3B |
| 3 | **2D** Cloudflare Workers | SEQUENTIAL after 2A + 2B | — |
| 4 | **3A** Compaction | SEQUENTIAL after 2B | 4A |
| 5 | **3B** Schema Evolution | SEQUENTIAL after 2B + 2C | — |
| 6 | **4A** Analyst / Union Read | SEQUENTIAL after 3A | — |

## All Tasks by Phase

### Phase 2A — Parquet Writer

**PARALLEL GROUP:**
- **Task 2A.1** — Arrow schema mapping (`packages/core/src/parquet/`)
  - Map `TableSchema` + `RowDelta` → Arrow Schema + Table
  - System columns: op, table, rowId, clientId, hlc (Int64), deltaId
  - User columns: string→Utf8, number→Float64, boolean→Bool, json→Utf8
- **Task 2A.2** — Parquet write/read module (`packages/parquet/`)
  - New package: `writeDeltasToParquet()` + `readParquetToDeltas()`
  - Uses `parquet-wasm` + `apache-arrow`

**SEQUENTIAL (after 2A.1 + 2A.2):**
- **Task 2A.3** — Gateway Parquet flush (`packages/gateway/`)
  - Add `flushFormat: 'json' | 'parquet'` to GatewayConfig
  - Flush produces `.parquet` files instead of `.json`
- **Task 2A.4** — Integration validation (`tests/integration/`)
  - Gateway flush → MinIO → read back → equality assertion

### Phase 2B — Iceberg Catalogue

**SEQUENTIAL:**
- **Task 2B.1** — Nessie REST client (`packages/catalogue/`)
  - New package: namespace CRUD, table CRUD, append files, snapshots
- **Task 2B.2** — Table metadata mapping
  - LakeSync `TableSchema` → Iceberg schema + partition spec
- **Task 2B.3** — Snapshot commit on flush
  - Gateway flush → Parquet write → catalogue commit
- **Task 2B.4** — Integration tests
  - Push → flush → Nessie snapshot → DuckDB reads

### Phase 2C — SQLite WASM Client

**PARALLEL GROUP (with 2A):**
- **Task 2C.1** — SQLite WASM integration (`packages/client/src/db/`)
  - `LocalDB` class: open, exec, query, transaction, close
  - OPFS backend, IndexedDB fallback, memory for tests
- **Task 2C.2** — Schema registry (`packages/client/src/db/`)
  - `_lakesync_meta` table, `registerSchema()`, `migrateSchema()`

**SEQUENTIAL (after 2C.1 + 2C.2):**
- **Task 2C.3** — Change tracking (`packages/client/src/sync/`)
  - `SyncTracker`: wraps LocalDB + SyncQueue + HLC
  - Automatic delta extraction on insert/update/delete
- **Task 2C.4** — Apply remote deltas (`packages/client/src/sync/`)
  - `applyRemoteDeltas()`: conflict resolution + SQLite writes
- **Task 2C.5** — Todo app upgrade (`apps/examples/todo-app/`)
  - Replace in-memory Map with LocalDB + SyncTracker
  - Show sync status, queue depth, offline CRUD

### Phase 2D — Cloudflare Workers

**SEQUENTIAL:**
- **Task 2D.1** — Wrangler project setup (`apps/gateway-worker/`)
- **Task 2D.2** — Durable Object wrapper (SyncGatewayDO)
- **Task 2D.3** — R2 adapter (LakeAdapter for R2 bindings)
- **Task 2D.4** — Alarm-based flush
- **Task 2D.5** — JWT auth middleware

### Phase 3A — Compaction

**SEQUENTIAL:**
- **Task 3A.1** — Compaction logic (read deltas, LWW resolve, write base file)
- **Task 3A.2** — Equality delete file handling (MOR strategy)
- **Task 3A.3** — Maintenance: compact → expire snapshots → remove orphans
- **Task 3A.4** — Scheduling (DO alarm or standalone cron)

### Phase 3B — Schema Evolution

**SEQUENTIAL:**
- **Task 3B.1** — Server-side schema versioning + gateway validation
- **Task 3B.2** — Client schema negotiation + ALTER TABLE

### Phase 4A — Analyst / Union Read

**SEQUENTIAL:**
- **Task 4A.1** — DuckDB-Wasm integration (`packages/analyst/`)
- **Task 4A.2** — Union Read (hot SQLite + cold Iceberg merge)
- **Task 4A.3** — Time-travel queries (snapshot-based + HLC-based)

## New Packages Created

| Package | Phase | Purpose |
|---------|-------|---------|
| `@lakesync/parquet` | 2A | Parquet write/read via parquet-wasm |
| `@lakesync/catalogue` | 2B | Nessie/Iceberg REST client |
| `@lakesync/gateway-worker` | 2D | Cloudflare Workers app |

## Existing Packages Modified

| Package | Phases | Changes |
|---------|--------|---------|
| `@lakesync/core` | 2A | Add `src/parquet/schema.ts` (Arrow mapping) |
| `@lakesync/client` | 2C | Add `src/db/` (LocalDB, schema registry) + `src/sync/` (tracker, applier) |
| `@lakesync/gateway` | 2A, 2B, 3B | Parquet flush, catalogue commit, schema validation |
| `@lakesync/adapter` | 2D | Add R2Adapter |
| `@lakesync/compactor` | 3A | Upgrade from placeholder |
| `@lakesync/analyst` | 4A | Upgrade from placeholder |
| `@lakesync/todo-app` | 2C | Replace in-memory store with SQLite + sync tracker |

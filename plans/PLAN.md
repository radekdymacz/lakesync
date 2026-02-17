# LakeSync — Master Plan

## Overview

LakeSync is a local-first sync engine with **pluggable backends**. Client data lives in SQLite on the device, syncs through a lightweight gateway, and flushes to the backend of your choice — Postgres/MySQL for small data, Apache Iceberg on S3/R2 for large data. The `LakeAdapter` interface abstracts the storage layer; swap backends without changing client code.

Phases 1–5 built the core: HLC, deltas, gateway, queue, proto, S3/R2 adapter, Parquet output, Iceberg catalogue, SQLite WASM client, Cloudflare Workers runtime, compaction, schema evolution, analytical queries, sync rules, and initial sync. Phase 6 adds database adapters to complete the "any backend" story.

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
| 5 (done) | [phase-5-sync-rules-initial-sync.md](./phase-5-sync-rules-initial-sync.md) | Sync rules + checkpoint generation + initial sync |
| 5B (done) | — | README rewrite, docs site, GitHub Pages deployment |
| 6 (done) | — | Database adapters, composite routing, migration tooling, flush accuracy |
| 7 | — | Gateway scaling: table sharding + self-hosted server |
| 8 | — | Advanced adapters: BigQuery, fan-out, data lifecycle |
| 9 (done) | [phase-9-simple-made-easy.md](./phase-9-simple-made-easy.md) | Rich Hickey "Simple Made Easy" review |
| 10 (done) | [phase-10-production-hardening.md](./phase-10-production-hardening.md) | Production hardening, transport, rate limiting, observability |
| 11 | [phase-11-saas-mvp.md](./phase-11-saas-mvp.md) | SaaS MVP: control plane, billing, dashboard, security, DevOps |

## Dependency Graph

```
Phases 1–10 (done)
  │
  └──▶ Phase 11: SaaS MVP
       │
       ├── PARALLEL GROUP A (Quick Wins — no deps)
       │   ├── A1: SQL identifier sanitisation
       │   ├── A2: API versioning (/v1/)
       │   ├── A3: signToken utility
       │   ├── A4: Security headers
       │   ├── A5: Structured error codes
       │   └── A6: Request ID in responses
       │
       ├── PARALLEL GROUP B (Control Plane — foundation)
       │   ├── B1: Tenant/Org data model
       │   ├── B2: Clerk auth integration      ← after B1
       │   ├── B3: Gateway provisioning API     ← after B1+B2
       │   └── B4: API key management           ← after B1+B2
       │
       ├── PARALLEL GROUP C (Billing — after B1)
       │   ├── C1: UsageRecorder + gateway hooks ← after B1
       │   ├── C2: Quota enforcement             ← after C1
       │   └── C3: Stripe billing                ← after B1+C1
       │
       ├── PARALLEL GROUP D (Dashboard — after B+C)
       │   ├── D1: Next.js scaffold + Clerk      ← after B2
       │   ├── D2: Gateway management pages      ← after B3+D1
       │   ├── D3: API key management pages      ← after B4+D1
       │   └── D4: Usage & billing pages         ← after C3+D1
       │
       ├── PARALLEL GROUP E (Security — after B1)
       │   ├── E1: Audit logging                 ← after B1
       │   ├── E2: GDPR data deletion            ← after B1
       │   ├── E3: RBAC expansion                ← after B1+B2
       │   └── E4: JWT secret rotation           ← independent
       │
       └── PARALLEL GROUP F (DevOps & DX)
           ├── F1: CD pipeline                   ← independent
           ├── F2: OpenAPI spec                  ← independent
           ├── F3: CLI tool                      ← after A3+B3+B4
           └── F4: Webhook system                ← after B1

Phase 1–10 Historical Dependency Graph:

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
  ├──▶ SEQUENTIAL (after 3A) ─────────────────────────────┤
  │    └── 4A: Analyst / Union Read (packages/analyst/)  │
  │         Tasks: 4A.1 → 4A.2 → 4A.3                   │
  │                                                        │
  ├──▶ SEQUENTIAL (after 2D + 3A) ───────────────────────┘
  │    └── 5: Sync Rules + Initial Sync
  │         Tasks: 5A.1 → 5B.1 → 5B.2 → 5B.3 →
  │                5C.1 → 5C.2 → 5C.3 →
  │                5D.1 → 5D.2 → 5E.1 → 5E.2
  │
  ├──▶ SEQUENTIAL (after 5) ────────────────────────────
  │    └── 6: Database Adapters + Docs
  │         6A.1 → 6A.2 → PARALLEL { 6B, 6C }
  │         6D (Docs) ✅ independent
  │         6E (Composite + Migrate) after 6A + 6B
  │         6F (Flush accuracy) independent
  │
  ├──▶ SEQUENTIAL (after 6) ────────────────────────────
  │    └── 7: Gateway Scaling
  │         PARALLEL { 7A (Table Sharding), 7B (Self-Hosted) }
  │
  └──▶ SEQUENTIAL (after 6 + 7) ───────────────────────
       └── 8: Advanced Adapters
            PARALLEL { 8A (BigQuery), 8B (Fan-Out), 8C (Lifecycle) }
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
| 7 | **5** Sync Rules + Initial Sync | SEQUENTIAL after 2D + 3A | 6 |
| 8 | **6A** Database adapter interface | SEQUENTIAL after 5 | 6B, 6C |
| 8 | **6D** Docs site | INDEPENDENT (done ✅) | — |
| 9 | **6B** Postgres adapter | PARALLEL with 6C, after 6A | 6E |
| 9 | **6C** MySQL adapter | PARALLEL with 6B, after 6A | — |
| 9 | **6F** Flush byte accuracy | INDEPENDENT | — |
| 10 | **6E** Composite + migration | SEQUENTIAL after 6A + 6B | — |
| 11 | **7A** Table sharding | PARALLEL with 7B, after 6 | — |
| 11 | **7B** Self-hosted gateway | PARALLEL with 7A, after 6 | — |
| 12 | **8A** BigQuery adapter | PARALLEL with 8B + 8C, after 7 | — |
| 12 | **8B** Fan-out adapter | PARALLEL with 8A + 8C, after 7 | — |
| 12 | **8C** Data lifecycle | PARALLEL with 8A + 8B, after 7 | — |
| 13 | **10** Production hardening | PARALLEL groups A–F, after 8 | — |
| 14 | **11** SaaS MVP | See [phase-11-saas-mvp.md](./phase-11-saas-mvp.md) | — |

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

### Phase 5 — Sync Rules + Initial Sync

**FOUNDATION (no deps):**
- **Task 5A.1** — Sync rules types + evaluator (`packages/core/src/sync-rules/`)
  - Types: `SyncRuleOp`, `SyncRuleFilter`, `BucketDefinition`, `SyncRulesConfig`, `SyncRulesContext`
  - Pure evaluator functions: `filterDeltas()`, `deltaMatchesBucket()`, `validateSyncRules()`

**SEQUENTIAL (after 5A.1):**
- **Task 5B.1** — Gateway filtered pull (`packages/gateway/`)
  - `handlePull(msg, context?)` with over-fetch + filter + bounded retry
- **Task 5B.2** — Checkpoint generator (`packages/compactor/`)
  - Reads base Parquet → proto-encoded chunks → R2 manifest
  - Per-table (not per-user); filtering at serve time
- **Task 5B.3** — Hook into maintenance (`packages/compactor/`)
  - Checkpoint generation after compaction; keys added to orphan protection

**SEQUENTIAL (after 5B):**
- **Task 5C.1** — JWT claims extension (`apps/gateway-worker/`)
  - Extract non-standard claims from JWT for sync rules evaluation
- **Task 5C.2** — Sync rules storage + admin endpoint (`apps/gateway-worker/`)
  - DO storage + `POST /admin/sync-rules/:gatewayId` + filtered pull integration
- **Task 5C.3** — Checkpoint serving endpoint (`apps/gateway-worker/`)
  - `GET /sync/:gatewayId/checkpoint` with serve-time filtering, one chunk in memory at a time

**SEQUENTIAL (after 5C):**
- **Task 5D.1** — Transport checkpoint method (`packages/client/`)
  - Optional `checkpoint()` on `SyncTransport`, implemented in `HttpTransport` and `LocalTransport`
- **Task 5D.2** — Initial sync in SyncCoordinator (`packages/client/`)
  - `syncOnce()` calls `initialSync()` when `lastSyncedHlc === 0`, then incremental pull

**SEQUENTIAL (after 5D):**
- **Task 5E.1** — Integration tests
  - Sync rules filtering + initial sync via checkpoint + delta catchup
- **Task 5E.2** — Documentation

### Phase 5B — README, Docs Site, GitHub Pages (done)

- **Task 5B.1** — Fumadocs site (`apps/docs/`) ✅
  - Fumadocs + Next.js 15 + Tailwind v4 + Mermaid diagrams
  - Landing page with pluggable backend story (sequence diagrams for core flow, small/large data, conflict resolution, offline sync, sync rules)
  - Architecture docs, API reference, getting started guide
  - Static export to GitHub Pages (`output: 'export'`, `basePath: /lakesync`)
- **Task 5B.2** — GitHub Actions deployment (`.github/workflows/docs.yml`) ✅
  - Triggers on push to main (paths: apps/docs/**, packages/*/src/**)
  - Builds with `NEXT_PUBLIC_BASE_PATH=/lakesync`, uploads to GitHub Pages
- **Task 5B.3** — README rewrite ✅
  - Updated to reflect pluggable backend vision ("Local-first sync. Any backend.")
  - Right-size your backend section (small data / large data / mix both)
  - Backend support table (R2, S3, MinIO, Postgres, MySQL, Composite, BigQuery planned)
  - Links to docs site, architecture diagrams
- **Task 5B.4** — GitHub Pages configuration ✅
  - Repo Settings → Pages → Source: GitHub Actions
  - `public/.nojekyll` for static export

### Phase 6 — Database Adapters + Docs

The "any backend" story. The `LakeAdapter` interface (`putObject`, `getObject`, `headObject`, `listObjects`, `deleteObject`, `deleteObjects`) currently has one implementation (MinIO/S3). Phase 6 adds database adapters so users can sync to Postgres, MySQL, or any SQL database for small-data use cases. The docs site (Fumadocs) is also part of this phase.

**6A — Database Adapter Interface** (`packages/adapter/`)

The current `LakeAdapter` is object-storage oriented (put/get bytes by path). Database adapters need a higher-level interface that works with deltas directly rather than raw bytes. Two approaches:

1. **Option A — Extend `LakeAdapter`**: Add `putDeltas()`, `getDeltas()`, `queryDeltas()` alongside existing object methods. Object-storage adapters ignore the delta methods; database adapters ignore the object methods.
2. **Option B — New `DatabaseAdapter` interface**: A separate interface that the gateway can accept as an alternative to `LakeAdapter`. Methods: `insertDeltas()`, `queryDeltasSince()`, `getLatestState()`, `runMigration()`.

Option B is cleaner — the gateway accepts `LakeAdapter | DatabaseAdapter` and dispatches accordingly. The flush path differs: object-storage adapters batch-write Parquet files, database adapters batch-INSERT rows.

**SEQUENTIAL:**
- **Task 6A.1** — `DatabaseAdapter` interface definition (`packages/adapter/src/db-types.ts`)
  - `insertDeltas(deltas: Delta[]): Promise<Result<void, AdapterError>>`
  - `queryDeltasSince(hlc: HLCTimestamp, tables?: string[]): Promise<Result<Delta[], AdapterError>>`
  - `getLatestState(table: string, rowId: string): Promise<Result<Record<string, unknown> | null, AdapterError>>`
  - `ensureSchema(schema: TableSchema): Promise<Result<void, AdapterError>>`
- **Task 6A.2** — Gateway dual-adapter support (`packages/gateway/`)
  - Accept `adapter: LakeAdapter | DatabaseAdapter` in config
  - Flush path: if `DatabaseAdapter`, batch-INSERT deltas; if `LakeAdapter`, write Parquet
  - Pull path unchanged — DeltaBuffer is the source of truth during a session

**6B — Postgres Adapter** (`packages/adapter/src/postgres.ts`)

**SEQUENTIAL (after 6A):**
- **Task 6B.1** — Postgres adapter implementation
  - Uses `pg` (node-postgres) or Drizzle ORM
  - Delta table: `lakesync_deltas` with columns matching Delta type
  - State table: `lakesync_state` with per-column HLC tracking
  - `insertDeltas()`: batch INSERT with ON CONFLICT for idempotency (deltaId is unique)
  - `queryDeltasSince()`: `SELECT * FROM lakesync_deltas WHERE hlc > $1 ORDER BY hlc`
  - `ensureSchema()`: CREATE TABLE IF NOT EXISTS + ALTER TABLE for new columns
- **Task 6B.2** — Integration tests (Docker Compose + Postgres)
  - Push → flush to Postgres → query back → verify delta integrity
  - Multi-client sync through Postgres backend
  - Schema evolution (add column → client pulls new schema)

**6C — MySQL Adapter** (`packages/adapter/src/mysql.ts`)

**SEQUENTIAL (after 6A):**
- **Task 6C.1** — MySQL adapter implementation
  - Same interface as Postgres, different SQL dialect
  - Uses `mysql2` driver
  - Handles MySQL-specific types (BIGINT for HLC, JSON columns)
- **Task 6C.2** — Integration tests (Docker Compose + MySQL)

**6D — Documentation Site** (moved to Phase 5B)

**6E — Composite Adapter + Migration Tooling**

**SEQUENTIAL (after 6A + 6B):**
- **Task 6E.1** — `CompositeAdapter` (`packages/adapter/src/composite.ts`)
  - Routes deltas to different adapters by table name
  - Config: `{ routes: [{ tables: string[], adapter }], default: adapter }`
  - Gateway flush dispatches deltas to the correct adapter per table
  - Example: `users` → Postgres, `events` → Iceberg on R2
- **Task 6E.2** — Migration tooling (`packages/adapter/src/migrate.ts`)
  - `migrateAdapter({ from, to, tables?, batchSize? })` — reads all state from source adapter, writes to target
  - Preserves deltaIds for idempotency — safe to re-run
  - Progress callback for CLI/UI reporting
  - Use case: "data outgrew Postgres, migrate to Iceberg"

**6F — Accurate flush byte estimation** (`packages/gateway/src/buffer.ts`)

- **Task 6F.1** — Replace fixed `ESTIMATED_BYTES_PER_COLUMN = 50` with actual serialised size
  - Measure `JSON.stringify(value).length` for each column value on append
  - More accurate memory pressure tracking, especially for JSON blobs vs booleans
  - Keeps the existing `maxBytes` / `maxAgeMs` dual-threshold flush trigger

### Phase 7 — Gateway Scaling

**7A — Table Sharding**

Shard a single tenant's traffic across multiple DOs by table name. A router Worker inspects the delta's `table` field and forwards to the correct DO. Each shard flushes independently.

**SEQUENTIAL:**
- **Task 7A.1** — Shard router (`apps/gateway-worker/src/shard-router.ts`)
  - Config: `{ shards: [{ tables: string[], gatewayId: string }], default: string }`
  - Worker-level routing — inspects request body, forwards to the correct DO
  - Pull fans out across shards and merges results
- **Task 7A.2** — Shard-aware admin endpoints
  - Flush, sync rules, checkpoint apply to all shards for a tenant
- **Task 7A.3** — Integration tests
  - Multi-table push/pull across shards, verify consistency

**7B — Self-Hosted Gateway**

The core `Gateway` class (`packages/gateway/`) is runtime-agnostic. This task adds a standalone Node.js/Bun server target alongside the existing CF DO wrapper.

**SEQUENTIAL:**
- **Task 7B.1** — Standalone gateway server (`packages/gateway-server/`)
  - New package: HTTP server (Hono or plain `Bun.serve`)
  - In-memory DeltaBuffer (same as DO), periodic flush via `setInterval`
  - Adapter injected at startup (Postgres, MySQL, S3, etc.)
- **Task 7B.2** — DeltaBuffer persistence (`packages/gateway-server/`)
  - SQLite or Redis for DeltaBuffer durability across restarts
  - WAL-mode SQLite for single-machine, Redis for multi-process
- **Task 7B.3** — Docker image + compose example
  - `Dockerfile` for gateway-server + example compose with Postgres adapter
  - README: "run your own gateway without Cloudflare"

### Phase 8 — Advanced Adapters

**8A — BigQuery Adapter** (`packages/adapter/src/bigquery.ts`)

- **Task 8A.1** — BigQuery adapter implementation
  - Uses `@google-cloud/bigquery` streaming insert API for writes
  - Standard SQL for `queryDeltasSince()`
  - `ensureSchema()`: dataset + table creation via BigQuery API
- **Task 8A.2** — Integration tests (BigQuery emulator or live project)

**8B — Read Replicas / Fan-Out**

- **Task 8B.1** — `FanOutAdapter` (`packages/adapter/src/fan-out.ts`)
  - Writes to primary adapter + replicates to one or more secondary adapters
  - Primary is sync (flush waits for ACK), secondaries are async (best-effort)
  - Use case: write to Postgres (primary), replicate to BigQuery (analytics)
- **Task 8B.2** — Integration tests

**8C — Data Lifecycle**

- **Task 8C.1** — Age-based tier migration (`packages/adapter/src/lifecycle.ts`)
  - Config: `{ hot: { adapter, maxAgeMs }, cold: { adapter } }`
  - Background job moves data from hot to cold adapter when HLC age exceeds threshold
  - Example: last 30 days in Postgres, older data in Iceberg
- **Task 8C.2** — Integration tests

### Phase 9 — Simple Made Easy (done)

- Rich Hickey review of codebase architecture. See [phase-9-simple-made-easy.md](./phase-9-simple-made-easy.md).

### Phase 10 — Production Hardening

See [phase-10-production-hardening.md](./phase-10-production-hardening.md) for the full plan.

**ALL PARALLEL:**
- **Group A** — Transport & client hardening (token refresh, offline detection)
- **Group B** — Gateway server production readiness (graceful shutdown, health probes, request timeouts)
- **Group C** — Adapter hardening (configurable pool options)
- **Group D** — Rate limiting & WebSocket backpressure
- **Group E** — Observability (structured logger, Prometheus metrics)
- **Group F** — Connector cursor persistence

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

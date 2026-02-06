# Phase 2B — Iceberg Catalogue (Nessie)

**Goal:** Register flushed Parquet files as Iceberg table snapshots via the Nessie REST catalogue, enabling time-travel queries and standard Iceberg tooling (DuckDB, Spark, Trino).

**Depends on:** Phase 2A (Parquet writer)
**Blocks:** 2D (Cloudflare Workers), 3A (Compaction), 3B (Schema Evolution), 4A (Analyst)

---

## SEQUENTIAL GROUP (all tasks in order)

### Task 2B.1: Nessie REST client

- **Package:** `packages/catalogue/` (new package)
- **Creates:**
  - `packages/catalogue/package.json`
  - `packages/catalogue/tsconfig.json`
  - `packages/catalogue/vitest.config.ts`
  - `packages/catalogue/src/index.ts`
  - `packages/catalogue/src/types.ts`
  - `packages/catalogue/src/nessie-client.ts`
  - `packages/catalogue/src/__tests__/nessie-client.test.ts`
- **Modifies:** none
- **Dependencies:** none (but package uses `Result` from `@lakesync/core`)
- **Implementation:**
  1. Create new package scaffolding:
     ```json
     {
       "name": "@lakesync/catalogue",
       "version": "0.0.1",
       "type": "module",
       "dependencies": {
         "@lakesync/core": "workspace:*"
       }
     }
     ```
  2. `src/types.ts` — Iceberg + Nessie types:
     ```typescript
     export interface CatalogueConfig {
       nessieUri: string;         // e.g. "http://localhost:19120/api/v2"
       warehouseUri: string;      // e.g. "s3://lakesync-warehouse"
       defaultBranch?: string;    // default: "main"
     }

     export interface IcebergSchema {
       type: "struct";
       fields: IcebergField[];
     }

     export interface IcebergField {
       id: number;
       name: string;
       required: boolean;
       type: string; // "string", "long", "double", "boolean"
     }

     export interface PartitionSpec {
       specId: number;
       fields: Array<{
         sourceId: number;
         fieldId: number;
         name: string;
         transform: string; // "day", "identity", etc.
       }>;
     }

     export interface DataFile {
       content: "data";
       filePath: string;
       fileFormat: "PARQUET";
       recordCount: number;
       fileSizeInBytes: number;
       partitionData?: Record<string, string>;
     }

     export type CatalogueError = import("@lakesync/core").LakeSyncError;
     ```
  3. `src/nessie-client.ts` — typed HTTP client:
     - `NessieCatalogueClient` class with methods:
       - `createNamespace(name: string[]): Promise<Result<void, CatalogueError>>`
       - `listNamespaces(): Promise<Result<string[][], CatalogueError>>`
       - `createTable(namespace: string[], name: string, schema: IcebergSchema, partitionSpec: PartitionSpec): Promise<Result<void, CatalogueError>>`
       - `loadTable(namespace: string[], name: string): Promise<Result<TableMetadata, CatalogueError>>`
       - `appendFiles(namespace: string[], table: string, files: DataFile[]): Promise<Result<void, CatalogueError>>`
       - `currentSnapshot(namespace: string[], table: string): Promise<Result<Snapshot | null, CatalogueError>>`
     - All methods use `fetch()` against Nessie Iceberg REST endpoints
     - All methods return `Result<T, CatalogueError>`
     - Handle HTTP errors → `CatalogueError` with status code
  4. Use Nessie's Iceberg REST Catalogue API v1 endpoints:
     - `POST /v1/namespaces` — create namespace
     - `GET /v1/namespaces` — list namespaces
     - `POST /v1/namespaces/{ns}/tables` — create table
     - `GET /v1/namespaces/{ns}/tables/{table}` — load table metadata
     - `POST /v1/namespaces/{ns}/tables/{table}` — update table (append)
- **Tests:**
  - All tests use `describe.skipIf(!process.env.NESSIE_URI)` for Docker skip
  - Create namespace → list → verify present
  - Create table → load → verify schema matches
  - Append file → current snapshot → verify file list
  - Error handling: 404 on missing table, 409 on conflict
- **Done when:** All Nessie client methods pass tests against live Nessie instance

---

### Task 2B.2: Table metadata mapping

- **Package:** `packages/catalogue/`
- **Creates:**
  - `packages/catalogue/src/schema-mapping.ts`
  - `packages/catalogue/src/__tests__/schema-mapping.test.ts`
- **Modifies:**
  - `packages/catalogue/src/index.ts` (add exports)
- **Dependencies:** Task 2B.1, Task 2A.1 (uses `TableSchema` and Arrow schema knowledge)
- **Implementation:**
  1. `src/schema-mapping.ts`:
     - `tableSchemaToIceberg(schema: TableSchema): IcebergSchema`
       - System columns: `op` (string), `table` (string), `rowId` (string), `clientId` (string), `hlc` (long), `deltaId` (string)
       - User columns: `string`→string, `number`→double, `boolean`→boolean, `json`→string
       - Field IDs assigned sequentially starting from 1
     - `buildPartitionSpec(schema: IcebergSchema): PartitionSpec`
       - Partition by `day(hlc)` — extract date from HLC wall clock
       - Single partition field: `hlc_day` using `day` transform on `hlc` field
     - `lakeSyncTableName(table: string): { namespace: string[]; name: string }`
       - Maps LakeSync table name → Iceberg namespace `["lakesync"]` + table name
  2. Ensure schema mapping aligns exactly with Arrow schema from 2A.1 — same column order, same type semantics
- **Tests:**
  - `TableSchema` with mixed types → correct Iceberg field types
  - System columns present and correctly typed
  - Partition spec targets `hlc` field with `day` transform
  - Field IDs are sequential and stable
- **Done when:** Schema mapping is deterministic and aligned with Arrow/Parquet schema

---

### Task 2B.3: Snapshot commit on flush

- **Package:** `packages/gateway/`
- **Creates:**
  - `packages/gateway/src/__tests__/catalogue-flush.test.ts`
- **Modifies:**
  - `packages/gateway/src/types.ts` — add optional `catalogue` to `GatewayConfig`
  - `packages/gateway/src/gateway.ts` — extend `flush()` with catalogue commit
- **Dependencies:** Task 2B.1 + 2B.2 + 2A.3
- **Implementation:**
  1. Add to `GatewayConfig`:
     ```typescript
     catalogue?: NessieCatalogueClient;
     ```
  2. Extend `flush()` in `gateway.ts`:
     - After successful Parquet write to object storage:
     - If `this.config.catalogue` is set:
       - Ensure namespace exists (idempotent `createNamespace`)
       - Ensure table exists (idempotent create, skip if already exists)
       - Build `DataFile` from the written Parquet file (path, size, record count)
       - Call `catalogue.appendFiles(namespace, table, [dataFile])`
       - On 409 conflict: retry once with fresh table metadata
       - On catalogue error: log warning but do NOT fail the flush (data is safe in storage)
  3. Catalogue commit is best-effort: the Parquet file is the source of truth. A background reconciliation process can re-register orphaned files later.
- **Tests:**
  - Flush with catalogue → Nessie shows new snapshot
  - Flush without catalogue → existing behaviour unchanged
  - Catalogue error → flush still succeeds (data written to storage)
  - 409 retry → second attempt succeeds
- **Done when:** Gateway flush optionally commits to Nessie, all tests pass

---

### Task 2B.4: Integration tests

- **Package:** `tests/integration/`
- **Creates:**
  - `tests/integration/iceberg-catalogue.test.ts`
- **Modifies:**
  - `.github/workflows/ci.yml` (ensure Nessie is available)
- **Dependencies:** Task 2B.3
- **Implementation:**
  1. End-to-end test:
     - Create gateway with MinIO adapter + Nessie catalogue
     - Push 100 deltas across 3 tables
     - Flush → verify Parquet files in MinIO
     - Verify Nessie has snapshots for each table
     - Read Parquet files back → verify delta equality
  2. Multi-flush test:
     - Flush twice → Nessie shows 2 snapshots → snapshot history is correct
  3. DuckDB Iceberg read (optional, skip if unavailable):
     - `duckdb -c "SELECT * FROM iceberg_scan('...')"` using Nessie catalogue URI
- **Tests:**
  - Push → flush → Nessie snapshot exists
  - Multiple flushes → snapshot chain grows
  - Cross-tool read via DuckDB (optional)
- **Done when:** Integration tests pass in CI with Docker MinIO + Nessie

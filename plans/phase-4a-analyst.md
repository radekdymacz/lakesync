# Phase 4A — Analyst / Union Read

**Goal:** Enable analytical queries across both hot local data (SQLite) and cold historical data (Iceberg/Parquet) using DuckDB-Wasm, with support for time-travel queries.

**Depends on:** Phase 3A (Compaction — needs compacted base files for efficient reads)
**Blocks:** none (capstone feature)

---

## SEQUENTIAL GROUP (all tasks in order)

### Task 4A.1: DuckDB-Wasm integration

- **Package:** `packages/analyst/` (upgrade from placeholder)
- **Creates:**
  - `packages/analyst/src/duckdb.ts`
  - `packages/analyst/src/types.ts`
  - `packages/analyst/src/index.ts`
  - `packages/analyst/src/__tests__/duckdb.test.ts`
- **Modifies:**
  - `packages/analyst/package.json` (add dependencies)
  - `packages/analyst/tsconfig.json`
  - `packages/analyst/vitest.config.ts`
- **Dependencies:** none within phase
- **Implementation:**
  1. `src/types.ts`:
     ```typescript
     export interface AnalystConfig {
       /** S3 endpoint for reading Parquet files from the lake */
       s3Endpoint: string;
       s3Bucket: string;
       s3Credentials: { accessKeyId: string; secretAccessKey: string };
       /** Nessie URI for Iceberg catalogue */
       nessieUri?: string;
     }

     export interface QueryResult<T = Record<string, unknown>> {
       rows: T[];
       columnNames: string[];
       rowCount: number;
       elapsed: number; // ms
     }

     export class AnalystError extends LakeSyncError {
       constructor(message: string, cause?: Error) {
         super(message, "ANALYST_ERROR", cause);
       }
     }
     ```
  2. `src/duckdb.ts` — `LakeAnalyst` class:
     ```typescript
     export class LakeAnalyst {
       static async create(config: AnalystConfig): Promise<Result<LakeAnalyst, AnalystError>>

       async query<T>(sql: string): Promise<Result<QueryResult<T>, AnalystError>>

       async close(): Promise<void>
     }
     ```
  3. DuckDB-Wasm setup:
     - Initialise DuckDB-Wasm with Web Worker (or main thread for Node)
     - Configure S3 credentials: `SET s3_endpoint`, `SET s3_access_key_id`, etc.
     - Install + load `iceberg` extension if Nessie URI is provided
     - Install + load `httpfs` extension for S3 access
  4. Query execution:
     - Pass SQL to DuckDB → get Arrow result
     - Convert to `QueryResult<T>` with typed rows
     - Wrap in `Result`; DuckDB errors → `AnalystError`
  5. Package dependency: `@duckdb/duckdb-wasm`
- **Tests:**
  - Create analyst → query `SELECT 1` → returns `{ rows: [{ "1": 1 }] }`
  - Read Parquet from local file path → correct row count
  - S3 read from MinIO (integration, skipIf no Docker)
  - Error handling: invalid SQL → `AnalystError`
- **Done when:** DuckDB-Wasm initialises, reads Parquet from S3, returns typed results

---

### Task 4A.2: Union Read (hot SQLite + cold Iceberg)

- **Package:** `packages/analyst/`
- **Creates:**
  - `packages/analyst/src/union-reader.ts`
  - `packages/analyst/src/__tests__/union-reader.test.ts`
- **Modifies:**
  - `packages/analyst/src/index.ts` (add exports)
- **Dependencies:** Task 4A.1 + Phase 2C (LocalDB)
- **Implementation:**
  1. `src/union-reader.ts` — `UnionReader` class:
     ```typescript
     export class UnionReader {
       constructor(
         private analyst: LakeAnalyst,  // cold path (Iceberg/Parquet)
         private localDb: LocalDB,       // hot path (SQLite)
       )

       /** Query that merges local hot data with cold lake data */
       async query<T>(
         table: string,
         options?: {
           where?: string;
           orderBy?: string;
           limit?: number;
         },
       ): Promise<Result<QueryResult<T>, AnalystError>>
     }
     ```
  2. Union Read algorithm:
     - **Cold path:** Query Iceberg table via DuckDB:
       ```sql
       SELECT * FROM iceberg_scan('nessie:lakesync.{table}')
       WHERE {where}
       ```
     - **Hot path:** Query local SQLite for rows modified since last compaction:
       ```sql
       SELECT * FROM {table}
       WHERE _rowId IN (SELECT DISTINCT rowId FROM _sync_pending)
       ```
     - **Merge:** Union cold + hot, with hot rows taking precedence (by `_rowId`):
       - Cold rows NOT in hot set → include as-is
       - Hot rows → include (overrides cold)
       - Deleted rows (in pending queue as DELETE) → exclude
     - Apply `orderBy` and `limit` to merged result
  3. Merge strategy uses MOR (Merge-on-Read) as per ADR-003:
     - Base data from Iceberg (compacted)
     - Equality deletes applied
     - Uncommitted local deltas overlaid
  4. This gives a consistent view: what the world looks like if all local changes were committed
- **Tests:**
  - Cold only: no local changes → result matches Iceberg
  - Hot only: no Iceberg data → result matches SQLite
  - Union: cold base + local updates → merged correctly
  - Local delete overrides cold row
  - Order and limit applied after merge
- **Done when:** Union Read produces consistent merged view, all tests pass

---

### Task 4A.3: Time-travel queries

- **Package:** `packages/analyst/`
- **Creates:**
  - `packages/analyst/src/time-travel.ts`
  - `packages/analyst/src/__tests__/time-travel.test.ts`
- **Modifies:**
  - `packages/analyst/src/index.ts` (add exports)
- **Dependencies:** Task 4A.1
- **Implementation:**
  1. `src/time-travel.ts`:
     ```typescript
     export interface TimeTravelOptions {
       /** Query as of a specific snapshot ID */
       snapshotId?: number;
       /** Query as of a specific timestamp */
       asOf?: Date;
       /** Query as of a specific HLC timestamp */
       asOfHlc?: HLCTimestamp;
     }

     export class TimeTravelReader {
       constructor(private analyst: LakeAnalyst)

       async queryAt<T>(
         table: string,
         options: TimeTravelOptions,
         query?: { where?: string; orderBy?: string; limit?: number },
       ): Promise<Result<QueryResult<T>, AnalystError>>

       async listSnapshots(
         namespace: string[],
         table: string,
       ): Promise<Result<SnapshotInfo[], AnalystError>>

       async diff(
         table: string,
         fromSnapshot: number,
         toSnapshot: number,
       ): Promise<Result<QueryResult, AnalystError>>
     }
     ```
  2. `queryAt()`:
     - If `snapshotId`: `SELECT * FROM iceberg_scan('...', snapshot_id => {id})`
     - If `asOf`: find snapshot closest to timestamp, then query at that snapshot
     - If `asOfHlc`: convert HLC to wall clock time → find snapshot → query
  3. `listSnapshots()`:
     - Query Iceberg metadata via catalogue
     - Return list of `{ snapshotId, timestamp, summary }` for the table
  4. `diff()`:
     - Compare two snapshots: rows added, modified, deleted between them
     - Use DuckDB set operations:
       ```sql
       -- Added/modified in toSnapshot
       SELECT * FROM iceberg_scan('...', snapshot_id => {to})
       EXCEPT
       SELECT * FROM iceberg_scan('...', snapshot_id => {from})
       ```
  5. Time-travel is read-only — no writes through this interface
- **Tests:**
  - Query at snapshot ID → returns data as of that snapshot
  - Query at timestamp → finds correct snapshot
  - List snapshots → returns ordered list
  - Diff between snapshots → shows changes
  - Invalid snapshot ID → `AnalystError`
- **Done when:** Time-travel queries work against Iceberg snapshots, all tests pass

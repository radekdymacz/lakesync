# Phase 3A — Compaction

**Goal:** Implement Merge-on-Read (MOR) compaction that collapses accumulated delta files into base data files with equality deletes, reducing read amplification and storage cost.

**Depends on:** Phase 2B (Iceberg catalogue — snapshots must exist to compact)
**Blocks:** Phase 4A (Analyst — needs compacted base files for efficient reads)

---

## SEQUENTIAL GROUP (all tasks in order)

### Task 3A.1: Compaction logic

- **Package:** `packages/compactor/` (upgrade from placeholder)
- **Creates:**
  - `packages/compactor/src/compactor.ts`
  - `packages/compactor/src/types.ts`
  - `packages/compactor/src/index.ts`
  - `packages/compactor/src/__tests__/compactor.test.ts`
- **Modifies:**
  - `packages/compactor/package.json` (add dependencies)
  - `packages/compactor/tsconfig.json`
  - `packages/compactor/vitest.config.ts`
- **Dependencies:** none within phase (but requires 2A + 2B packages)
- **Implementation:**
  1. `src/types.ts`:
     ```typescript
     export interface CompactionConfig {
       /** Minimum number of delta files before compaction triggers */
       minDeltaFiles: number;       // default: 10
       /** Maximum number of delta files to compact in one pass */
       maxDeltaFiles: number;       // default: 100
       /** Target base file size in bytes */
       targetFileSizeBytes: number; // default: 128 * 1024 * 1024 (128 MB)
     }

     export interface CompactionResult {
       baseFilesWritten: number;
       deleteFilesWritten: number;
       deltaFilesCompacted: number;
       bytesRead: number;
       bytesWritten: number;
     }
     ```
  2. `src/compactor.ts` — `Compactor` class:
     ```typescript
     export class Compactor {
       constructor(
         private adapter: LakeAdapter,
         private catalogue: NessieCatalogueClient,
         private config: CompactionConfig,
       )

       async compact(namespace: string[], table: string): Promise<Result<CompactionResult, LakeSyncError>>
     }
     ```
  3. `compact()` algorithm:
     - Load current table metadata from catalogue
     - List delta files from current snapshot's manifest
     - If count < `minDeltaFiles` → skip (no compaction needed)
     - Read all delta files → `readParquetToDeltas()` for each
     - Group deltas by `rowKey(table, rowId)`
     - For each row: apply all deltas in HLC order using `applyDelta()`
       - If final state is DELETE → emit equality delete record
       - If final state is live → emit to base file
     - Write base data as Parquet file(s) (split if > `targetFileSizeBytes`)
     - Write equality delete file(s) as Parquet (rowId column only)
     - Commit new snapshot to catalogue:
       - Add base files + delete files
       - Remove compacted delta files from manifest
     - Return `CompactionResult`
  4. Use `Result` throughout; do NOT throw
- **Tests:**
  - 20 INSERT deltas → compact → 1 base file with 20 rows, 0 delete files
  - INSERT + UPDATE same row → compact → base file has latest values
  - INSERT + DELETE same row → compact → equality delete file, no base row
  - Mixed: 50 rows, 10 deleted → correct base + delete counts
  - Below threshold (`minDeltaFiles`) → skip, return zero result
- **Done when:** Compactor produces correct base + delete files, all tests pass

---

### Task 3A.2: Equality delete file handling

- **Package:** `packages/compactor/`
- **Creates:**
  - `packages/compactor/src/equality-delete.ts`
  - `packages/compactor/src/__tests__/equality-delete.test.ts`
- **Modifies:**
  - `packages/compactor/src/compactor.ts` (use equality delete module)
- **Dependencies:** Task 3A.1
- **Implementation:**
  1. `src/equality-delete.ts`:
     ```typescript
     export async function writeEqualityDeletes(
       deletedRowIds: Array<{ table: string; rowId: string }>,
       schema: TableSchema,
     ): Promise<Result<Uint8Array, FlushError>>
     ```
     - Iceberg equality delete file format:
       - Parquet file with only the equality columns (`_rowId`)
       - File metadata: `delete-type=equality-deletes`, `equality-ids=[fieldId]`
     - Use `writeDeltasToParquet()` with a synthetic schema or write directly via `parquet-wasm`
  2. `readEqualityDeletes(data: Uint8Array): Promise<Result<string[], FlushError>>`
     - Read Parquet → extract `_rowId` column → return list of deleted row IDs
  3. Integrate into `Compactor.compact()`:
     - After grouping and resolving: separate live rows from deleted rows
     - Write base file for live rows
     - Write equality delete file for deleted rows
     - Both files registered in catalogue snapshot
- **Tests:**
  - Write 5 deleted rowIds → read back → exact match
  - Empty deletes → no file written
  - Large batch (1000 deletes) → single file
- **Done when:** Equality delete files follow Iceberg spec, roundtrip correctly

---

### Task 3A.3: Maintenance operations

- **Package:** `packages/compactor/`
- **Creates:**
  - `packages/compactor/src/maintenance.ts`
  - `packages/compactor/src/__tests__/maintenance.test.ts`
- **Modifies:**
  - `packages/compactor/src/index.ts` (add exports)
- **Dependencies:** Task 3A.1 + 3A.2
- **Implementation:**
  1. `src/maintenance.ts`:
     ```typescript
     export class MaintenanceRunner {
       constructor(
         private compactor: Compactor,
         private adapter: LakeAdapter,
         private catalogue: NessieCatalogueClient,
       )

       /** Run full maintenance cycle: compact → expire → clean */
       async run(namespace: string[], table: string): Promise<Result<MaintenanceReport, LakeSyncError>>

       /** Remove snapshots older than retention period */
       async expireSnapshots(namespace: string[], table: string, retainCount: number): Promise<Result<number, LakeSyncError>>

       /** Delete orphaned files not referenced by any snapshot */
       async removeOrphans(namespace: string[], table: string): Promise<Result<number, LakeSyncError>>
     }
     ```
  2. Maintenance order (critical — must follow this sequence):
     1. **Compact** — merge delta files into base files
     2. **Expire snapshots** — mark old snapshots for deletion (keep last N)
     3. **Remove orphans** — delete files not referenced by any live snapshot
  3. `expireSnapshots()`:
     - List all snapshots from catalogue
     - Keep the most recent `retainCount` snapshots
     - Remove older snapshots via catalogue API
  4. `removeOrphans()`:
     - List all files in storage under the table prefix
     - List all files referenced by live snapshots
     - Delete files in storage but not in any snapshot
     - Safety: only delete files older than 24 hours (avoid race with in-progress flushes)
- **Tests:**
  - Full cycle: compact → expire → clean → storage has only live files
  - Expire with retainCount=2 → keeps 2 most recent snapshots
  - Orphan removal skips files < 24 hours old
  - No snapshots to expire → noop
- **Done when:** Full maintenance cycle runs correctly, storage is cleaned up

---

### Task 3A.4: Scheduling

- **Package:** `packages/compactor/` + `apps/gateway-worker/` (optional)
- **Creates:**
  - `packages/compactor/src/scheduler.ts`
- **Modifies:**
  - `packages/compactor/src/index.ts` (add exports)
  - `apps/gateway-worker/src/sync-gateway-do.ts` (optional: add compaction alarm)
- **Dependencies:** Task 3A.3
- **Implementation:**
  1. `src/scheduler.ts`:
     ```typescript
     export interface SchedulerConfig {
       compactionIntervalMs: number;  // default: 1 hour
       maintenanceIntervalMs: number; // default: 6 hours
     }

     export class CompactionScheduler {
       /** Check if compaction is needed and run if so */
       async tick(namespace: string[], table: string): Promise<Result<void, LakeSyncError>>

       /** Check if maintenance is needed and run if so */
       async maintain(namespace: string[], table: string): Promise<Result<void, LakeSyncError>>
     }
     ```
  2. For Cloudflare Workers: integrate with DO alarm system
     - After flush alarm, check if compaction is due
     - Run compaction in the same alarm handler (or schedule a separate alarm)
  3. For standalone: provide a `setInterval`-based runner for Node/Bun
  4. Compaction should be idempotent — safe to run multiple times
- **Tests:**
  - Scheduler respects interval (mock time)
  - Scheduler skips if below threshold
  - Idempotent: double-run produces same result
- **Done when:** Compaction runs on schedule, integrates with DO alarm or standalone timer

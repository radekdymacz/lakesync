# Phase 2C — SQLite WASM Client

**Goal:** Replace the in-memory client storage with SQLite WASM (OPFS-backed), add automatic change tracking, and upgrade the todo-app to demonstrate full offline-first sync.

**Depends on:** Phase 1 (complete)
**Blocks:** 3B (Schema Evolution)

---

## PARALLEL GROUP 1 (runs alongside Phase 2A)

### Task 2C.1: SQLite WASM integration

- **Package:** `packages/client/src/db/`
- **Creates:**
  - `packages/client/src/db/local-db.ts`
  - `packages/client/src/db/types.ts`
  - `packages/client/src/db/__tests__/local-db.test.ts`
- **Modifies:**
  - `packages/client/package.json` (add SQLite WASM dependency)
  - `packages/client/src/index.ts` (add db exports)
- **Dependencies:** none
- **Implementation:**
  1. **Library choice:** Use `@aspect-build/aspect-sqlite` or `wa-sqlite` with OPFS backend. Key requirements:
     - OPFS support (hard rule: no localStorage/sessionStorage)
     - Works in Web Workers (required for OPFS synchronous access handle)
     - Falls back to IndexedDB VFS for Safari (no OPFS sync access)
     - Has a Node/Bun-compatible mode for testing (memory VFS or better-sqlite3 shim)
  2. `src/db/types.ts`:
     ```typescript
     export interface DbConfig {
       name: string;
       /** "opfs" | "idb" | "memory" — auto-detected if not set */
       backend?: "opfs" | "idb" | "memory";
     }

     export class DbError extends LakeSyncError {
       constructor(message: string, cause?: Error) {
         super(message, "DB_ERROR", cause);
       }
     }

     export interface Transaction {
       exec(sql: string, params?: unknown[]): Promise<Result<void, DbError>>;
       query<T>(sql: string, params?: unknown[]): Promise<Result<T[], DbError>>;
     }
     ```
  3. `src/db/local-db.ts` — `LocalDB` class:
     ```typescript
     export class LocalDB {
       static async open(config: DbConfig): Promise<Result<LocalDB, DbError>>
       async exec(sql: string, params?: unknown[]): Promise<Result<void, DbError>>
       async query<T>(sql: string, params?: unknown[]): Promise<Result<T[], DbError>>
       async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<Result<T, DbError>>
       async close(): Promise<void>
     }
     ```
     - `open()`: detect environment (OPFS if available, else IDB, else memory)
     - All SQL operations wrapped in `Result<T, DbError>`
     - `transaction()`: BEGIN → fn(tx) → COMMIT, ROLLBACK on error
     - `close()`: release OPFS lock / close database
  4. For testing: use memory backend (`:memory:`) so tests run in Node/Bun without OPFS
- **Tests:**
  - `open()` + `close()` lifecycle
  - `exec()` CREATE TABLE + INSERT
  - `query<T>()` returns typed rows
  - `transaction()` commits on success, rolls back on error
  - Error handling: invalid SQL → `DbError`
  - Concurrent `query()` calls (no deadlock)
- **Done when:** `LocalDB` passes all tests in Bun with memory backend, types exported from client package

---

### Task 2C.2: Schema registry

- **Package:** `packages/client/src/db/`
- **Creates:**
  - `packages/client/src/db/schema-registry.ts`
  - `packages/client/src/db/__tests__/schema-registry.test.ts`
- **Modifies:**
  - `packages/client/src/index.ts` (add exports)
- **Dependencies:** Task 2C.1 (uses `LocalDB`)
- **Implementation:**
  1. `src/db/schema-registry.ts`:
     - On first use, create `_lakesync_meta` table:
       ```sql
       CREATE TABLE IF NOT EXISTS _lakesync_meta (
         table_name TEXT PRIMARY KEY,
         schema_version INTEGER NOT NULL DEFAULT 1,
         schema_json TEXT NOT NULL,
         updated_at TEXT NOT NULL
       )
       ```
     - `registerSchema(db: LocalDB, schema: TableSchema): Promise<Result<void, DbError>>`
       - Insert or update schema in `_lakesync_meta`
       - Create the user table with columns matching `TableSchema`
       - Column type mapping: `string`→TEXT, `number`→REAL, `boolean`→INTEGER, `json`→TEXT, `null`→TEXT
       - Always include `_rowId TEXT PRIMARY KEY`
     - `getSchema(db: LocalDB, table: string): Promise<Result<TableSchema | null, DbError>>`
       - Query `_lakesync_meta` → parse `schema_json` → return `TableSchema`
     - `migrateSchema(db: LocalDB, oldSchema: TableSchema, newSchema: TableSchema): Promise<Result<void, DbError>>`
       - Compare old and new: find added columns
       - For each added column: `ALTER TABLE {table} ADD COLUMN {name} {type}`
       - Only ADD COLUMN supported (nullable); other changes return `SchemaError`
       - Update `_lakesync_meta` with new schema and incremented version
  2. All operations within transactions for consistency
- **Tests:**
  - Register schema → table created → schema retrievable
  - Register same schema twice → idempotent
  - Migrate: add column → ALTER succeeds → schema version incremented
  - Migrate: remove column → `SchemaError`
  - Migrate: change type → `SchemaError`
- **Done when:** Schema registry manages table lifecycle, all tests pass

---

## SEQUENTIAL (after Group 1)

### Task 2C.3: Change tracking

- **Package:** `packages/client/src/sync/`
- **Creates:**
  - `packages/client/src/sync/tracker.ts`
  - `packages/client/src/sync/__tests__/tracker.test.ts`
- **Modifies:**
  - `packages/client/src/index.ts` (add sync exports)
- **Dependencies:** Task 2C.1 + 2C.2 (uses `LocalDB` + schema registry)
- **Implementation:**
  1. `src/sync/tracker.ts` — `SyncTracker` class:
     ```typescript
     export class SyncTracker {
       constructor(
         private db: LocalDB,
         private queue: SyncQueue,
         private hlc: HLC,
         private clientId: string,
       )

       async insert(table: string, rowId: string, data: Record<string, unknown>): Promise<Result<void, LakeSyncError>>
       async update(table: string, rowId: string, data: Record<string, unknown>): Promise<Result<void, LakeSyncError>>
       async delete(table: string, rowId: string): Promise<Result<void, LakeSyncError>>
       async query<T>(sql: string, params?: unknown[]): Promise<Result<T[], DbError>>
     }
     ```
  2. `insert()`:
     - Write row to SQLite: `INSERT INTO {table} (_rowId, ...) VALUES (?, ...)`
     - `extractDelta(null, data, { table, rowId, clientId, hlc })` → queue.push()
  3. `update()`:
     - Read current row from SQLite
     - Write updated row: `UPDATE {table} SET ... WHERE _rowId = ?`
     - `extractDelta(oldRow, newData, ...)` → only changed columns → queue.push()
  4. `delete()`:
     - Read current row (for delta extraction)
     - `DELETE FROM {table} WHERE _rowId = ?`
     - `extractDelta(oldRow, null, ...)` → queue.push()
  5. `query()` — passthrough to `LocalDB.query()` for reads
  6. Create `_sync_cursor` table:
     ```sql
     CREATE TABLE IF NOT EXISTS _sync_cursor (
       table_name TEXT PRIMARY KEY,
       last_synced_hlc TEXT NOT NULL
     )
     ```
  7. Application-level change interception (not SQLite triggers) — more portable and explicit
- **Tests:**
  - `insert()` → row in SQLite + delta in queue with INSERT op
  - `update()` partial → delta has only changed columns
  - `update()` no change → no delta queued
  - `delete()` → row removed + DELETE delta in queue
  - Queue depth matches number of tracked changes
- **Done when:** SyncTracker wraps all CRUD with automatic delta extraction, all tests pass

---

### Task 2C.4: Apply remote deltas

- **Package:** `packages/client/src/sync/`
- **Creates:**
  - `packages/client/src/sync/applier.ts`
  - `packages/client/src/sync/__tests__/applier.test.ts`
- **Modifies:**
  - `packages/client/src/index.ts` (add exports)
- **Dependencies:** Task 2C.3 (uses `SyncTracker` + `LocalDB`)
- **Implementation:**
  1. `src/sync/applier.ts`:
     ```typescript
     export async function applyRemoteDeltas(
       db: LocalDB,
       deltas: RowDelta[],
       resolver: ConflictResolver,
       pendingQueue: SyncQueue,
     ): Promise<Result<number, LakeSyncError>>
     ```
  2. For each remote delta:
     - Check if the same `rowId` has a pending local delta in the queue
     - If conflict: call `resolver.resolve(localDelta, remoteDelta)`
       - If remote wins: apply remote to SQLite, remove local from queue
       - If local wins: skip remote, keep local in queue
     - If no conflict: apply remote delta directly:
       - INSERT → `INSERT INTO {table} (...) VALUES (...)`
       - UPDATE → `UPDATE {table} SET ... WHERE _rowId = ?`
       - DELETE → `DELETE FROM {table} WHERE _rowId = ?`
  3. After batch: update `_sync_cursor` with max HLC from applied deltas
  4. Return count of applied deltas
  5. Run the whole batch inside a transaction for atomicity
- **Tests:**
  - Apply INSERT → row appears in SQLite
  - Apply UPDATE → row updated in SQLite
  - Apply DELETE → row removed from SQLite
  - Conflict: local pending + remote → LWW resolves correctly
  - Cursor advances after batch
  - Empty batch → cursor unchanged, returns 0
- **Done when:** Remote deltas apply to SQLite with conflict resolution, all tests pass

---

### Task 2C.5: Todo app upgrade

- **Package:** `apps/examples/todo-app/`
- **Creates:** none (modifies existing files)
- **Modifies:**
  - `apps/examples/todo-app/src/db.ts` — replace `TodoDB` (Map) with `LocalDB` + `SyncTracker`
  - `apps/examples/todo-app/src/sync.ts` — replace `SyncManager` with new sync using tracker + applier
  - `apps/examples/todo-app/src/main.ts` — update initialisation
  - `apps/examples/todo-app/src/ui.ts` — add sync status display
  - `apps/examples/todo-app/package.json` — ensure dependencies
- **Dependencies:** Task 2C.3 + 2C.4
- **Implementation:**
  1. Replace `db.ts`:
     - Remove `TodoDB` class (in-memory Map)
     - Create init function that opens `LocalDB`, registers todo schema, returns `SyncTracker`
     - Todo schema: `{ table: "todos", columns: [{ name: "title", type: "string" }, { name: "completed", type: "boolean" }, { name: "created_at", type: "string" }, { name: "updated_at", type: "string" }] }`
  2. Replace `sync.ts`:
     - Remove old `SyncManager` class
     - New `SyncCoordinator`:
       - Uses `SyncTracker` for local writes
       - Uses `applyRemoteDeltas()` for incoming
       - Periodic pull from gateway → apply remote deltas
       - Push pending queue to gateway on each local write
       - Flush trigger (manual button or timer)
  3. Update `ui.ts`:
     - Show sync status: "synced" / "pending: N" / "syncing..."
     - Show queue depth from `SyncQueue.depth()`
     - Show last sync time
     - Keep existing todo CRUD functionality
  4. Update `main.ts`:
     - Async initialisation: open DB → register schema → create tracker → create coordinator
     - Wire up UI with new coordinator
- **Tests:** Manual testing (Vite dev server). Verify:
  - Todos persist across page refresh (OPFS/IndexedDB)
  - Offline CRUD → reconnect → sync → data reaches gateway buffer
  - Sync status updates in real time
- **Done when:** Todo app uses SQLite WASM, tracks changes automatically, shows sync status

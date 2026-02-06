# Phase 3B — Schema Evolution

**Goal:** Support safe schema changes (initially: adding nullable columns) across the distributed system — from client SQLite through gateway to Iceberg catalogue — without breaking existing data or sync.

**Depends on:** Phase 2B (Iceberg catalogue) + Phase 2C (SQLite client with schema registry)
**Blocks:** none

---

## SEQUENTIAL GROUP (all tasks in order)

### Task 3B.1: Server-side schema versioning

- **Package:** `packages/gateway/` + `packages/catalogue/`
- **Creates:**
  - `packages/gateway/src/schema-manager.ts`
  - `packages/gateway/src/__tests__/schema-manager.test.ts`
- **Modifies:**
  - `packages/gateway/src/types.ts` (add schema version to config)
  - `packages/gateway/src/gateway.ts` (schema validation on push)
  - `packages/catalogue/src/nessie-client.ts` (schema evolution API)
- **Dependencies:** none within phase
- **Implementation:**
  1. `src/schema-manager.ts` — `SchemaManager` class:
     ```typescript
     export class SchemaManager {
       constructor(
         private catalogue: NessieCatalogueClient,
       )

       /** Get current schema version for a table */
       async getCurrentSchema(namespace: string[], table: string): Promise<Result<{ schema: TableSchema; version: number }, LakeSyncError>>

       /** Evolve schema: add new nullable columns */
       async evolveSchema(
         namespace: string[],
         table: string,
         newSchema: TableSchema,
       ): Promise<Result<{ version: number }, LakeSyncError>>

       /** Validate that incoming deltas are compatible with current schema */
       validateDelta(delta: RowDelta, schema: TableSchema): Result<void, SchemaError>
     }
     ```
  2. Schema evolution rules (strict, safe-only):
     - **Allowed:** Add nullable column (no default required)
     - **Forbidden:** Remove column, rename column, change type, add required column
     - Return `SchemaError` for forbidden changes
  3. `validateDelta()`:
     - Check that all columns in the delta exist in the schema
     - Unknown columns → `SchemaError` (reject, don't silently drop)
     - Missing columns in delta → fine (column-level deltas are sparse)
  4. Integrate into `SyncGateway.handlePush()`:
     - If `SchemaManager` is configured, validate each delta against current schema
     - Reject push if any delta has unknown columns
     - Return schema version in push response so client knows if it's behind
  5. Catalogue schema evolution:
     - Map `TableSchema` change to Iceberg `UpdateSchema` operation
     - Use Nessie's table update endpoint with schema evolution
     - Iceberg handles schema evolution natively (field IDs remain stable)
- **Tests:**
  - Add column → schema version incremented → Iceberg schema updated
  - Remove column → `SchemaError`
  - Push with unknown column → rejected
  - Push with subset of columns → accepted (sparse deltas)
  - Concurrent evolution → optimistic concurrency (409 + retry)
- **Done when:** Schema changes propagate safely from gateway to Iceberg, all tests pass

---

### Task 3B.2: Client schema negotiation + ALTER TABLE

- **Package:** `packages/client/`
- **Creates:**
  - `packages/client/src/sync/schema-sync.ts`
  - `packages/client/src/sync/__tests__/schema-sync.test.ts`
- **Modifies:**
  - `packages/client/src/db/schema-registry.ts` (add version comparison)
  - `packages/client/src/sync/tracker.ts` (schema version awareness)
- **Dependencies:** Task 3B.1 + Task 2C.2
- **Implementation:**
  1. `src/sync/schema-sync.ts` — `SchemaSynchroniser` class:
     ```typescript
     export class SchemaSynchroniser {
       constructor(
         private db: LocalDB,
         private registry: SchemaRegistry,
       )

       /** Compare local schema version with server, apply migrations if behind */
       async synchronise(
         serverSchema: TableSchema,
         serverVersion: number,
       ): Promise<Result<void, LakeSyncError>>
     }
     ```
  2. `synchronise()` flow:
     - Get local schema version from `_lakesync_meta`
     - If local version >= server version → noop
     - If local version < server version:
       - Diff schemas → find added columns
       - Call `migrateSchema()` from schema registry (Task 2C.2)
       - This runs `ALTER TABLE ... ADD COLUMN ...` for each new column
       - Update local schema version
  3. Integrate with sync pull:
     - After each pull response, check if server returned a new schema version
     - If yes, run `SchemaSynchroniser.synchronise()` before applying deltas
     - This ensures local SQLite has the new column before deltas reference it
  4. Handle edge case: client pushes delta with new column that server doesn't know yet
     - Server rejects with `SchemaError`
     - Client should surface this as a sync error (user needs to coordinate schema change)
- **Tests:**
  - Server adds column → client syncs → ALTER TABLE runs → new column usable
  - No change → noop
  - Server removes column → error surfaced (not supported)
  - Client ahead of server → noop (local schema is superset)
  - Migration within transaction → rollback on failure
- **Done when:** Client automatically picks up schema changes from server, all tests pass

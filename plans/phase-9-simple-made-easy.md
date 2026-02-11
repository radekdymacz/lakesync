# Phase 9 — Simple Made Easy Refactor

## Goal
Decompose God classes, eliminate complecting, apply Rich Hickey's principles. Keep all features. No backwards-compat shims needed but preserve public API surface where possible.

## Constraints
- All existing tests must pass after each task
- Public exports from each package should remain stable (types can change)
- `Result<T, E>` pattern everywhere — no new throws
- British English in comments/docs

---

## PARALLEL GROUP A (4 agents, independent packages)

### Task 1: Core Types — ConnectorConfig + Registries
**Package:** `packages/core`
**Files:** `connector/types.ts`, `connector/validate.ts`, `connector/registry.ts`, `connector/index.ts`, `create-poller.ts`, `connector/register-builtin.ts`

**Changes:**
1. **ConnectorConfig → discriminated union:**
   ```typescript
   type ConnectorConfig =
     | { type: "postgres"; name: string; postgres: PostgresConnectorConfig; ingest?: ConnectorIngestConfig }
     | { type: "mysql"; name: string; mysql: MySQLConnectorConfig; ingest?: ConnectorIngestConfig }
     | { type: "bigquery"; name: string; bigquery: BigQueryConnectorConfig; ingest?: ConnectorIngestConfig }
     | { type: "jira"; name: string; jira: JiraConnectorConfig; ingest?: ConnectorIngestConfig }
     | { type: "salesforce"; name: string; salesforce: SalesforceConnectorConfig; ingest?: ConnectorIngestConfig }
   ```
   Each variant carries exactly its own config. No optional fields from other types.

2. **validateConnectorConfig → type-driven decomposition:**
   - Create per-type validators: `validatePostgresConfig()`, `validateMySQLConfig()`, etc.
   - Main validator: switch on `type`, delegate to per-type validator. TypeScript enforces exhaustiveness.
   - Separate structural validation from business rules.

3. **Registry → explicit values:**
   - Change `descriptors` Map to a builder: `createConnectorRegistry(descriptors: ConnectorDescriptor[])` returns `ConnectorRegistry` value.
   - `ConnectorRegistry` is a plain object with `get(type)`, `list()` methods — no mutable state.
   - `createPollerRegistry(factories: Map<string, PollerFactory>)` returns `PollerRegistry` value.
   - Remove side-effect `import "./register-builtin"` from `connector/index.ts`.
   - Export `defaultConnectorRegistry` and `defaultPollerRegistry` as pre-built values.
   - `createPoller()` takes optional registry param, defaults to `defaultPollerRegistry`.

4. **Update consumers:**
   - `gateway-server/src/server.ts` `handleRegisterConnector()` — use discriminated union, access config directly (e.g. `config.postgres` when `config.type === "postgres"`).
   - `connector-jira`, `connector-salesforce` — update poller factory signatures.

**Tests:** Run `bun run test --filter core && bun run test --filter gateway-server && bun run test --filter connector-jira && bun run test --filter connector-salesforce`

---

### Task 2: Adapter — Extract Shared Materialise Algorithm
**Package:** `packages/adapter`
**Files:** `materialise.ts`, `postgres.ts`, `mysql.ts`, `bigquery.ts`, `fan-out.ts`, `lifecycle.ts`

**Changes:**
1. **Create `MaterialiseExecutor`** in `materialise.ts`:
   - Interface `SqlDialect`:
     ```typescript
     interface SqlDialect {
       createDestinationTable(dest: string, columns: ColumnDef[], pk: string[], softDelete: boolean): string;
       queryDeltaHistory(table: string, rowIds: string[]): { sql: string; params: unknown[] };
       buildUpsert(dest: string, columns: string[], conflictCols: string[], softDelete: boolean, rows: Row[]): { sql: string; params: unknown[] };
       buildDelete(dest: string, ids: string[], pk: string[], softDelete: boolean): { sql: string; params: unknown[] };
     }
     ```
   - `executeMaterialise(client: QueryExecutor, dialect: SqlDialect, deltas: RowDelta[], schemas: ReadonlyArray<TableSchema>): Promise<Result<void, AdapterError>>`
   - This function contains the shared algorithm: group by table → build schema index → for each table: create dest table → query history → merge → upsert → delete.
   - `QueryExecutor` interface: `{ query(sql: string, params: unknown[]): Promise<Result<Row[], AdapterError>> }`

2. **Create dialect implementations:**
   - `PostgresSqlDialect` — `$1` params, `ON CONFLICT DO UPDATE`, `JSONB`, `TIMESTAMPTZ`
   - `MySqlDialect` — `?` params, `ON DUPLICATE KEY UPDATE`, `JSON`, `TIMESTAMP`
   - `BigQuerySqlDialect` — `@name` params, `MERGE`, `JSON`, `TIMESTAMP`, `CLUSTER BY`

3. **Refactor each adapter's `materialise()`** to delegate:
   ```typescript
   async materialise(deltas, schemas) {
     return executeMaterialise(this.queryExecutor, this.dialect, deltas, schemas);
   }
   ```

4. **Fix FanOut/Lifecycle Materialisable claims:**
   - Remove `implements Materialisable` from class declaration.
   - Instead, conditionally implement: if primary/hot `isMaterialisable()`, delegate. Otherwise don't expose `materialise` method.
   - Use a factory function or conditional class mixin so `isMaterialisable(fanOut)` returns `true` only when primary is actually materialisable.
   - Simplest approach: keep the runtime check but make `isMaterialisable()` on FanOut/Lifecycle check the inner adapter:
     ```typescript
     // In FanOutAdapter, add a materialise method that's only callable when primary supports it
     // But make isMaterialisable() actually check the primary:
     get canMaterialise(): boolean { return isMaterialisable(this.primary); }
     ```
     Actually simplest: just don't declare `implements Materialisable`. The duck-type guard `isMaterialisable()` already checks for the method. Keep the `materialise()` method but add an early return with Ok if inner doesn't support it. The type guard will still return true (method exists), but that's correct — the method handles it gracefully. Document this explicitly.

**Tests:** Run `bun run test --filter adapter`

---

### Task 3: Client — Decompose SyncCoordinator + React Fixes
**Package:** `packages/client`, `packages/react`
**Files:** `sync/coordinator.ts`, `create-client.ts`, `queue/memory-queue.ts`, `queue/memory-action-queue.ts`, `react/use-action.ts`, `react/use-sync-status.ts`, `react/context.ts`

**Changes:**
1. **Extract `AutoSyncScheduler`** from SyncCoordinator:
   - New file `sync/auto-sync.ts`
   - Takes `syncFn: () => Promise<void>`, `intervalMs: number`
   - Methods: `start()`, `stop()`, `get isRunning`
   - Handles visibility change listener
   - SyncCoordinator delegates to this instead of owning timer/visibility state

2. **Extract `ActionProcessor`** from SyncCoordinator:
   - New file `sync/action-processor.ts`
   - Takes `actionQueue: ActionQueue`, `transport: SyncTransport`, `maxRetries: number`
   - Methods: `enqueue(params)`, `processQueue()`, `describeActions()`, `listConnectorTypes()`
   - Emits `onActionComplete` events via callback
   - SyncCoordinator delegates action concerns here

3. **Expose coordinator state as readable snapshot:**
   ```typescript
   get state(): SyncState {
     return { syncing: this.syncing, lastSyncTime: this._lastSyncTime, lastSyncedHlc: this.lastSyncedHlc };
   }
   ```
   Add `onSyncStart` event so hooks know when sync begins (not just ends).

4. **Unify queues → `MemoryOutbox<T>`:**
   - New file `queue/memory-outbox.ts`
   - Generic over entry type `T`
   - `MemoryQueue = MemoryOutbox<RowDelta>` (thin alias)
   - `MemoryActionQueue = MemoryOutbox<Action>` (thin alias)
   - Keep old class names as re-exports for API stability.

5. **Fix `useAction` identity bug:**
   - Track `pendingActionId` ref inside the hook.
   - In `onActionComplete` handler, only update state if `actionId === pendingActionId.current`.

6. **Fix `useSyncStatus`:**
   - Read `coordinator.state` directly instead of reconstructing from events.
   - Subscribe to events only for invalidation trigger (re-read the state snapshot).
   - Add `onSyncStart` listener to know when syncing begins.

7. **Split React context:**
   - `LakeSyncStableContext`: coordinator, tracker (never changes)
   - `LakeSyncDataContext`: dataVersion, invalidate (changes on every delta)
   - `useLakeSync()` reads stable context.
   - `useQuery`/`useMutation` read data context.
   - `useSyncStatus`/`useAction` read stable context only (no unnecessary re-renders).

8. **Fix `createClient`:** Don't auto-start sync in constructor. Return client, let caller call `coordinator.startAutoSync()`. Or keep current behaviour with a `start: false` option.

**Tests:** Run `bun run test --filter client && bun run test --filter react`

---

### Task 4: Gateway — Decompose SyncGateway
**Package:** `packages/gateway`
**Files:** `gateway.ts`, `buffer.ts`, `action-dispatcher.ts`

**Changes:**
1. **Make SyncGateway a thin composition:**
   - SyncGateway constructor still takes same `GatewayConfig` (API stability).
   - Internally creates and composes: `DeltaBuffer`, `ActionDispatcher`, `SchemaManager`, `SourceRegistry`, `FlushCoordinator`.
   - Public methods delegate to composed modules. No logic in gateway itself.

2. **Extract `SourceRegistry`:**
   - New file `source-registry.ts`
   - Simple Map wrapper: `register(name, adapter)`, `unregister(name)`, `get(name)`, `list()`
   - Moves source adapter management out of gateway.

3. **Extract `FlushCoordinator`:**
   - New file `flush-coordinator.ts`
   - Owns the `flushing` state (or better: uses a queue/lock).
   - Methods: `flush(buffer, adapter, deps)`, `flushTable(table, buffer, adapter, deps)`
   - Eliminates the duplicated DB/Lake branching in gateway's flush() — FlushCoordinator calls `flushEntries()` with appropriate params regardless of adapter type.

4. **Refactor `handlePush` as pipeline:**
   - Extract pure functions: `checkBackpressure()`, `filterDuplicates()`, `validateSchemas()`, `validateHLCs()`, `resolveConflicts()`.
   - `handlePush` becomes: pipe input through these functions, then `buffer.append()`.
   - Each step is independently testable.

5. **Improve `DeltaBuffer.drain()`:**
   - Add `snapshot(): { entries: RowDelta[], byteSize: number }` — returns data without clearing.
   - Add `clear()` — resets buffer.
   - `drain()` remains as convenience (`snapshot` + `clear`) but `FlushCoordinator` uses `snapshot` + `clear` separately for transactional flush.

6. **Add TTL to `ActionDispatcher` caches:**
   - `executedActions` Set → LRU with max size (e.g. 10_000).
   - `idempotencyMap` Map → entries expire after configurable TTL (e.g. 5 min).
   - Simple implementation: on each dispatch, evict entries older than TTL.

**Tests:** Run `bun run test --filter gateway`

---

## SEQUENTIAL GROUP B (after Group A, 2 agents)

### Task 5: Gateway-Server — Decompose GatewayServer
**Package:** `packages/gateway-server`
**Depends on:** Task 4 (SyncGateway decomposition)
**Files:** `server.ts`

**Changes:**
1. **Extract middleware chain:**
   - `auth-middleware.ts`: JWT validation, returns 401 on failure.
   - `cors-middleware.ts`: CORS header handling.
   - `router.ts`: URL pattern matching → handler dispatch. Replace the switch statement.
   - Main server composes: `cors → auth → route → handler`.

2. **Extract `WebSocketManager`:**
   - `ws-manager.ts`: handles upgrade, message parsing, broadcast, client tracking.
   - Decouples WebSocket protocol from HTTP server lifecycle.

3. **Extract `ConnectorManager`:**
   - `connector-manager.ts`: registration, poller lifecycle, adapter creation.
   - Uses `createPoller()` from core instead of manual if/else chains.
   - Each connector type is handled via the registry pattern from Task 1.

4. **Fix `AdapterBasedLock`:**
   - Create a proper `DistributedLock` implementation that doesn't abuse the delta protocol.
   - Option A: Use a dedicated `__lakesync_meta` table with proper lock columns (not delta protocol).
   - Option B: Use adapter's raw query capability for advisory locks (Postgres: `pg_advisory_lock`, MySQL: `GET_LOCK`).

5. **Fix rehydration:**
   - Don't construct fake `SyncPush`. Instead, add a `DeltaBuffer.restore(deltas)` method that loads persisted deltas directly into the buffer without going through push validation/HLC/conflict resolution.

**Tests:** Run `bun run test --filter gateway-server`

---

### Task 6: Core — Decompose BaseSourcePoller
**Package:** `packages/core`, `packages/connector-jira`, `packages/connector-salesforce`
**Depends on:** Task 1 (registries)
**Files:** `base-poller.ts`, connector poller implementations

**Changes:**
1. **Extract `PollingScheduler`:**
   - New file `polling/scheduler.ts`
   - Takes `pollFn: () => Promise<void>`, `intervalMs: number`
   - Methods: `start()`, `stop()`, `pollOnce()`, `get isRunning`
   - Pure lifecycle management — no knowledge of deltas or gateways.

2. **Extract `ChunkedPusher`:**
   - New file `polling/chunked-pusher.ts`
   - Takes `target: PushTarget`, `chunkSize: number`, `clientId: string`
   - Methods: `pushDeltas(deltas)` — chunks and pushes
   - Handles backpressure retry (flush + retry once)

3. **Extract `PressureManager`:**
   - New file `polling/pressure-manager.ts`
   - Takes `target: IngestTarget`, `flushThreshold: number`
   - Methods: `checkAndFlush()` — check buffer pressure, flush if needed
   - `shouldFlushBefore(additionalBytes)` — check if push would exceed threshold

4. **Refactor `BaseSourcePoller`:**
   - Becomes a thin composition of `PollingScheduler` + `ChunkedPusher` + `PressureManager`.
   - Subclasses still implement `poll()`, `getCursorState()`, `setCursorState()`.
   - `pushDeltas()` delegates to `ChunkedPusher` which consults `PressureManager`.
   - Remove runtime `isIngestTarget` check — if target has flush capability, PressureManager is created; otherwise it's null.

5. **Update connector implementations:**
   - `connector-jira/src/jira-poller.ts` — verify still works with refactored base.
   - `connector-salesforce/src/salesforce-poller.ts` — same.

**Tests:** Run `bun run test --filter core && bun run test --filter connector`

---

## Post-Refactor Verification

After all tasks complete:
```bash
bun run typecheck     # full monorepo type check
bun run test          # all unit tests
bun run lint          # biome check
bun run build         # full build
```

# LakeSync

## Vision
**Declare what data goes where. The engine handles the rest.**

LakeSync is a declarative data sync engine. Adapters connect any readable or writable system — Postgres, BigQuery, Iceberg, CloudWatch, Stripe, local SQLite. Sync rules control what data flows between them. Every adapter is both a source and a destination; local SQLite (via the client SDK) is one destination among many, not the whole product.

Current adapters: S3/R2 (Iceberg), Postgres, MySQL, BigQuery. The adapter interface is the extension point for any data source or destination.

## Internal Context (not public)
LakeSync is the data backbone for:
- **CompanyOS** (https://company-os.pages.dev/) — company operating system web app
- **AgentOS** — Claude Code Teams organised per department

Real use cases driving development:
- Agents analysing CloudWatch logs — sync a filtered subset locally, reason over it
- Financial data in BigQuery — agent or dashboard gets exactly the slice it needs, or materialises results back into Postgres
- Offline web app — CompanyOS works on a plane, catches up when back
- Cross-backend flows — Iceberg → BigQuery, Postgres → Iceberg, any source → any destination

LakeSync ensures data flows between all systems: web apps, agents, and backends. The library itself is the public-facing product; CompanyOS and AgentOS are the primary consumers. Focus development on the library, not the consumers.

## Monorepo
TurboRepo + Bun. Packages in `packages/`, apps in `apps/`.

## Architecture
- 14 packages: core, client, gateway, gateway-server, adapter, proto, parquet, catalogue, compactor, analyst, lakesync, react, connector-jira, connector-salesforce
- 3 apps: examples/todo-app (Vite + vanilla TS), gateway-worker (Cloudflare Workers + DO), docs (Fumadocs + Next.js)
- All phases complete (1 through 8): HLC, Delta, Result, Conflict, Queue, Gateway, Proto, Adapter, Parquet, Catalogue, SQLite client, CF Workers, Compaction, Schema Evolution, Analyst, Sync Rules, Initial Sync, Database Adapters, Table Sharding, Self-Hosted Gateway, Fan-Out, Lifecycle, Materialise, Actions, Connectors, React Hooks, WebSocket, Clustering

### Adapters — Any Data Source (packages/adapter)
- Adapter interfaces (`LakeAdapter`, `DatabaseAdapter`, `isDatabaseAdapter`, `Materialisable`, `isMaterialisable`) are defined in `@lakesync/core` (`packages/core/src/adapter-types.ts`). `@lakesync/adapter` re-exports them for backward compatibility. Implementations remain in `@lakesync/adapter`.
- `LakeAdapter` interface: `putObject`, `getObject`, `headObject`, `listObjects`, `deleteObject`, `deleteObjects`
- `DatabaseAdapter` interface: `insertDeltas`, `queryDeltasSince`, `getLatestState`, `ensureSchema` — adapters are both sources AND destinations
- Implementations: S3Adapter (S3/R2/MinIO), PostgresAdapter, MySQLAdapter, BigQueryAdapter, CompositeAdapter, FanOutAdapter, LifecycleAdapter
- `AdapterFactoryRegistry` pattern: `createAdapterFactoryRegistry()` creates an immutable registry; `defaultAdapterFactoryRegistry()` includes postgres/mysql/bigquery; `createDatabaseAdapter(config, registry?)` uses the registry (defaults to built-in)
- `FanOutAdapter`: writes to primary (sync), replicates to secondaries (async best-effort)
- `LifecycleAdapter`: hot/cold tiers based on delta age; `migrateToTier()` moves aged-out deltas
- `migrateAdapter()`: copies data between any two adapters
- `Materialisable` interface: opt-in capability for adapters that can materialise deltas into queryable destination tables
- `isMaterialisable()` type guard: duck-typed check for materialisation support
- All three database adapters (PostgresAdapter, MySQLAdapter, BigQueryAdapter) implement `Materialisable` — creates destination tables with synced columns + `props JSONB` + `synced_at` via the generic `SqlDialect` + `executeMaterialise()` pattern
- Materialisation is auto-called after flush (non-fatal — failures are logged but never fail the flush)
- Gateway takes an optional adapter — flush target is fully decoupled from sync logic
- Gateway supports `sourceAdapters` — named DatabaseAdapters for adapter-sourced pull
- The adapter interface is the extension point — any data source that can be read from can become an adapter (CloudWatch, Stripe, etc.)
- Same client code regardless of backend; swap adapters at the gateway level

### Key Patterns
- `Result<T, E>` — never throw from public APIs
- `HLCTimestamp` = branded bigint (48-bit wall + 16-bit counter)
- Column-level LWW with clientId tiebreak
- DeltaBuffer = atomic `BufferSnapshot` pattern (immutable state swapped atomically, like SchemaManager)
- Deterministic deltaId via SHA-256 of stable-stringified payload
- `TableSchema` = `{ table: string; columns: Array<{ name: string; type: ... }> }`

### Actions (packages/core/src/action/ + packages/gateway)
- Imperative action system for executing commands against external systems (e.g. create PR, send message)
- `Action`: unique actionId (SHA-256), connector name, actionType, params, optional idempotencyKey
- `ActionHandler` interface: `supportedActions` (descriptors) + `executeAction()` — implemented by connectors
- `ActionDescriptor`: describes an action type with optional JSON Schema for params
- `ActionDispatcher` (gateway): dispatches actions to registered handlers with idempotency dedup (actionId + idempotencyKey)
- `SyncGateway.handleAction()`: delegates to `ActionDispatcher`, returns `ActionResponse` with per-action results
- Discovery: `describeActions()` returns all registered connectors and their supported action types
- Routes: `POST /sync/:id/action` (execute), `GET /sync/:id/actions` (discover)

### Connectors (packages/core/src/connector/ + packages/connector-*)
- `ConnectorConfig`: open discriminated union — `ConnectorConfigBase` (open `type: string`) plus typed variants for known types, plus a catch-all `(ConnectorConfigBase & Record<string, unknown>)` for extensibility. Custom connector types work without modifying core.
- Supported types: `"postgres"`, `"mysql"`, `"bigquery"`, `"jira"`, `"salesforce"` (`CONNECTOR_TYPES` const)
- `ConnectorIngestConfig`: tables to poll, interval, chunk size, memory budget
- `ConnectorIngestTable`: target table + SQL query + row ID column + strategy (`cursor` or `diff`)
- Dynamic registration: `POST /admin/connectors/:gatewayId` registers connector, creates adapter, starts poller
- `createPoller(config, gateway, registry)` factory: requires an explicit `PollerRegistry` parameter. No global mutable registry.
- `BaseSourcePoller` (packages/core): abstract class with start/stop lifecycle, chunked push, memory-aware flush
- `PushTarget` / `IngestTarget` interfaces: pollers push to gateway without direct dependency
- `CallbackPushTarget`: lightweight PushTarget for testing or simple integrations
- **connector-jira**: `JiraSourcePoller` extends `BaseSourcePoller`, polls Jira Cloud REST API (issues, projects, comments)
- **connector-salesforce**: `SalesforceSourcePoller` extends `BaseSourcePoller`, polls Salesforce SOQL (accounts, contacts, opportunities, leads)
- Both connectors export poller factories as named exports (`jiraPollerFactory`, `salesforcePollerFactory`) — build a `PollerRegistry` via `createPollerRegistry()` and pass it to `createPoller()`

### Materialise Protocol
- `Materialisable` interface and `isMaterialisable()` type guard are defined in `@lakesync/core` (`packages/core/src/adapter-types.ts`)
- Generic algorithm: `executeMaterialise(executor, dialect, deltas, schemas)` in `@lakesync/adapter` — dialect-agnostic, works with any SQL destination
- `SqlDialect` interface: `createDestinationTable`, `queryDeltaHistory`, `buildUpsert`, `buildDelete` — each database adapter provides its own dialect
- `QueryExecutor` interface: minimal `query()` + `queryRows()` — abstracts any SQL connection
- All three database adapters implement `Materialisable`: PostgresAdapter (`PostgresSqlDialect`), MySQLAdapter (`MySqlDialect`), BigQueryAdapter (`BigQuerySqlDialect`)
- Destination tables follow the hybrid column model: synced columns + `props JSONB DEFAULT '{}'` + `synced_at`
- Adding materialisation to a new database = implement 4 `SqlDialect` methods + a `QueryExecutor`
- FanOutAdapter delegates materialisation to primary (sync) + secondaries (async best-effort)
- LifecycleAdapter delegates materialisation to the hot tier only
- Auto-called after successful delta flush (non-fatal — failures are warned, never fail the flush)

### FlushQueue (packages/gateway)
- `FlushQueue` interface: producer-only queue sitting between flush and materialisation — `publish(entries, context)` returns `Result<void, FlushQueueError>`
- `FlushContext`: `{ gatewayId: string; schemas: ReadonlyArray<TableSchema> }` — metadata passed alongside deltas
- `isFlushQueue()` type guard: duck-typed check (same pattern as `isMaterialisable`)
- `MemoryFlushQueue`: default implementation — calls `processMaterialisation()` inline (identical to previous synchronous path, fully backward-compatible)
- `R2FlushQueue`: writes serialised delta batches to object storage under `materialise-jobs/{gatewayId}/{timestamp}-{rand}.json`; a separate polling consumer processes and deletes
- `CloudflareFlushQueue` (apps/gateway-worker): claim-check pattern — writes payload to R2, publishes lightweight `MaterialiseJobMessage` reference to CF Queue (128 KB message limit)
- `handleMaterialiseQueue()` (apps/gateway-worker): CF Queue consumer — fetches payload from R2, calls `processMaterialisation()`, deletes R2 object on success, `message.retry()` on failure
- `processMaterialisation()`: standalone function extracted from FlushCoordinator — iterates materialisation targets, catches failures per-table, never throws
- `collectMaterialisers()`: builds materialiser list from adapter (if `isMaterialisable`) + explicit `materialisers` array
- `FlushQueueError` (`@lakesync/core`): error class with code `"FLUSH_QUEUE_ERROR"`
- `GatewayConfig.flushQueue`: optional — when absent, gateway auto-builds `MemoryFlushQueue` from adapter + materialisers
- `GatewayConfig.onMaterialisationFailure`: optional callback `(table, deltaCount, error)` for metrics/alerting
- `GatewayServerConfig.flushQueue`: forwarded to underlying `SyncGateway`
- gateway-worker `Env.MATERIALISE_QUEUE`: CF Queue binding for `CloudflareFlushQueue`

### Client SDK (packages/client)
- LocalDB: sql.js WASM + IndexedDB snapshot persistence
- `SyncEngine`: pure sync operations (push, pull, syncOnce) extracted from coordinator. `syncOnce()` is an explicit pull-then-push transaction — ordering is structural, not temporal.
- `SyncCoordinator`: composes `SyncEngine` + `AutoSyncScheduler` + event system. Delegates sync to engine, handles scheduling/lifecycle. Exposes `readonly engine: SyncEngine` for advanced consumers.
- SyncTracker: wraps LocalDB + queue + HLC; insert/update/delete with auto delta extraction
- Transport interfaces are split by capability: `SyncTransport` (core push/pull, required), `CheckpointTransport` (checkpoint downloads for initial sync), `RealtimeTransport` (WebSocket real-time broadcast), `ActionTransport` (imperative action execution)
- `TransportWithCapabilities = SyncTransport & Partial<CheckpointTransport> & Partial<RealtimeTransport> & Partial<ActionTransport>`
- HttpTransport implements SyncTransport + CheckpointTransport + ActionTransport
- LocalTransport implements SyncTransport + CheckpointTransport + ActionTransport
- WebSocketTransport implements SyncTransport + RealtimeTransport + CheckpointTransport; binary protobuf with tag-based framing (0x01=push, 0x02=pull, 0x03=broadcast), exponential backoff reconnect
- Queues: MemoryQueue, IDBQueue (outbox pattern)
- SchemaSynchroniser: client-side schema migration
- applyRemoteDeltas: conflict-aware remote delta application
- Initial sync: checkpoint download on first sync (`lastSyncedHlc === 0`), then incremental pull

### Sync Rules (packages/core/src/sync-rules/)
- Declarative bucket-based filtering with `eq`, `neq`, `in`, `gt`, `lt`, `gte`, `lte` operators
- JWT claim references via `jwt:` prefix (e.g. `"jwt:sub"`)
- `filterDeltas()`: pure function, union across buckets
- Gateway `handlePull()` accepts optional `SyncRulesContext` for filtered pulls
- Adapter-sourced pull: `handlePull({ source: "name" })` queries named DatabaseAdapter with sync rules filtering

### Gateway (packages/gateway)
- `SyncGateway`: thin facade composing `DeltaBuffer`, `ActionDispatcher`, `SchemaManager`, and `flushEntries`
- `SyncGateway.rehydrate(deltas)`: restores persisted deltas without push validation
- `DeltaBuffer`: atomic `BufferSnapshot` pattern — immutable state (log, index, deltaIds, bytes, tableStats) swapped atomically on each mutation. Same pattern as `SchemaSnapshot`.
- `ActionDispatcher`: dispatches imperative actions to registered `ActionHandler`s with idempotency
- `SchemaManager`: schema versioning, delta validation, safe evolution (add nullable columns only). Uses atomic `SchemaSnapshot` pattern (single state object swapped atomically).
- `ConfigStore` / `MemoryConfigStore`: stores sync rules, schemas, and connector configs
- `FlushCoordinator`: queue publish is fire-and-forget — flush returns immediately after adapter write, materialisation runs asynchronously via `FlushQueue`
- `flushEntries`: unified flush module — handles both Lake (Parquet/JSON) and Database adapter paths, auto-materialise
- Shared request handlers (`handlePushRequest`, `handlePullRequest`, etc.) used by both gateway-worker and gateway-server

### Gateway Server (packages/gateway-server)
- Self-hosted gateway server wrapping SyncGateway in a node:http server (works in Node.js and Bun)
- Middleware pipeline architecture: `runPipeline()` composes independent middleware (CORS, auth, drain guard, timeout, rate limit, route matching, dispatch). Each middleware is a focused `(context, next)` function.
- Data-driven route dispatch: `Record<string, RouteHandler>` map built by `buildRouteHandlers()` — routes are data, dispatch is a map lookup. Open for extension.
- Routes mirror gateway-worker: /sync/:id/push, /sync/:id/pull, /sync/:id/action, /sync/:id/actions, /sync/:id/ws, /admin/flush/:id, /admin/schema/:id, /admin/sync-rules/:id, /admin/connectors/:id, /admin/metrics/:id, /health
- Optional JWT auth (HMAC-SHA256) when `jwtSecret` is provided
- CORS support with configurable origins
- Periodic flush via setInterval (default 30s)
- Persistence: `'memory'` (default) or `'sqlite'` (WAL-mode, survives restarts)
- WebSocket support: binary protobuf sync + server-initiated broadcast to connected clients
- Source polling ingest: `SourcePoller` with cursor and diff strategies for polling external databases
- Dynamic connector registration: `POST /admin/connectors/:id` creates adapter + starts poller at runtime
- Clustering: `DistributedLock` interface + `AdapterBasedLock` for coordinated flush across instances; `SharedBuffer` for cross-instance pull visibility
- `GatewayServerConfig` accepts optional `pollerRegistry` and `adapterRegistry` for extensibility. `ConnectorManager` uses registry-based dispatch.
- Docker image + compose example in package directory

### Table Sharding (apps/gateway-worker)
- Shard router splits tenant traffic across multiple DOs by table name
- Config via `SHARD_CONFIG` env variable (JSON)
- Push: partitions deltas by table, fans out to correct shard DOs
- Pull: fans out to all shards, merges results sorted by HLC
- Admin ops (flush, schema, sync-rules): apply to all shards
- Backward compatible — no shard config = unchanged behaviour

### Checkpoints (packages/compactor + apps/gateway-worker)
- Per-table proto-encoded chunks (not per-user); filtering at serve time
- Generated post-compaction from base Parquet files
- Byte-budget sizing (default 16 MB per chunk for 128 MB DO)
- Serve endpoint: `GET /sync/:gatewayId/checkpoint` with JWT-claim filtering

### React Hooks (packages/react)
- `LakeSyncProvider`: context provider wrapping `SyncCoordinator`; maintains per-table `tableVersions` map + `globalVersion` fallback for table-scoped reactivity
- `useQuery<T>(sql, params?)`: reactive SQL query hook with table-scoped reactivity — extracts table names from SQL via `extractTables()`, only re-runs when affected tables change (not on every delta)
- `useMutation()`: wraps `SyncTracker.insert/update/delete` with table-scoped invalidation via `invalidateTables([table])`
- `useAction()`: executes imperative actions via `SyncCoordinator.executeAction()`, tracks `lastResult` and `isPending`
- `useActionDiscovery()`: fetches available connectors and their supported action types from the gateway
- `useSyncStatus()`: observes sync lifecycle — `isSyncing`, `lastSyncTime`, `queueDepth`, `error`
- `useLakeSync()`: raw context access for advanced use cases

## Code Style
- TypeScript strict mode, no `any`
- Functional style where practical; classes for stateful components (DO, client)
- Result<T, E> pattern — never throw from public APIs
- JSDoc on all public APIs
- British English in comments and docs (serialise, initialise, synchronise, catalogue, behaviour)
- Vitest for testing, co-located in `__tests__/`

## Build System
- `bun run build` = `turbo run build` (tsc --noEmit for all packages)
- `bun run test` = `turbo run test` (vitest per package)
- `bun run lint` = `biome check .` (tabs, no any, noNonNullAssertion off)
- `bun run typecheck` = `turbo run typecheck`
- `bun run test:integration` = vitest with root config
- `bun run publish:npm` = builds + publishes the `lakesync` unified package

## Task Execution
Read plans/PLAN.md for task breakdown.
For PARALLEL GROUPs: launch all tasks as parallel Task subagents.
For SEQUENTIAL tasks: execute one at a time.

## Hard Rules
- NEVER use localStorage or sessionStorage — use OPFS or IndexedDB
- NEVER throw exceptions from public APIs — use Result<T, E>
- NEVER flush per-sync to Iceberg — always batch
- Backend is adapter-dependent — any data source can be an adapter
- NEVER use `any` type
- NEVER create custom subagents — use built-in Task tool only

## Gotchas
- Biome `--fix --unsafe` can break TypeScript by replacing `!` with `?.` on indexed access
- IndexedDB structuredClone cannot handle bigint — IDBQueue serialises HLC to string
- Adapter tests skip cleanly without Docker (top-level await + describe.skipIf)
- Multi-client tests need a shared monotonic clock — `Date.now()` HLCs are flaky within fast tests (same ms). Use `createSharedClock()` injected into gateway + all coordinators
- Gateway HLC is private — override post-construction: `(gateway as any).hlc = new HLC(clock)`
- `SyncEngine.syncOnce()` enforces pull-before-push structurally — ordering is built into the transaction, not a convention
- todo-app tsconfig must exclude `__tests__/` to avoid tsc errors on vitest globals
- gateway-worker: ALL routes except /health require JWT (including admin routes)
- gateway-worker flush route is `/admin/flush/:gatewayId` (not `/sync/`)
- gateway-worker sync rules route is `POST /admin/sync-rules/:gatewayId`
- gateway-worker checkpoint route is `GET /sync/:gatewayId/checkpoint`
- Checkpoint chunks contain ALL rows — sync rules filtering happens at serve time, not generation time
- gateway-worker: `SHARD_CONFIG` env is optional JSON — when absent, sharding is disabled (backward compatible)
- gateway-server uses node:http (not Bun.serve) so tests run under vitest/Node without issues
- gateway-server SqlitePersistence serialises HLC bigints to string (same structuredClone constraint as IDBQueue)
- FanOutAdapter secondary failures are silently caught — never affect the return value
- LifecycleAdapter determines age from HLC upper 48 bits: `wallMs = Number(hlc >> 16n)`
- Materialise failures are non-fatal — always warned, never fail the flush
- Connector packages export poller factories as named exports (`jiraPollerFactory`, `salesforcePollerFactory`) — build a `PollerRegistry` via `createPollerRegistry()` and pass it to `createPoller()`
- WebSocketTransport delegates checkpoint downloads to an internal HttpTransport (large binary payloads)
- gateway-server WebSocket auth: token via Authorization header or `?token=` query param
- gateway-server clustering: `SharedBuffer` writes through to shared adapter on push, merges on pull
- gateway-server dynamic connectors: Jira/Salesforce use API-based pollers (no DatabaseAdapter), database connectors use SourcePoller + queryFn
- ActionDispatcher caches non-retryable errors but not retryable ones (allows retry)
- todo-app moved to `apps/examples/todo-app`
- `MemoryFlushQueue` is auto-built from adapter + materialisers when no `flushQueue` is provided in `GatewayConfig`
- FlushQueue `publish()` failures are non-fatal — warned, never fail the flush (same semantics as direct materialisation)

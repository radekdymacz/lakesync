# LakeSync

## Vision
**Sync any data source to a local working set.** Your app or agent declares what it needs. The engine handles the rest.

Any data source can be an adapter — Postgres, BigQuery, Iceberg, CloudWatch, Stripe, anything you can read from. Consumers (web apps, AI agents, dashboards) get a local SQLite copy of exactly the slice they need via declarative sync rules. The sync rules DSL is the product — adapters are plumbing.

Current adapters: S3/R2 (Iceberg), Postgres, MySQL, BigQuery. The adapter interface is the extension point for any data source.

## Internal Context (not public)
LakeSync is the data backbone for:
- **CompanyOS** (https://company-os.pages.dev/) — company operating system web app
- **AgentOS** — Claude Code Teams organised per department

Real use cases driving development:
- Agents analysing CloudWatch logs — sync a filtered subset locally, reason over it
- Financial data in BigQuery — agent or dashboard gets exactly the slice it needs
- Offline web app — CompanyOS works on a plane, catches up when back
- Cross-backend flows — Iceberg → BigQuery, Postgres → Iceberg, any source → any destination

LakeSync ensures data flows between all systems: web apps, agents, and backends. The library itself is the public-facing product; CompanyOS and AgentOS are the primary consumers. Focus development on the library, not the consumers.

## Monorepo
TurboRepo + Bun. Packages in `packages/`, apps in `apps/`.

## Architecture
- 10 packages: core, client, gateway, adapter, proto, parquet, catalogue, compactor, analyst, lakesync
- 3 apps: todo-app (Vite + vanilla TS), gateway-worker (Cloudflare Workers + DO), docs (Fumadocs + Next.js)
- All phases complete (1 through 6): HLC, Delta, Result, Conflict, Queue, Gateway, Proto, Adapter, Parquet, Catalogue, SQLite client, CF Workers, Compaction, Schema Evolution, Analyst, Sync Rules, Initial Sync, Database Adapters

### Adapters — Any Data Source (packages/adapter)
- `LakeAdapter` interface: `putObject`, `getObject`, `headObject`, `listObjects`, `deleteObject`, `deleteObjects`
- `DatabaseAdapter` interface: `insertDeltas`, `queryDeltasSince`, `getLatestState`, `ensureSchema` — adapters are both sources AND destinations
- Implementations: S3Adapter (S3/R2/MinIO), PostgresAdapter, MySQLAdapter, BigQueryAdapter, CompositeAdapter
- `migrateAdapter()`: copies data between any two adapters
- Gateway takes an optional adapter — flush target is fully decoupled from sync logic
- The adapter interface is the extension point — any data source that can be read from can become an adapter (CloudWatch, Stripe, etc.)
- Same client code regardless of backend; swap adapters at the gateway level

### Key Patterns
- `Result<T, E>` — never throw from public APIs
- `HLCTimestamp` = branded bigint (48-bit wall + 16-bit counter)
- Column-level LWW with clientId tiebreak
- DeltaBuffer = dual structure (log + index)
- Deterministic deltaId via SHA-256 of stable-stringified payload
- `TableSchema` = `{ table: string; columns: Array<{ name: string; type: ... }> }`

### Client SDK (packages/client)
- LocalDB: sql.js WASM + IndexedDB snapshot persistence
- SyncCoordinator: push/pull orchestration, auto-sync (10s interval + visibility)
- SyncTracker: wraps LocalDB + queue + HLC; insert/update/delete with auto delta extraction
- Transports: HttpTransport (remote gateway), LocalTransport (in-process)
- Queues: MemoryQueue, IDBQueue (outbox pattern)
- SchemaSynchroniser: client-side schema migration
- applyRemoteDeltas: conflict-aware remote delta application
- Initial sync: checkpoint download on first sync (`lastSyncedHlc === 0`), then incremental pull

### Sync Rules (packages/core/src/sync-rules/)
- Declarative bucket-based filtering with `eq`/`in` operators
- JWT claim references via `jwt:` prefix (e.g. `"jwt:sub"`)
- `filterDeltas()`: pure function, union across buckets
- Gateway `handlePull()` accepts optional `SyncRulesContext` for filtered pulls

### Checkpoints (packages/compactor + apps/gateway-worker)
- Per-table proto-encoded chunks (not per-user); filtering at serve time
- Generated post-compaction from base Parquet files
- Byte-budget sizing (default 16 MB per chunk for 128 MB DO)
- Serve endpoint: `GET /sync/:gatewayId/checkpoint` with JWT-claim filtering

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
- SyncCoordinator advances `lastSyncedHlc` on push via `serverHlc` — pull-before-push is the correct sync pattern
- todo-app tsconfig must exclude `__tests__/` to avoid tsc errors on vitest globals
- gateway-worker: ALL routes except /health require JWT (including admin routes)
- gateway-worker flush route is `/admin/flush/:gatewayId` (not `/sync/`)
- gateway-worker sync rules route is `POST /admin/sync-rules/:gatewayId`
- gateway-worker checkpoint route is `GET /sync/:gatewayId/checkpoint`
- Checkpoint chunks contain ALL rows — sync rules filtering happens at serve time, not generation time

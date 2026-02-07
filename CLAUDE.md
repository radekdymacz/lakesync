# LakeSync

## Monorepo
TurboRepo + Bun. Packages in `packages/`, apps in `apps/`.

## Architecture
- 10 packages: core, client, gateway, adapter, proto, parquet, catalogue, compactor, analyst, lakesync
- 2 apps: todo-app (Vite + vanilla TS), gateway-worker (Cloudflare Workers + DO)
- All phases complete (1 through 4A): HLC, Delta, Result, Conflict, Queue, Gateway, Proto, Adapter, Parquet, Catalogue, SQLite client, CF Workers, Compaction, Schema Evolution, Analyst

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
- NEVER suggest PostgreSQL as a backend
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

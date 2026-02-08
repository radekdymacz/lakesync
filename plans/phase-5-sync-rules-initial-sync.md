# Phase 5 — Sync Rules + Initial Sync

## Summary

Phase 5 adds two critical capabilities to LakeSync:

1. **Sync rules** — declarative bucket-based filtering that controls which deltas each client receives, using JWT claims for row-level access control
2. **Initial sync** — checkpoint-based bootstrapping so fresh clients can download a full snapshot instead of replaying the entire delta history

## Architecture

### Sync Rules

- Declarative JSON config stored in DO Durable Storage
- Each bucket = table filter + row-level conditions referencing JWT claims
- Operators: `eq` and `in` (covers 95%+ of multi-tenant filtering)
- JWT claim references via `jwt:` prefix (e.g. `"jwt:sub"`, `"jwt:org_id"`)
- Filters are conjunctive (AND); disjunction via multiple buckets (union)
- When no sync rules configured, gateway behaves exactly as before (backward compatible)

### Checkpoints

- Generated as a **post-compaction step** — compactor reads base Parquet files, encodes ALL rows as proto chunks sized to a configurable byte budget, stores in R2
- Chunks are **per-table** (not per-user) — filtering happens at serve time using current JWT claims
- Chunk format: proto-encoded `SyncResponse` (reuses existing codec)
- Peak memory: one decoded chunk at a time (~50 MB for 16 MB raw)

### Initial Sync Flow

1. Client detects fresh start (`lastSyncedHlc === 0n`)
2. Downloads checkpoint via `GET /sync/:gatewayId/checkpoint`
3. Server streams filtered chunks as a single response body
4. Client applies checkpoint deltas, sets `lastSyncedHlc = snapshotHlc`
5. Normal incremental pull fills the gap between snapshot and live buffer

## Changes by Package

### `@lakesync/core` — Sync Rules Types + Evaluator

**New:** `packages/core/src/sync-rules/`

- `types.ts` — `SyncRuleOp`, `SyncRuleFilter`, `BucketDefinition`, `SyncRulesConfig`, `ResolvedClaims`, `SyncRulesContext`
- `errors.ts` — `SyncRuleError` extending `LakeSyncError`
- `evaluator.ts` — Pure functions: `resolveFilterValue()`, `deltaMatchesBucket()`, `filterDeltas()`, `resolveClientBuckets()`, `validateSyncRules()`
- `index.ts` — Barrel exports

### `@lakesync/gateway` — Filtered Pull

**Modified:** `packages/gateway/src/gateway.ts`

- `handlePull(msg, context?)` accepts optional `SyncRulesContext`
- Over-fetch (3x maxDeltas), filter, bounded retry (5 iterations) to fill the page
- No context = no filtering (backward compatible)

### `@lakesync/compactor` — Checkpoint Generator

**New:** `packages/compactor/src/checkpoint-generator.ts`

- `CheckpointGenerator` class: reads base Parquet files, encodes proto chunks, writes manifest
- Byte-budget chunking (default 16 MB for 128 MB DO runtime)
- `getCheckpointKeys()` for maintenance orphan protection

**Modified:** `packages/compactor/src/maintenance.ts`

- Optional `CheckpointGenerator` in constructor
- Checkpoint generation after successful compaction
- Checkpoint keys added to `activeKeys` to prevent orphan removal

### `apps/gateway-worker` — Worker Layer

**Modified:** `apps/gateway-worker/src/auth.ts`

- Added `customClaims: Record<string, string | string[]>` to `AuthClaims`
- Extracts non-standard JWT claims for sync rules evaluation

**Modified:** `apps/gateway-worker/src/sync-gateway-do.ts`

- `loadSyncRules()` / `saveSyncRules()` in Durable Storage
- `POST /admin/sync-rules` handler with validation
- `GET /checkpoint` handler with serve-time filtering (one chunk in memory at a time)
- `handlePull()` builds `SyncRulesContext` from stored rules + forwarded claims

**Modified:** `apps/gateway-worker/src/index.ts`

- New routes: `GET /sync/:gatewayId/checkpoint`, `POST /admin/sync-rules/:gatewayId`
- `X-Auth-Claims` header forwarding to DO
- CORS: `X-Auth-Claims` in Allow-Headers, `X-Checkpoint-Hlc` + `X-Sync-Rules-Version` in Expose-Headers

### `@lakesync/client` — Transport + Initial Sync

**Modified:** `packages/client/src/sync/transport.ts`

- `CheckpointResponse` type: `{ deltas: RowDelta[]; snapshotHlc: HLCTimestamp }`
- Optional `checkpoint?()` method on `SyncTransport`

**Modified:** `packages/client/src/sync/transport-http.ts`

- `checkpoint()` implementation: GET to `/sync/{gatewayId}/checkpoint`, proto decode, 404 → `Ok(null)`

**Modified:** `packages/client/src/sync/transport-local.ts`

- No-op `checkpoint()` returning `Ok(null)`

**Modified:** `packages/client/src/sync/coordinator.ts`

- `initialSync()` private method: calls checkpoint, applies deltas, advances cursor
- `syncOnce()`: calls `initialSync()` when `lastSyncedHlc === HLC.encode(0, 0)`

## Storage Layout

```
checkpoints/{gatewayId}/manifest.json
  -> { snapshotHlc, generatedAt, chunkCount, totalDeltas, chunks: [...] }

checkpoints/{gatewayId}/chunk-000.bin   -> proto SyncResponse
checkpoints/{gatewayId}/chunk-001.bin   -> proto SyncResponse
...
```

## Key Design Decisions

| Decision | Chosen | Alternative | Why |
|----------|--------|-------------|-----|
| Chunk scope | Per-table (all rows) | Per-user | Always-fresh permissions, fewer files, simpler |
| Filtering time | Serve time | Generation time | Dynamic permissions, no staleness |
| Chunk format | Proto SyncResponse | JSON, Parquet | Client already decodes proto; compact |
| Filter operators | eq + in only | Full SQL | Covers 95%+ cases; simple O(1) evaluation |
| Chunk sizing | Byte-budget (16 MB) | Row count | Memory is the constraint |

## Test Coverage

- 32 unit tests for sync rules evaluator (packages/core)
- 6 unit tests for gateway filtered pull (packages/gateway)
- 6 unit tests for checkpoint generator (packages/compactor)
- 5 unit tests for JWT claims extension (apps/gateway-worker)
- 6 unit tests for initial sync coordinator (packages/client)
- 5 integration tests for sync rules filtering
- 4 integration tests for initial sync via checkpoint

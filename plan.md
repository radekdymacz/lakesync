# LakeSync — Claude Code Implementation Plan

**Project:** LakeSync (NativeKloud)  
**Author:** Radek Dymacz  
**Date:** 2026-02-06  
**Phase:** 1 — Foundation (Weeks 1–6)  
**Runtime:** Bun + TurboRepo monorepo, TypeScript-strict, Vitest

---

## How This Plan Works

This plan is designed for Claude Code's **built-in Task tool** (the general-purpose subagent). No custom agents needed.

**Execution model:**
- You paste this plan into `PLAN.md` at the project root
- Tell the main Claude Code agent: *"Read PLAN.md and execute it phase by phase. For each PARALLEL GROUP, launch all tasks as parallel subagents using the Task tool. Wait for all tasks in a group to finish before starting the next group."*
- The main agent orchestrates. Built-in Task subagents do the implementation work.
- Each subagent gets its own context window — keeps the main conversation clean.

**Why this works:**
- Tasks within a PARALLEL GROUP have **zero cross-dependencies**
- Each task is **self-contained** with exact file paths, type signatures, and test cases
- The main agent only needs to synthesise results between groups

**Prompt to kick it off:**

```
Read PLAN.md and execute Phase 0 first. Then for each subsequent 
PARALLEL GROUP, launch all tasks in the group as parallel Task 
subagents simultaneously. Wait for all tasks in a group to complete 
before moving to the next group. Each task is self-contained — give 
the subagent the full task text from PLAN.md as its prompt.
```

---

## CLAUDE.md (place in project root)

```markdown
# LakeSync

## Monorepo
TurboRepo + Bun. Packages in `packages/`, apps in `apps/`, Rust crates in `crates/`.

## Code Style
- TypeScript strict mode, no `any`
- Functional style where practical; classes for stateful components (DO, client)
- Result<T, E> pattern — never throw from public APIs
- JSDoc on all public APIs
- British English in comments and docs (serialise, initialise, synchronise, catalogue, behaviour)
- Vitest for testing, co-located in `__tests__/`

## Task Execution
Read PLAN.md for task breakdown.
For PARALLEL GROUPs: launch all tasks as parallel Task subagents.
For SEQUENTIAL tasks: execute one at a time.

## Hard Rules
- NEVER use localStorage or sessionStorage — use OPFS or IndexedDB
- NEVER throw exceptions from public APIs — use Result<T, E>
- NEVER flush per-sync to Iceberg — always batch
- NEVER suggest PostgreSQL as a backend
- NEVER use `any` type
- NEVER create custom subagents — use built-in Task tool only
```

---

## Phase 0 — Scaffold (SEQUENTIAL)

> Execute these two as parallel tasks — they don't depend on each other.

---

### TASK 0.1 — Monorepo Bootstrap

**Package:** `/` (root)  
**Depends on:** nothing

Initialise the LakeSync monorepo:

1. Init git repo. `.gitignore`: node_modules, dist, .turbo, *.wasm, .DS_Store
2. Root `package.json` with workspaces: `packages/*`, `apps/*`, `crates/*`
3. `turbo.json` pipeline: `build` (depends on `^build`), `test` (depends on `build`), `lint`, `typecheck`
4. Root `tsconfig.base.json` — strict, no any, ES2022, moduleResolution bundler
5. `biome.json` for linting/formatting
6. Directory skeleton:

```
lakesync/
├── apps/docs/
├── apps/examples/todo-app/
├── packages/core/          
├── packages/client/        
├── packages/gateway/       
├── packages/adapter/       
├── packages/compactor/     
├── packages/analyst/       
├── packages/proto/         
├── crates/parquet-wasm/    
├── docker/                 
└── adrs/                   
```

7. Each package gets: `package.json` (`@lakesync/<pkg>`), `tsconfig.json` extending root, `src/index.ts`, `src/__tests__/`, `vitest.config.ts`
8. Root `vitest.workspace.ts`
9. Verify: `bun install && bun run build && bun run test` all pass (empty)

**⚠ CRITICAL — Package exports map:**
Every package `package.json` MUST include an explicit `exports` field. Without this, cross-package imports fail during typecheck in Bun/TurboRepo.

```json
{
  "name": "@lakesync/core",
  "version": "0.0.1",
  "type": "module",
  "exports": {
    ".": "./src/index.ts",
    "./*": "./src/*.ts"
  },
  "scripts": {
    "build": "tsc --noEmit",
    "test": "vitest run",
    "typecheck": "tsc --noEmit"
  },
  "devDependencies": {
    "typescript": "^5.7.0",
    "vitest": "^3.0.0"
  }
}
```

**Reference configurations (use these exactly):**

**`turbo.json`:**
```json
{
  "$schema": "https://turbo.build/schema.json",
  "tasks": {
    "build": {
      "dependsOn": ["^build"],
      "outputs": []
    },
    "test": {
      "dependsOn": ["build"],
      "outputs": [],
      "inputs": ["src/**", "tests/**", "__tests__/**"]
    },
    "typecheck": {
      "dependsOn": ["^build"],
      "outputs": []
    },
    "lint": {
      "outputs": []
    }
  }
}
```

**⚠ NOTE on build vs typecheck:**
In Phase 1, `build` and `typecheck` both run `tsc --noEmit` (no `dist/` output). This is deliberate — we treat the monorepo as a pure TS workspace where Bun resolves `./src/index.ts` directly via the `exports` map. The `dist/` build step is deferred to Phase 2 when we need compiled JS for Cloudflare Workers deployment. `turbo.json` `outputs` for `build` is set to `[]` (empty) to match, avoiding confusing cache misses.

**`tsconfig.base.json`:**
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "bundler",
    "lib": ["ES2022"],
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "noEmit": true,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "esModuleInterop": true,
    "forceConsistentCasingInFileNames": true,
    "skipLibCheck": true,
    "isolatedModules": true,
    "resolveJsonModule": true,
    "types": ["vitest/globals"]
  },
  "exclude": ["node_modules", "dist"]
}
```

**Per-package `tsconfig.json` (template):**
```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "rootDir": "src",
    "outDir": "dist"
  },
  "include": ["src"]
}
```

**Done when:** `turbo build`, `turbo test`, `turbo typecheck` all succeed.

---

### TASK 0.2 — Docker Compose Dev Environment

**Package:** `docker/`  
**Depends on:** nothing

1. `docker/docker-compose.yml`:
   - **MinIO**: ports 9000/9001, bucket `lakesync-dev` auto-created
   - **Nessie**: port 19120, in-memory backend
2. `docker/init-minio.sh` — create default bucket on startup
3. `docker/.env.example`:
   ```
   MINIO_ROOT_USER=lakesync
   MINIO_ROOT_PASSWORD=lakesync123
   MINIO_ENDPOINT=http://localhost:9000
   NESSIE_URI=http://localhost:19120/api/v1
   ```
4. `docker/README.md` with quickstart

**Done when:** `docker compose up -d && docker compose ps` shows both healthy, MinIO bucket exists, Nessie responds on :19120.

---

## Phase 1A — Core Primitives (PARALLEL GROUP — 3 tasks)

> Launch all 3 as parallel Task subagents. Zero cross-dependencies.

---

### TASK 1A.1 — Hybrid Logical Clock (HLC)

**Package:** `packages/core/src/hlc/`  
**Depends on:** Task 0.1 only

**Files:**
- `hlc.ts` — HLC class
- `types.ts` — `HLCTimestamp` branded type
- `__tests__/hlc.test.ts`

**HLC spec:** 64-bit, layout `[48-bit wall clock ms][16-bit logical counter]`, 5-second max drift.

**⚠ CRITICAL — Wall clock source:**
- The default `wallClock` must be **monotonic-safe**. `Date.now()` alone is NOT safe — NTP adjustments can move it backward.
- In `now()`, if the new wall clock reading is **less than** `this.lastWall`, use `this.lastWall` instead (the HLC absorbs backward jumps into the logical counter).
- The injectable `wallClock` constructor parameter is for testing. Default should be `() => Date.now()` but the `now()` method must enforce `wall = Math.max(wallClock(), this.lastWall)`.
- This is already implicit in a correct HLC implementation, but state it explicitly: **the physical component must never decrease**.

```typescript
// types.ts
export type HLCTimestamp = bigint & { readonly __brand: 'HLCTimestamp' };

// hlc.ts
export class HLC {
  private wallClock: () => number; // injectable for testing
  private counter: number = 0;
  private lastWall: number = 0;
  static MAX_DRIFT_MS = 5_000;

  constructor(wallClock?: () => number);
  now(): HLCTimestamp;
  recv(remote: HLCTimestamp): Result<HLCTimestamp, ClockDriftError>;
  static encode(wall: number, counter: number): HLCTimestamp;
  static decode(ts: HLCTimestamp): { wall: number; counter: number };
  static compare(a: HLCTimestamp, b: HLCTimestamp): -1 | 0 | 1;
}
```

**⚠ CRITICAL — No throws anywhere:**
`recv()` returns `Result`, not throws. This is consistent with the project-wide "never throw from public APIs" rule. Test assertions must use `expect(result.ok).toBe(false)` and check `result.error instanceof ClockDriftError`, NOT `expect(...).toThrow()`.

**Tests (minimum 9):**
1. `now()` monotonically increasing
2. `now()` increments logical counter when wall unchanged
3. `now()` absorbs backward clock jump (wall goes backward, timestamp still advances)
4. `recv()` valid remote → `Ok` with advanced local
5. `recv()` >5s future drift → `Err(ClockDriftError)`
6. `recv()` past timestamp → `Ok` with still-advancing ts
7. `encode()`/`decode()` roundtrip edge values (0, max wall, max counter)
8. `compare()` orders correctly
9. Counter overflow: when counter reaches 65535, behaviour is defined (either advance wall or return Err)

**Done when:** all tests pass, exported from `packages/core/src/index.ts`.

---

### TASK 1A.2 — Delta Extraction (Column-Level)

**Package:** `packages/core/src/delta/`  
**Depends on:** Task 0.1 only

**Files:**
- `types.ts` — `ColumnDelta`, `RowDelta`, `DeltaOp`, `TableSchema`, `RowKey`
- `extract.ts` — `extractDelta(before, after, schema) → RowDelta | null`
- `apply.ts` — `applyDelta(row, delta) → merged row`
- `row-key.ts` — `rowKey(table, rowId)` utility
- `__tests__/extract.test.ts`
- `__tests__/apply.test.ts`

```typescript
import type { HLCTimestamp } from '../hlc/types';

export type DeltaOp = 'INSERT' | 'UPDATE' | 'DELETE';

export interface ColumnDelta {
  column: string;
  value: unknown; // serialisable JSON — NEVER undefined, use null instead
}

export interface RowDelta {
  op: DeltaOp;
  table: string;
  rowId: string;
  clientId: string;            // required — used for LWW tiebreak and audit
  columns: ColumnDelta[];      // empty for DELETE
  hlc: HLCTimestamp;           // branded bigint, NOT plain bigint
  deltaId: string;             // deterministic: hash(clientId + hlc + table + rowId + columns)
}

/** Minimal schema for Phase 1. Column allow-list + type hints. */
export interface TableSchema {
  table: string;
  columns: Array<{
    name: string;
    type: 'string' | 'number' | 'boolean' | 'json' | 'null';
  }>;
}

/** Composite key utility — avoids string concatenation bugs */
export type RowKey = string & { readonly __brand: 'RowKey' };
export function rowKey(table: string, rowId: string): RowKey {
  return `${table}:${rowId}` as RowKey;
}
```

**⚠ CRITICAL — Type contracts across packages:**
- `hlc` field is `HLCTimestamp` (branded bigint), NOT plain `bigint`. This prevents accidentally passing arbitrary bigints.
- `clientId` is required on every delta. The LWW resolver needs it for equal-HLC tiebreak (Task 1C.1 test 4).
- `deltaId` is deterministic. Use `fast-json-stable-stringify` on `{clientId, hlc: hlc.toString(), table, rowId, columns}` then SHA-256 hash. This enables idempotent re-push after gateway restart.
- `TableSchema` is used by `extractDelta` to filter columns (only emit deltas for columns in the schema) and enforce consistent column ordering. In Phase 1, passing `undefined` for schema should be allowed (extract all columns).
- `RowKey` is used internally by the DeltaBuffer and resolver as a composite lookup key. Export it from core.
- `value: unknown` must be JSON-serialisable. `undefined` is NEVER valid — use `null`. If `extractDelta` encounters `undefined`, it should skip the column (treat as absent, not as a change to null).

**Logic:**
- `extractDelta`: compare before/after, emit only changed columns. INSERT = all columns. DELETE = empty columns. UPDATE = only changed. No-op (identical) returns `null`.
- `applyDelta`: merge delta columns onto existing row, return new object (immutable). DELETE returns `null`.

**⚠ CRITICAL — Comparison strategy:**
- Primitives (string, number, boolean, null): use `Object.is()`
- Objects/arrays: use **key-order-agnostic deep equality** — NOT `JSON.stringify` (it is not deterministic on key order: `{a:1,b:2}` vs `{b:2,a:1}` produce different strings).
- Install `fast-deep-equal` (`bun add fast-deep-equal`) and use it for deep comparison.
- If you need canonical serialisation elsewhere (e.g. hashing), use `fast-json-stable-stringify` — but prefer deep equality for delta extraction.

**Tests (minimum 11):**
1. INSERT → all columns
2. UPDATE single column → only that column
3. UPDATE multiple → only changed
4. DELETE → empty columns
5. No-op → returns null
6. `applyDelta` merges partial update
7. `applyDelta` INSERT creates new row
8. `applyDelta` DELETE returns null
9. `null` values handled (null is a valid change)
10. Nested objects with different key order → no false delta
11. Nested objects with actual value change → correct delta

**Done when:** all tests pass, exported from `packages/core/src/index.ts`.

---

### TASK 1A.3 — Result Type & Error Handling

**Package:** `packages/core/src/result/`  
**Depends on:** Task 0.1 only

**Files:**
- `result.ts` — `Result<T, E>` discriminated union + helpers
- `errors.ts` — base error hierarchy
- `__tests__/result.test.ts`

```typescript
export type Result<T, E = LakeSyncError> =
  | { ok: true; value: T }
  | { ok: false; error: E };

export const Ok = <T>(value: T): Result<T, never> => ({ ok: true, value });
export const Err = <E>(error: E): Result<never, E> => ({ ok: false, error });

export function mapResult<T, U, E>(r: Result<T, E>, fn: (v: T) => U): Result<U, E>;
export function flatMapResult<T, U, E>(r: Result<T, E>, fn: (v: T) => Result<U, E>): Result<U, E>;
export function unwrapOrThrow<T, E>(r: Result<T, E>): T;
export function fromPromise<T>(p: Promise<T>): Promise<Result<T, Error>>;

// Error hierarchy
export class LakeSyncError extends Error { readonly code: string; readonly cause?: Error; }
export class ClockDriftError extends LakeSyncError { code = 'CLOCK_DRIFT'; }
export class ConflictError extends LakeSyncError { code = 'CONFLICT'; }
export class FlushError extends LakeSyncError { code = 'FLUSH_FAILED'; }
export class SchemaError extends LakeSyncError { code = 'SCHEMA_MISMATCH'; }
export class AdapterError extends LakeSyncError { code = 'ADAPTER_ERROR'; }
```

**Tests (minimum 7):**
1. `Ok`/`Err` correct discriminants
2. `mapResult` transforms Ok, passes Err
3. `flatMapResult` chains correctly
4. `unwrapOrThrow` returns on Ok, throws on Err
5. `fromPromise` wraps resolve → Ok, reject → Err
6. All errors `instanceof LakeSyncError`
7. Error codes are correct strings

**Done when:** all tests pass, exported from `packages/core/src/index.ts`.

---

## Phase 1B — Protocol & Adapter (PARALLEL GROUP — 2 tasks)

> Depends on Phase 1A complete. Launch both as parallel Task subagents.

---

### TASK 1B.1 — Protobuf Schema & Codec

**Package:** `packages/proto/`  
**Depends on:** Phase 1A (types from core)

**Files:**
- `src/lakesync.proto`
- `src/codec.ts` — encode/decode using `protobuf-es`
- `src/__tests__/codec.test.ts`
- `buf.gen.yaml`

**Proto (minimal Phase 1):**

```protobuf
syntax = "proto3";
package lakesync.v1;

enum DeltaOp {
  DELTA_OP_UNSPECIFIED = 0;
  DELTA_OP_INSERT = 1;
  DELTA_OP_UPDATE = 2;
  DELTA_OP_DELETE = 3;
}

message ColumnDelta {
  string column = 1;
  bytes value = 2;   // JSON-encoded, UTF-8. NEVER undefined — use JSON null.
}

message RowDelta {
  DeltaOp op = 1;
  string table = 2;
  string row_id = 3;
  repeated ColumnDelta columns = 4;
  fixed64 hlc = 5;
  string client_id = 6;   // required — matches core RowDelta.clientId
  string delta_id = 7;    // deterministic hash — enables idempotent re-push
}

message SyncPush {
  string client_id = 1;
  repeated RowDelta deltas = 2;
  fixed64 last_seen_hlc = 3;
}

message SyncPull {
  string client_id = 1;
  fixed64 since_hlc = 2;
  uint32 max_deltas = 3;
}

message SyncResponse {
  repeated RowDelta deltas = 1;
  fixed64 server_hlc = 2;
  bool has_more = 3;
}
```

Use `protobuf-es` (Buf's TypeScript protobuf library) for code generation.

**⚠ CRITICAL — BigInt ↔ Protobuf fixed64 bridge:**
JavaScript `bigint` does NOT serialise to JSON natively (`JSON.stringify(1n)` throws). The codec must:
1. Use `protobuf-es`'s native `bigint` support for `fixed64` fields (it handles this correctly).
2. If any JSON serialisation is needed (e.g. logging, debugging), add a project-level `BigInt.prototype.toJSON` patch in an **explicit opt-in file** (NOT auto-imported by core index — it's a global side effect):
   ```typescript
   // packages/core/src/bigint-patch.ts — import explicitly where needed
   (BigInt.prototype as any).toJSON = function () { return this.toString(); };
   ```
3. The codec's `toRowDelta()` / `fromRowDelta()` converters must explicitly map between the core `HLCTimestamp` (branded bigint) and the protobuf-es generated `fixed64` field.
4. Add a test that roundtrips `HLCTimestamp` → protobuf bytes → `HLCTimestamp` and verifies exact equality.

**Tests (minimum 7):**
1. `SyncPush` roundtrip with multiple deltas
2. `SyncPull` roundtrip
3. `SyncResponse` roundtrip with `has_more = true`
4. HLC preserved through `fixed64` (bigint exact equality)
5. Empty deltas array
6. Large payload (1000 deltas) no corruption
7. `HLCTimestamp` → proto → `HLCTimestamp` exact roundtrip

**Done when:** proto compiles, all roundtrip tests pass, exported from `packages/proto/src/index.ts`.

---

### TASK 1B.2 — Lake Adapter Interface + MinIO Implementation

**Package:** `packages/adapter/`  
**Depends on:** Phase 1A (Result type)

**Files:**
- `src/types.ts` — `LakeAdapter` interface
- `src/minio.ts` — `MinIOAdapter`
- `src/__tests__/minio.test.ts` (integration, needs Docker)

```typescript
export interface LakeAdapter {
  putObject(path: string, data: Uint8Array, contentType?: string): Promise<Result<void, AdapterError>>;
  getObject(path: string): Promise<Result<Uint8Array, AdapterError>>;
  headObject(path: string): Promise<Result<{ size: number; lastModified: Date }, AdapterError>>;
  listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>>;
  deleteObject(path: string): Promise<Result<void, AdapterError>>;
  deleteObjects(paths: string[]): Promise<Result<void, AdapterError>>;
}

export interface ObjectInfo {
  key: string;             // S3 object key (consistent with AWS SDK terminology)
  size: number;
  lastModified: Date;
}

export interface AdapterConfig {
  endpoint: string;
  bucket: string;
  region?: string;
  credentials: { accessKeyId: string; secretAccessKey: string };
}
```

Use `@aws-sdk/client-s3`. All methods return `Result` — never throw.

**Integration tests (minimum 8, require docker compose up):**
1. `putObject` + `getObject` roundtrip
2. `putObject` overwrites
3. `getObject` missing → Err
4. `headObject` correct size/date
5. `listObjects` prefix filtering
6. `deleteObject` then `getObject` → Err
7. `deleteObjects` batch
8. Connection failure → AdapterError (not thrown)

Use `describe.skipIf` when Docker not available. **Detection:** attempt a lightweight `HEAD` request to the MinIO endpoint in `beforeAll`, not just env var check — avoids flaky CI failures when Docker is up but MinIO hasn't started yet.

**Done when:** all tests pass with Docker, skip cleanly without it. Exported from `packages/adapter/src/index.ts`.

---

## Phase 1C — Conflict Resolution & Sync Queue (PARALLEL GROUP — 2 tasks)

> Depends on Phase 1A + 1B complete. Launch both as parallel Task subagents.

---

### TASK 1C.1 — Column-Level Last-Write-Wins Resolver

**Package:** `packages/core/src/conflict/`  
**Depends on:** Tasks 1A.1, 1A.2

**Files:**
- `src/conflict/lww.ts`
- `src/conflict/resolver.ts` — `ConflictResolver` interface
- `src/conflict/__tests__/lww.test.ts`

```typescript
export interface ConflictResolver {
  resolve(local: RowDelta, remote: RowDelta): Result<RowDelta, ConflictError>;
}

// Column-level LWW: for each column in both deltas,
// higher HLC wins. Columns only in one delta always included.
export function resolveLWW(local: RowDelta, remote: RowDelta): Result<RowDelta, ConflictError>;
```

**⚠ NOTE:** `RowDelta` now includes `clientId` (added in Task 1A.2). The LWW resolver uses it for equal-HLC tiebreak: lexicographically higher `clientId` wins. This is deterministic and consistent across all clients.

**Tests (minimum 10):**
1. No overlapping columns → union
2. Same column, remote HLC higher → remote wins
3. Same column, local HLC higher → local wins
4. Equal HLC → deterministic tiebreak (higher clientId wins)
5. INSERT vs UPDATE → merge columns
6. DELETE vs UPDATE, DELETE has higher HLC → DELETE wins
7. DELETE vs UPDATE, UPDATE has higher HLC → UPDATE wins (row resurrected)
8. UPDATE older than DELETE tombstone → tombstone wins, row stays deleted
9. DELETE vs DELETE → no-op
10. Mismatched table/rowId → ConflictError

**Done when:** all tests pass, exported from `packages/core/src/index.ts`.

---

### TASK 1C.2 — Client Sync Queue (Outbox Pattern)

**Package:** `packages/client/src/queue/`  
**Depends on:** Tasks 1A.1, 1A.2, 1A.3

**Files:**
- `src/queue/types.ts`
- `src/queue/memory-queue.ts` — in-memory (tests + Node)
- `src/queue/idb-queue.ts` — IndexedDB-backed (browser)
- `src/queue/__tests__/memory-queue.test.ts`
- `src/queue/__tests__/idb-queue.test.ts` (use `fake-indexeddb`)

```typescript
export interface QueueEntry {
  id: string;
  delta: RowDelta;
  status: 'pending' | 'sending' | 'acked';
  createdAt: number;
  retryCount: number;
}

export interface SyncQueue {
  push(delta: RowDelta): Promise<Result<QueueEntry, LakeSyncError>>;
  peek(limit: number): Promise<Result<QueueEntry[], LakeSyncError>>;
  markSending(ids: string[]): Promise<Result<void, LakeSyncError>>;
  ack(ids: string[]): Promise<Result<void, LakeSyncError>>;
  nack(ids: string[]): Promise<Result<void, LakeSyncError>>;
  depth(): Promise<Result<number, LakeSyncError>>;
  clear(): Promise<Result<void, LakeSyncError>>;
}
```

**No localStorage/sessionStorage.** IDB uses `idb` library. `fake-indexeddb` for tests.

**⚠ CRITICAL — IDB concurrency:**
The `peek` + `markSending` pattern is racy if done in separate transactions. The IDB implementation MUST use a **single readwrite transaction** that queries pending entries and atomically updates their status to `sending` (a "claim" operation). Implement this as a combined `claimBatch(limit)` method internally, with `peek` + `markSending` as the public API that delegates to it.

**Tests (minimum 10, for each implementation):**
1. `push` → status `pending`
2. `peek` ordered by `createdAt`
3. `peek` respects limit
4. `peek` only returns `pending`
5. `markSending` transitions status
6. `ack` removes entries
7. `nack` resets to `pending`, increments `retryCount`
8. `depth` correct count
9. `clear` empties queue
10. Concurrent peek + markSending no double-process

**Done when:** all tests pass both implementations. Exported from `packages/client/src/index.ts`.

---

## Phase 1D — Gateway (SEQUENTIAL — single task)

> Depends on Phase 1C complete. This is the integration point — run as one task.

---

### TASK 1D.1 — Sync Gateway (In-Memory Simulation)

**Package:** `packages/gateway/`  
**Depends on:** Tasks 1A.*, 1B.1, 1C.1

Phase 1 implements gateway logic as a plain TypeScript class — not on Cloudflare yet.
Phase 2 wraps it in a real Durable Object.

**Files:**
- `src/types.ts` — `GatewayConfig`, `GatewayState`
- `src/gateway.ts` — `SyncGateway` class
- `src/buffer.ts` — `DeltaBuffer`
- `src/__tests__/gateway.test.ts`
- `src/__tests__/buffer.test.ts`

**`SyncGateway` responsibilities:**

1. **handlePush(msg: SyncPush):** Validate HLC drift (compare remote wall vs server wall, NOT vs last local HLC) → conflict resolve (LWW) against buffer index → append to log + upsert index → return ack with server HLC
2. **handlePull(msg: SyncPull):** Return **change events** (not snapshots) from the log since `since_hlc`, paginate with `max_deltas` + `has_more`
3. **flush (buffer → adapter):** Triggered at threshold (100MB or timer). Write the log as a **FlushEnvelope** (see below). Clear on success. Retain + retry on failure.

**⚠ CRITICAL — DeltaBuffer dual structure:**
The buffer maintains TWO internal structures:
- **Log** (`RowDelta[]`): append-only ordered stream. Used by `handlePull` (returns change events) and `flush` (writes to adapter).
- **Index** (`Map<RowKey, RowDelta>`): latest merged state per row. Used by `handlePush` for conflict resolution (O(1) lookup, not O(n) scan).

This separation resolves the review concern: pull returns events, conflict resolution uses snapshots.

```typescript
import type { RowKey } from '@lakesync/core';

export class DeltaBuffer {
  private log: RowDelta[] = [];
  private index: Map<RowKey, RowDelta> = new Map();

  /** Append to log AND upsert index (post-conflict-resolution) */
  append(delta: RowDelta): void;

  /** Get the current merged state for a row (for conflict resolution) */
  getRow(key: RowKey): RowDelta | undefined;

  /** Return change events from log since HLC (for pull) */
  getEventsSince(table: string, hlc: HLCTimestamp, limit: number): { deltas: RowDelta[]; hasMore: boolean };

  shouldFlush(config: { maxBytes: number; maxAgeMs: number }): boolean;

  /** Drain log for flush. Returns log entries and clears both log + index. */
  drain(): RowDelta[];

  readonly logSize: number;   // number of log entries
  readonly indexSize: number;  // number of unique rows
  readonly byteSize: number;   // estimated bytes
}
```

**⚠ CRITICAL — Flush file format contract (Phase 1→2 migration safety):**
Even though Phase 1 writes JSON (not Parquet), wrap it in a versioned envelope so Phase 2 doesn't need to re-teach consumers.

```typescript
/** Written to adapter on flush */
export interface FlushEnvelope {
  version: 1;
  gatewayId: string;
  createdAt: string;  // ISO 8601
  hlcRange: { min: HLCTimestamp; max: HLCTimestamp };
  deltaCount: number;
  byteSize: number;
  deltas: RowDelta[];
}
```

**Object key pattern:** `deltas/{YYYY-MM-DD}/{gatewayId}/{minHlc}-{maxHlc}.json`  
This pattern is stable across Phase 1 (JSON) → Phase 2 (Parquet, just change extension).

**⚠ CRITICAL — Idempotent re-push after gateway restart:**
Since `RowDelta` now carries `deltaId`, the gateway can deduplicate on re-push. In Phase 1, the buffer is in-memory and lost on restart, so the client re-pushes unacked deltas. The gateway checks `deltaId` against the index — if already present with same HLC, it's a no-op. Add ADR-009 for this invariant.

**Tests (minimum 14):**
1. Push single delta → log contains it, index contains it
2. Push + pull returns the delta from log
3. Pull `since_hlc` filters correctly (log-based)
4. Pull pagination (`has_more`)
5. Push with future drift → `Err(ClockDriftError)` (drift validated against server wall)
6. Concurrent updates to same row: index reflects merged state
7. Concurrent updates to same row: log contains both events
8. Buffer `shouldFlush` at byte threshold
9. Buffer `shouldFlush` at age threshold
10. Buffer `drain` returns log entries and clears both structures
11. Flush writes FlushEnvelope to adapter (mock) with correct key pattern
12. Flush failure retains buffer (both log and index intact)
13. Gateway HLC advances on each push
14. Re-push same deltaId → idempotent (no duplicate in log, index unchanged)

**Done when:** all tests pass, flush tested with mock adapter. Exported from `packages/gateway/src/index.ts`.

---

## Phase 1E — Integration & App (SEQUENTIAL then PARALLEL)

> 1E.1 is sequential. Then 1E.2 + 1F tasks can run in parallel.

---

### TASK 1E.1 — End-to-End Integration Tests

**Package:** root `tests/integration/`  
**Depends on:** all prior tasks

**Files:**
- `tests/integration/e2e-sync.test.ts`
- `tests/integration/multi-client.test.ts`
- `tests/integration/conflict.test.ts`
- `tests/integration/helpers.ts`

**Scenarios:**
1. **Single client sync:** Push 10 deltas → gateway buffers → force flush → adapter receives data
2. **Two clients:** A writes col X, B writes col Y same row → both sync → merged row
3. **Conflict:** A and B write same column → LWW resolves → both converge
4. **Offline queue:** Client queues 50 offline → comes online → all pushed
5. **Gateway restart:** Buffer lost → client re-pushes unacked (idempotency)
6. **Clock drift:** Bad clock rejected gracefully

Use in-memory queue + in-memory gateway + MinIO adapter (Docker).

**Done when:** all pass, <30s execution.

---

### TASK 1E.2 — Todo App Reference Implementation

**Package:** `apps/examples/todo-app/`  
**Depends on:** Task 1E.1

Minimal reference app — validates the full pipeline works.

**Files:**
- `package.json` (Vite + vanilla TS)
- `src/main.ts`, `src/db.ts` (in-memory Map — not real SQLite yet), `src/sync.ts`, `src/ui.ts`
- `index.html`
- `README.md`

**Scope:**
- Todo CRUD: `id`, `title`, `completed`, `created_at`, `updated_at`
- Writes to in-memory Map, extracts deltas, queues
- Syncs to in-process gateway (function calls, not network)
- Gateway flushes to MinIO
- **No SQLite Wasm, no framework, no OPFS** — Phase 2 concerns

**README must work in <15 minutes:**
```
1. git clone + bun install
2. docker compose -f docker/docker-compose.yml up -d
3. bun run dev --filter=todo-app
4. Open http://localhost:5173
5. Add a todo → see it in MinIO bucket
```

**Done when:** app builds, todos produce deltas, data visible in MinIO after flush.

---

## Phase 1F — Documentation (PARALLEL GROUP — 2 tasks)

> Can run in parallel with Task 1E.2.

---

### TASK 1F.1 — Architecture Decision Records

**Package:** `adrs/`  
**Depends on:** all implementation complete (documents decisions)

Create these ADRs using this template:

```markdown
# ADR-XXX: [Title]
**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context
## Decision
## Consequences
### Positive
### Negative
### Risks
## Alternatives Considered
```

| ADR | Title |
|-----|-------|
| ADR-001 | Column-level delta granularity |
| ADR-002 | Column-level Last-Write-Wins conflict resolution |
| ADR-003 | Merge-on-Read with equality deletes |
| ADR-004 | Iceberg REST Catalogue specification |
| ADR-005 | No multi-table transactions in v1.0 |
| ADR-006 | Hybrid Logical Clocks (64-bit, 5s drift) |
| ADR-007 | Result pattern over thrown exceptions |
| ADR-008 | Protobuf wire protocol |
| ADR-009 | Deterministic delta identity and idempotent push |

**Done when:** all 9 ADRs in `adrs/`, British English throughout.

**⚠ British English reminder:** LLMs default to American English for technical docs. Explicitly use: "serialise", "initialise", "optimise", "synchronise", "catalogue", "behaviour", "colour", "centre", "licence" (noun). Review each ADR for American spellings before marking done.

---

### TASK 1F.2 — Package READMEs

**Package:** each `packages/*/`  
**Depends on:** respective implementation

Create `README.md` for: `core`, `client`, `gateway`, `adapter`, `proto`.

Each includes:
1. Purpose (1 paragraph)
2. Install: `bun add @lakesync/<pkg>`
3. Quick usage example
4. API surface
5. Testing instructions

**Done when:** all 5 READMEs created, British English, code examples compile.

---

## Execution Flow — Dependency Graph

```
Phase 0 (parallel tasks):
    ├── Task 0.1 — Monorepo scaffold
    └── Task 0.2 — Docker compose
         │
         ▼
Phase 1A (3 parallel tasks):
    ├── Task 1A.1 — HLC
    ├── Task 1A.2 — Delta extraction
    └── Task 1A.3 — Result type
         │
         ▼
Phase 1B (2 parallel tasks):
    ├── Task 1B.1 — Protobuf codec
    └── Task 1B.2 — MinIO adapter
         │
         ▼
Phase 1C (2 parallel tasks):
    ├── Task 1C.1 — LWW resolver
    └── Task 1C.2 — Sync queue
         │
         ▼
Phase 1D (1 task):
    └── Task 1D.1 — Gateway
         │
         ▼
Phase 1E.1 (1 task):
    └── Integration tests
         │
         ├─── parallel ───┐
         ▼                ▼
Phase 1E.2 + 1F (3 parallel tasks):
    ├── Task 1E.2 — Todo app
    ├── Task 1F.1 — ADRs
    └── Task 1F.2 — READMEs
```

**Total tasks:** 14  
**Max parallel subagents at once:** 3  
**Critical path:** 0.1 → 1A.1 → 1B.1 → 1C.1 → 1D.1 → 1E.1 → 1E.2
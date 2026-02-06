# Phase 2D — Cloudflare Workers Gateway

**Goal:** Port the SyncGateway to Cloudflare Workers using Durable Objects for stateful WebSocket handling, R2 for object storage, and alarm-based flush scheduling.

**Depends on:** Phase 2A (Parquet writer) + Phase 2B (Iceberg catalogue)
**Blocks:** none (enables production deployment)

---

## SEQUENTIAL GROUP (all tasks in order)

### Task 2D.1: Wrangler project setup

- **Package:** `apps/gateway-worker/` (new app)
- **Creates:**
  - `apps/gateway-worker/package.json`
  - `apps/gateway-worker/tsconfig.json`
  - `apps/gateway-worker/wrangler.toml`
  - `apps/gateway-worker/src/index.ts` (Worker entrypoint)
  - `apps/gateway-worker/src/env.ts` (Env type bindings)
- **Modifies:** none
- **Dependencies:** none
- **Implementation:**
  1. Scaffold Cloudflare Worker project:
     ```toml
     # wrangler.toml
     name = "lakesync-gateway"
     main = "src/index.ts"
     compatibility_date = "2025-01-01"

     [[durable_objects.bindings]]
     name = "SYNC_GATEWAY"
     class_name = "SyncGatewayDO"

     [[r2_buckets]]
     binding = "LAKE_BUCKET"
     bucket_name = "lakesync-data"
     ```
  2. `src/env.ts`:
     ```typescript
     export interface Env {
       SYNC_GATEWAY: DurableObjectNamespace;
       LAKE_BUCKET: R2Bucket;
       NESSIE_URI: string;          // secret
       JWT_SECRET: string;          // secret
     }
     ```
  3. `src/index.ts` — minimal Worker that routes requests to DO:
     - `GET /sync/:gatewayId` → upgrade to WebSocket, forward to DO
     - `POST /sync/:gatewayId/push` → forward to DO (HTTP fallback)
     - `GET /sync/:gatewayId/pull` → forward to DO
     - `POST /admin/flush/:gatewayId` → trigger manual flush
  4. Package dependencies: `@lakesync/core`, `@lakesync/gateway`, `@lakesync/parquet`, `@lakesync/catalogue`
- **Tests:** Wrangler types check (`wrangler types`), basic request routing
- **Done when:** Wrangler project builds and deploys to local dev

---

### Task 2D.2: Durable Object wrapper

- **Package:** `apps/gateway-worker/`
- **Creates:**
  - `apps/gateway-worker/src/sync-gateway-do.ts`
- **Modifies:**
  - `apps/gateway-worker/src/index.ts` (export DO class)
- **Dependencies:** Task 2D.1
- **Implementation:**
  1. `SyncGatewayDO` class extending `DurableObject`:
     ```typescript
     export class SyncGatewayDO extends DurableObject {
       private gateway: SyncGateway | null = null;

       private getGateway(): SyncGateway { /* lazy init */ }

       async fetch(request: Request): Promise<Response> { /* route handler */ }

       async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string): void { /* proto decode → handlePush/Pull */ }

       async webSocketClose(ws: WebSocket): void { /* cleanup */ }

       async alarm(): Promise<void> { /* periodic flush */ }
     }
     ```
  2. WebSocket handling:
     - Accept WebSocket upgrade → `this.ctx.acceptWebSocket(ws)`
     - Incoming binary messages → `decodeSyncPush()` → `gateway.handlePush()`
     - Response → `encodeSyncResponse()` → `ws.send(bytes)`
     - Pull requests → `decodeSyncPull()` → `gateway.handlePull()` → encode + send
  3. HTTP fallback:
     - `POST /push` → read body as proto → `handlePush()` → proto response
     - `GET /pull?since=&limit=` → `handlePull()` → proto response
  4. Gateway state lives in DO memory (ephemeral buffer). Durable state = flushed Parquet files.
  5. Export DO class from `src/index.ts`
- **Tests:** Unit test with miniflare (mock R2, mock WebSocket)
- **Done when:** DO handles push/pull over WebSocket and HTTP

---

### Task 2D.3: R2 adapter

- **Package:** `packages/adapter/` + `apps/gateway-worker/`
- **Creates:**
  - `packages/adapter/src/r2.ts`
  - `packages/adapter/src/__tests__/r2.test.ts`
  - OR: `apps/gateway-worker/src/r2-adapter.ts` (if R2 types are worker-only)
- **Modifies:**
  - `packages/adapter/src/index.ts` (add R2 export, if in adapter package)
- **Dependencies:** Task 2D.1
- **Implementation:**
  1. `R2Adapter` implementing `LakeAdapter`:
     ```typescript
     export class R2Adapter implements LakeAdapter {
       constructor(private bucket: R2Bucket) {}

       async putObject(path: string, data: Uint8Array, contentType?: string): Promise<Result<void, AdapterError>>
       async getObject(path: string): Promise<Result<Uint8Array, AdapterError>>
       async headObject(path: string): Promise<Result<{ size: number; lastModified: Date }, AdapterError>>
       async listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>>
       async deleteObject(path: string): Promise<Result<void, AdapterError>>
       async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>>
     ```
  2. R2 API mapping:
     - `putObject` → `bucket.put(key, data, { httpMetadata: { contentType } })`
     - `getObject` → `bucket.get(key)` → `.arrayBuffer()` → `new Uint8Array()`
     - `headObject` → `bucket.head(key)`
     - `listObjects` → `bucket.list({ prefix })`
     - `deleteObject` → `bucket.delete(key)`
     - `deleteObjects` → `bucket.delete(keys)` (R2 supports batch)
  3. All methods wrapped in try/catch → `Result<T, AdapterError>`
- **Tests:** Mock R2Bucket in tests (miniflare or manual mock)
- **Done when:** R2Adapter implements full LakeAdapter interface, all methods tested

---

### Task 2D.4: Alarm-based flush

- **Package:** `apps/gateway-worker/`
- **Creates:** none (modifies DO)
- **Modifies:**
  - `apps/gateway-worker/src/sync-gateway-do.ts` — add alarm handling
- **Dependencies:** Task 2D.2 + 2D.3
- **Implementation:**
  1. In `SyncGatewayDO`:
     - After each `handlePush()`, check `gateway.shouldFlush()`
     - If should flush: `this.ctx.storage.setAlarm(Date.now())` (immediate)
     - Also set periodic alarm: `this.ctx.storage.setAlarm(Date.now() + maxBufferAgeMs)` after each push
  2. `alarm()` handler:
     - Call `gateway.flush()` (uses R2Adapter + optional catalogue)
     - If flush fails: reschedule alarm with exponential backoff
     - If flush succeeds + buffer still has data: reschedule immediately
     - If flush succeeds + buffer empty: no reschedule (next push will set alarm)
  3. Guard against concurrent flushes (gateway already has `this.flushing` flag)
  4. Metrics: log flush duration, delta count, Parquet size via `console.log` (CF logs)
- **Tests:**
  - Push → alarm fires → flush to R2 → verify object exists
  - Multiple pushes → single flush (alarm coalescing)
  - Flush failure → alarm rescheduled
- **Done when:** DO flushes automatically via alarms, data reaches R2

---

### Task 2D.5: JWT auth middleware

- **Package:** `apps/gateway-worker/`
- **Creates:**
  - `apps/gateway-worker/src/auth.ts`
- **Modifies:**
  - `apps/gateway-worker/src/index.ts` — add auth check before routing
- **Dependencies:** Task 2D.1
- **Implementation:**
  1. `src/auth.ts`:
     ```typescript
     export async function verifyToken(
       token: string,
       secret: string,
     ): Promise<Result<{ clientId: string; gatewayId: string }, AuthError>>
     ```
  2. JWT verification using Web Crypto API (no external deps):
     - HMAC-SHA256 signature verification
     - Check `exp` claim (reject expired)
     - Extract `sub` (clientId) and `gw` (gatewayId) claims
  3. Middleware in `src/index.ts`:
     - Extract `Authorization: Bearer <token>` header
     - For WebSocket: check token in `Sec-WebSocket-Protocol` or query param
     - Verify → extract clientId → pass to DO
     - 401 on missing/invalid token
  4. Skip auth in dev mode (via `wrangler.toml` var or env flag)
- **Tests:**
  - Valid token → extracted claims
  - Expired token → 401
  - Missing token → 401
  - Tampered token → 401
- **Done when:** All routes require valid JWT, clientId extracted and forwarded to DO

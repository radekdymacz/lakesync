# Phase 10 — Production Hardening

## Goal
Make LakeSync production-ready across the gateway, client, adapter, and observability layers.

---

## PARALLEL GROUP A — Transport & Client Hardening

### A1. Token Refresh Callback on Transports
**Files:** `packages/client/src/sync/transport-http.ts`, `packages/client/src/sync/transport-ws.ts`, `packages/client/src/sync/transport.ts`

Add a `getToken` callback to transport configs so tokens can be refreshed on each request:

1. Add `getToken?: () => string | Promise<string>` to `HttpTransportConfig`
2. In `HttpTransport`, use `getToken()` (if provided) instead of static `this.token` before each fetch call. Fall back to `this.token` if `getToken` is not set.
3. Add `getToken?: () => string | Promise<string>` to `WebSocketTransportConfig` (if it exists as a separate config)
4. In `WebSocketTransport.connect()`, call `getToken()` to get a fresh token before reconnecting.
5. On HTTP 401 response in `HttpTransport.push()` and `pull()`, if `getToken` is provided, refresh the token and retry once before returning the error.
6. Add tests: verify getToken is called, verify 401 retry with fresh token works, verify backward compat (no getToken = same behaviour).

### A2. Offline Detection on Client
**Files:** `packages/client/src/sync/coordinator.ts`

Integrate `navigator.onLine` (when available) to skip sync when offline:

1. In `SyncCoordinator`, add an `online` property (default `true`).
2. In `startAutoSync()`, register `window.addEventListener('online'/'offline')` listeners if `typeof window !== 'undefined'`.
3. In the auto-sync interval callback, skip `sync()` if `this.online === false`.
4. When coming back online, trigger an immediate sync.
5. Expose `readonly isOnline: boolean` for consumers.
6. Guard all window/navigator access with `typeof` checks for Node/SSR.
7. Add tests: mock navigator.onLine, verify sync skipped when offline, verify immediate sync on reconnect.

---

## PARALLEL GROUP B — Gateway Server Production Readiness

### B1. Graceful Shutdown (SIGTERM + Drain)
**Files:** `packages/gateway-server/src/server.ts`

1. Add `setupSignalHandlers()` called from `start()`:
   - Listen for `SIGTERM` and `SIGINT`.
   - On signal: set `this.draining = true`, log, call `this.shutdown()`.
2. Add `private async shutdown()`:
   - Stop accepting new connections (`server.close()`).
   - Wait for in-flight requests to complete (track active request count with `++/--` around handler).
   - Wait up to `drainTimeoutMs` (default 10s, configurable) for active requests to drain.
   - Final flush: call `this.gateway.flush()` to persist buffer.
   - Then call existing `stop()` to clean up pollers/ws/timers.
   - `process.exit(0)`.
3. While draining, reject new push/pull requests with 503 Service Unavailable.
4. Add `drainTimeoutMs?: number` to `GatewayServerConfig`.
5. Add tests: verify SIGTERM triggers shutdown, verify 503 during drain, verify final flush.

### B2. Health & Readiness Probes
**Files:** `packages/gateway-server/src/server.ts`

Enhance `/health` and add `/ready`:

1. `/health` (liveness): keep as-is — returns `{ status: "ok" }` if HTTP server is up.
2. Add `/ready` (readiness): checks that:
   - Server is not draining.
   - If adapter is configured, it's reachable (e.g., for DatabaseAdapter, run a lightweight query like `SELECT 1`).
   - Returns `{ status: "ready" }` (200) or `{ status: "not_ready", reason: "..." }` (503).
3. Add `checkAdapterHealth()` method on gateway-server:
   - For DatabaseAdapter: try `ensureSchema` or a simple query with a 5s timeout.
   - For LakeAdapter: try `headObject` on a known key or just return ok (S3 is stateless).
   - Wrap in try/catch with timeout, return boolean.
4. Add tests: verify /ready returns 503 during drain, verify /ready checks adapter.

### B3. Request Timeouts
**Files:** `packages/gateway-server/src/server.ts`, `packages/gateway/src/flush-coordinator.ts`

1. Add `requestTimeoutMs?: number` (default 30s) to `GatewayServerConfig`.
2. In `handleRequest()`, set `res.setTimeout(this.config.requestTimeoutMs)` — abort and return 504 on timeout.
3. In `periodicFlush()`, wrap `this.gateway.flush()` in a `Promise.race` with a timeout (default 60s). Log a warning on timeout.
4. Add `flushTimeoutMs?: number` to `GatewayServerConfig` (default 60s).
5. Add tests: verify request timeout returns 504, verify flush timeout logs warning.

---

## PARALLEL GROUP C — Adapter Hardening

### C1. Configurable Pool Options for Postgres & MySQL
**Files:** `packages/adapter/src/postgres.ts`, `packages/adapter/src/mysql.ts`, `packages/adapter/src/db-types.ts`

1. Extend `DatabaseAdapterConfig` in `db-types.ts`:
   ```ts
   export interface DatabaseAdapterConfig {
     connectionString: string;
     /** Maximum number of connections in the pool (default: 10). */
     poolMax?: number;
     /** Connection idle timeout in milliseconds (default: 10000). */
     idleTimeoutMs?: number;
     /** Connection acquisition timeout in milliseconds (default: 30000). */
     connectionTimeoutMs?: number;
     /** Statement timeout in milliseconds (default: 30000). */
     statementTimeoutMs?: number;
   }
   ```
2. In `PostgresAdapter` constructor, map config to `PoolConfig`:
   - `max: config.poolMax ?? 10`
   - `idleTimeoutMillis: config.idleTimeoutMs ?? 10_000`
   - `connectionTimeoutMillis: config.connectionTimeoutMs ?? 30_000`
   - `statement_timeout: config.statementTimeoutMs ?? 30_000`
3. In `MySQLAdapter` constructor, map to mysql2 pool options:
   - `connectionLimit: config.poolMax ?? 10`
   - `connectTimeout: config.connectionTimeoutMs ?? 30_000`
   - `idleTimeout: config.idleTimeoutMs ?? 10_000` (if supported by mysql2)
4. Add tests: verify custom pool config is passed through, verify default values used when not specified.

---

## PARALLEL GROUP D — Rate Limiting & WebSocket Backpressure

### D1. Per-Client Rate Limiting
**Files:** `packages/gateway-server/src/rate-limiter.ts` (new), `packages/gateway-server/src/server.ts`

1. Create `RateLimiter` class using token bucket algorithm:
   ```ts
   export interface RateLimiterConfig {
     /** Max requests per window (default: 100). */
     maxRequests?: number;
     /** Window size in milliseconds (default: 60_000). */
     windowMs?: number;
   }
   export class RateLimiter {
     tryConsume(clientId: string): boolean;
     reset(): void;
   }
   ```
2. Use a `Map<string, { count: number; windowStart: number }>` for tracking.
3. Periodically clean up stale entries (every 60s).
4. In `GatewayServerConfig`, add `rateLimiter?: RateLimiterConfig`.
5. In `handleRequest()`, after auth but before route dispatch:
   - Extract clientId from auth claims (or IP fallback).
   - If `rateLimiter.tryConsume(clientId)` returns false, return 429 Too Many Requests with `Retry-After` header.
6. Add tests: verify rate limiting kicks in, verify Retry-After header, verify cleanup.

### D2. WebSocket Connection Limits & Message Rate Limiting
**Files:** `packages/gateway-server/src/ws-manager.ts`

1. Add `maxConnections?: number` (default: 1000) and `maxMessagesPerSecond?: number` (default: 50) to `WebSocketManager` constructor params.
2. In `handleUpgrade()`, reject new connections with 503 if `this.clients.size >= maxConnections`.
3. Add per-client message rate tracking: `Map<WsWebSocket, { messageCount: number; windowStart: number }>`.
4. In `handleMessage()`, check rate. If exceeded, send a close frame with code 1008 "Rate limit exceeded".
5. Reset rate counters every second via `setInterval`.
6. Clean up interval on `close()`.
7. Add tests: verify connection limit, verify message rate limit, verify cleanup.

---

## PARALLEL GROUP E — Observability

### E1. Structured Logger
**Files:** `packages/gateway-server/src/logger.ts` (new), update `server.ts`

1. Create a minimal structured logger (no external deps):
   ```ts
   export type LogLevel = "debug" | "info" | "warn" | "error";
   export interface LogEntry {
     level: LogLevel;
     msg: string;
     ts: string;
     requestId?: string;
     [key: string]: unknown;
   }
   export class Logger {
     constructor(private minLevel: LogLevel = "info");
     info(msg: string, data?: Record<string, unknown>): void;
     warn(msg: string, data?: Record<string, unknown>): void;
     error(msg: string, data?: Record<string, unknown>): void;
     debug(msg: string, data?: Record<string, unknown>): void;
     child(bindings: Record<string, unknown>): Logger;
   }
   ```
2. Output JSON lines to stdout (compatible with any log aggregator).
3. Add `requestId` generation (nanoid or crypto.randomUUID) in `handleRequest()`, pass via `child()` logger.
4. Replace all `console.log`/`console.warn` in gateway-server with logger calls.
5. Add `logLevel?: LogLevel` to `GatewayServerConfig`.
6. Add tests: capture stdout, verify JSON structure, verify log levels.

### E2. Prometheus-Compatible Metrics
**Files:** `packages/gateway-server/src/metrics.ts` (new), update `server.ts`

1. Create a `Metrics` class tracking:
   - `lakesync_push_total` — counter (labels: status)
   - `lakesync_pull_total` — counter (labels: status)
   - `lakesync_flush_total` — counter (labels: status)
   - `lakesync_flush_duration_ms` — histogram (buckets: 10, 50, 100, 500, 1000, 5000)
   - `lakesync_buffer_bytes` — gauge
   - `lakesync_buffer_deltas` — gauge
   - `lakesync_ws_connections` — gauge
   - `lakesync_push_latency_ms` — histogram (buckets: 1, 5, 10, 50, 100, 500)
   - `lakesync_active_requests` — gauge
2. Add `GET /metrics` endpoint that returns Prometheus text exposition format.
3. Instrument `handlePush`, `handlePull`, `handleFlush` to record counters and latencies.
4. Update buffer gauge on each push/flush.
5. Update ws_connections gauge on connect/disconnect in WsManager.
6. Add tests: verify counter increments, verify Prometheus format output.

---

## PARALLEL GROUP F — Connector Cursor Persistence

### F1. Persist Connector Cursors
**Files:** `packages/gateway-server/src/connector-manager.ts`, `packages/gateway-server/src/persistence.ts`

1. Add cursor persistence methods to `DeltaPersistence` interface:
   ```ts
   saveCursor(connectorName: string, cursor: string): void;
   loadCursor(connectorName: string): string | null;
   ```
2. Implement in `MemoryPersistence` (in-memory map) and `SqlitePersistence` (new table `connector_cursors`).
3. In `ConnectorManager.register()`, load persisted cursor and pass to poller on start.
4. In poller `afterPoll` callback (or periodic save), persist the current cursor.
5. On crash recovery + restart, pollers resume from persisted cursor instead of full resync.
6. Add tests: verify cursor saved, verify cursor loaded on restart, verify full sync when no cursor.

---

## Execution Order

All groups (A through F) are independent and can run in parallel.

Within each group, tasks are sequential (e.g., A1 before A2 within group A — though they're actually independent too).

## Test Strategy

Each task includes its own tests. After all tasks complete:
1. Run `bun run typecheck` — all packages must pass.
2. Run `bun run test` — all existing + new tests must pass.
3. Run `bun run lint` — biome check must pass.

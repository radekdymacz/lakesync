export { type AuthClaims, AuthError, verifyToken } from "./auth";
export { type AuthResult, authenticateRequest, extractBearerToken } from "./auth-middleware";
export { AdapterBasedLock, type DistributedLock } from "./cluster";
export { ConnectorManager } from "./connector-manager";
export { type CorsConfig, corsHeaders, handlePreflight } from "./cors-middleware";
export {
	type CursorStrategy,
	type DiffStrategy,
	type IngestSourceConfig,
	type IngestTableConfig,
	type QueryFn,
	SourcePoller,
} from "./ingest";
export { type LogEntry, Logger, type LogLevel } from "./logger";
export {
	Counter,
	Gauge,
	Histogram,
	type Labels,
	MetricsRegistry,
} from "./metrics";
export {
	type DeltaPersistence,
	MemoryPersistence,
	SqlitePersistence,
} from "./persistence";
export { RateLimiter, type RateLimiterConfig } from "./rate-limiter";
export { matchRoute, type RouteMatch } from "./router";
export { GatewayServer, type GatewayServerConfig } from "./server";
export {
	type ConsistencyMode,
	SharedBuffer,
	type SharedBufferConfig,
	type SharedBufferError,
} from "./shared-buffer";
export { type WebSocketLimitsConfig, WebSocketManager } from "./ws-manager";

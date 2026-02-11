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
export {
	type DeltaPersistence,
	MemoryPersistence,
	SqlitePersistence,
} from "./persistence";
export { matchRoute, type RouteMatch } from "./router";
export { GatewayServer, type GatewayServerConfig } from "./server";
export { SharedBuffer } from "./shared-buffer";
export { WebSocketManager } from "./ws-manager";

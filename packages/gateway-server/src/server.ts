import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AdapterFactoryRegistry } from "@lakesync/adapter";
import { jiraPollerFactory } from "@lakesync/connector-jira";
import { salesforcePollerFactory } from "@lakesync/connector-salesforce";
import type {
	DatabaseAdapter,
	HLCTimestamp,
	LakeAdapter,
	PollerRegistry,
	RowDelta,
	SyncResponse,
} from "@lakesync/core";
import { bigintReplacer, createPollerRegistry, isDatabaseAdapter } from "@lakesync/core";
import type { ConfigStore, FlushQueue, HandlerResult } from "@lakesync/gateway";
import {
	DEFAULT_MAX_BUFFER_AGE_MS,
	DEFAULT_MAX_BUFFER_BYTES,
	handleActionRequest,
	handleFlushRequest,
	handleListConnectorTypes,
	handleMetrics,
	handlePullRequest,
	handlePushRequest,
	handleSaveSchema,
	handleSaveSyncRules,
	MAX_PUSH_PAYLOAD_BYTES,
	MemoryConfigStore,
	SyncGateway,
} from "@lakesync/gateway";
import { authenticateRequest } from "./auth-middleware";
import type { DistributedLock } from "./cluster";
import { ConnectorManager } from "./connector-manager";
import { corsHeaders, handlePreflight } from "./cors-middleware";
import { SourcePoller } from "./ingest/poller";
import type { IngestSourceConfig } from "./ingest/types";
import { Logger, type LogLevel } from "./logger";
import { MetricsRegistry } from "./metrics";
import type { Middleware, RequestContext, RouteHandler } from "./middleware";
import { runPipeline } from "./middleware";
import { type DeltaPersistence, MemoryPersistence, SqlitePersistence } from "./persistence";
import { RateLimiter, type RateLimiterConfig } from "./rate-limiter";
import { matchRoute } from "./router";
import { SharedBuffer, type SharedBufferConfig } from "./shared-buffer";
import { type WebSocketLimitsConfig, WebSocketManager } from "./ws-manager";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Configuration for the self-hosted gateway server. */
export interface GatewayServerConfig {
	/** Port to listen on (default 3000). */
	port?: number;
	/** Unique gateway identifier. */
	gatewayId: string;
	/** Storage adapter — LakeAdapter (S3/R2) or DatabaseAdapter (Postgres/MySQL). */
	adapter?: LakeAdapter | DatabaseAdapter;
	/** Maximum buffer size in bytes before triggering flush (default 4 MiB). */
	maxBufferBytes?: number;
	/** Maximum buffer age in milliseconds before triggering flush (default 30s). */
	maxBufferAgeMs?: number;
	/** HMAC-SHA256 secret for JWT verification. When omitted, auth is disabled. */
	jwtSecret?: string;
	/** Interval in milliseconds between periodic flushes (default 30s). */
	flushIntervalMs?: number;
	/** CORS allowed origins. When omitted, all origins are reflected. */
	allowedOrigins?: string[];
	/** DeltaBuffer persistence strategy (default "memory"). */
	persistence?: "memory" | "sqlite";
	/** Path to the SQLite file when `persistence` is "sqlite" (default "./lakesync-buffer.sqlite"). */
	sqlitePath?: string;
	/** Polling ingest sources. Each source is polled independently. */
	ingestSources?: IngestSourceConfig[];
	/** Optional clustering configuration for multi-instance deployment. */
	cluster?: {
		/** Distributed lock for coordinated flush. */
		lock: DistributedLock;
		/** Shared database adapter for cross-instance visibility. */
		sharedAdapter: DatabaseAdapter;
		/** Shared buffer configuration (consistency mode, etc.). */
		sharedBufferConfig?: SharedBufferConfig;
	};
	/** Custom poller registry for connector pollers. Defaults to Jira + Salesforce. */
	pollerRegistry?: PollerRegistry;
	/** Custom adapter factory registry. Defaults to Postgres, MySQL, BigQuery. */
	adapterRegistry?: AdapterFactoryRegistry;
	/** Drain timeout in milliseconds for graceful shutdown (default 10s). */
	drainTimeoutMs?: number;
	/** Request timeout in milliseconds (default 30s). Aborts with 504 on timeout. */
	requestTimeoutMs?: number;
	/** Flush timeout in milliseconds for periodic flushes (default 60s). */
	flushTimeoutMs?: number;
	/** Per-client rate limiter configuration. When provided, rate limiting is enabled. */
	rateLimiter?: RateLimiterConfig;
	/** Optional flush queue for post-flush materialisation (e.g. R2FlushQueue). */
	flushQueue?: FlushQueue;
	/** WebSocket connection and message rate limits. */
	wsLimits?: WebSocketLimitsConfig;
	/** Minimum log level for the structured logger (default "info"). */
	logLevel?: LogLevel;
	/** Whether to enable the Prometheus metrics endpoint at GET /metrics (default true). */
	enableMetrics?: boolean;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_PORT = 3000;
const DEFAULT_FLUSH_INTERVAL_MS = 30_000;
const DEFAULT_DRAIN_TIMEOUT_MS = 10_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_FLUSH_TIMEOUT_MS = 60_000;
const DEFAULT_ADAPTER_HEALTH_TIMEOUT_MS = 5_000;

// ---------------------------------------------------------------------------
// Node HTTP helpers
// ---------------------------------------------------------------------------

/** Read the full request body as a string. */
function readBody(req: IncomingMessage): Promise<string> {
	return new Promise((resolve, reject) => {
		const chunks: Buffer[] = [];
		req.on("data", (chunk: Buffer) => chunks.push(chunk));
		req.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
		req.on("error", reject);
	});
}

/** Send a JSON response. */
function sendJson(
	res: ServerResponse,
	body: unknown,
	status = 200,
	extraHeaders?: Record<string, string>,
): void {
	const json = JSON.stringify(body, bigintReplacer);
	res.writeHead(status, {
		"Content-Type": "application/json",
		...extraHeaders,
	});
	res.end(json);
}

/** Send a JSON error response. */
function sendError(
	res: ServerResponse,
	message: string,
	status: number,
	extraHeaders?: Record<string, string>,
): void {
	sendJson(res, { error: message }, status, extraHeaders);
}

/** Send a HandlerResult as HTTP response. */
function sendResult(
	res: ServerResponse,
	result: HandlerResult,
	corsH: Record<string, string>,
): void {
	sendJson(res, result.body, result.status, corsH);
}

// ---------------------------------------------------------------------------
// GatewayServer
// ---------------------------------------------------------------------------

/**
 * Self-hosted HTTP gateway server wrapping {@link SyncGateway}.
 *
 * Composes extracted modules: cors-middleware, auth-middleware, router,
 * ws-manager, and connector-manager. Request handling follows a
 * middleware pipeline: cors -> static routes -> drain -> timeout ->
 * route match -> auth -> rate limit -> dispatch.
 *
 * @example
 * ```ts
 * const server = new GatewayServer({
 *   gatewayId: "my-gateway",
 *   port: 3000,
 *   adapter: new PostgresAdapter({ connectionString: "..." }),
 *   jwtSecret: process.env.JWT_SECRET,
 * });
 * await server.start();
 * ```
 */
export class GatewayServer {
	private readonly gateway: SyncGateway;
	private readonly config: Required<
		Pick<GatewayServerConfig, "port" | "gatewayId" | "flushIntervalMs">
	> &
		GatewayServerConfig;
	private readonly configStore: ConfigStore;
	private readonly persistence: DeltaPersistence;
	private readonly connectors: ConnectorManager;
	private readonly sharedBuffer: SharedBuffer | null;
	private readonly rateLimiter: RateLimiter | null;
	private readonly logger: Logger;
	private readonly metrics: MetricsRegistry;
	private readonly pipeline: Middleware[];
	private readonly routeHandlers: Record<string, RouteHandler>;

	private httpServer: Server | null = null;
	private wsManager: WebSocketManager | null = null;
	private flushTimer: ReturnType<typeof setInterval> | null = null;
	private resolvedPort = 0;
	private pollers: SourcePoller[] = [];

	/** Whether the server is draining (rejecting new requests during shutdown). */
	private draining = false;
	/** Number of in-flight requests currently being handled. */
	private activeRequests = 0;
	/** Signal handler cleanup functions. */
	private signalCleanup: (() => void) | null = null;

	constructor(config: GatewayServerConfig) {
		this.config = {
			port: config.port ?? DEFAULT_PORT,
			flushIntervalMs: config.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS,
			...config,
		};

		this.gateway = new SyncGateway(
			{
				gatewayId: config.gatewayId,
				maxBufferBytes: config.maxBufferBytes ?? DEFAULT_MAX_BUFFER_BYTES,
				maxBufferAgeMs: config.maxBufferAgeMs ?? DEFAULT_MAX_BUFFER_AGE_MS,
				flushQueue: config.flushQueue,
			},
			config.adapter,
		);

		this.configStore = new MemoryConfigStore();

		this.persistence =
			config.persistence === "sqlite"
				? new SqlitePersistence(config.sqlitePath ?? "./lakesync-buffer.sqlite")
				: new MemoryPersistence();

		this.sharedBuffer = config.cluster
			? new SharedBuffer(config.cluster.sharedAdapter, config.cluster.sharedBufferConfig)
			: null;

		// Build poller registry — default includes Jira + Salesforce
		const pollerRegistry =
			config.pollerRegistry ??
			createPollerRegistry()
				.with("jira", jiraPollerFactory)
				.with("salesforce", salesforcePollerFactory);

		this.connectors = new ConnectorManager(this.configStore, this.gateway, {
			pollerRegistry,
			adapterRegistry: config.adapterRegistry,
			persistence: this.persistence,
		});

		this.rateLimiter = config.rateLimiter ? new RateLimiter(config.rateLimiter) : null;
		this.logger = new Logger(config.logLevel ?? "info");
		this.metrics = new MetricsRegistry();

		// Build route handler map and middleware pipeline
		this.routeHandlers = this.buildRouteHandlers();
		this.pipeline = this.buildPipeline();
	}

	/**
	 * Start the HTTP server and periodic flush timer.
	 *
	 * Rehydrates unflushed deltas from the persistence layer directly
	 * into the buffer (bypassing push validation) before accepting
	 * connections.
	 */
	async start(): Promise<void> {
		// Rehydrate unflushed deltas directly into the buffer
		const persisted = this.persistence.loadAll();
		if (persisted.length > 0) {
			this.gateway.rehydrate(persisted);
			this.persistence.clear();
		}

		this.httpServer = createServer((req, res) => {
			void this.handleRequest(req, res);
		});

		await new Promise<void>((resolve) => {
			this.httpServer!.listen(this.config.port, () => {
				const addr = this.httpServer!.address();
				if (addr && typeof addr === "object") {
					this.resolvedPort = addr.port;
				}
				resolve();
			});
		});

		// WebSocket manager
		this.wsManager = new WebSocketManager(
			this.gateway,
			this.configStore,
			this.config.gatewayId,
			this.config.jwtSecret,
			this.config.wsLimits,
		);
		this.wsManager.attach(this.httpServer);

		// Periodic flush
		this.flushTimer = setInterval(() => {
			void this.periodicFlush();
		}, this.config.flushIntervalMs);

		// Start ingest pollers
		if (this.config.ingestSources) {
			for (const source of this.config.ingestSources) {
				const poller = new SourcePoller(source, this.gateway);

				// Restore persisted cursor state
				const saved = this.persistence.loadCursor(source.name);
				if (saved) {
					poller.setCursorState(JSON.parse(saved));
				}
				poller.onCursorUpdate = (state) => {
					this.persistence.saveCursor(source.name, JSON.stringify(state));
				};

				poller.start();
				this.pollers.push(poller);
			}
		}

		// Signal handlers for graceful shutdown
		this.setupSignalHandlers();
	}

	/** Stop the server, pollers, connectors, and WebSocket connections. */
	async stop(): Promise<void> {
		// Remove signal handlers
		if (this.signalCleanup) {
			this.signalCleanup();
			this.signalCleanup = null;
		}

		// Stop dynamic connectors (pollers + adapters)
		await this.connectors.stopAll();

		// Stop ingest pollers
		for (const poller of this.pollers) {
			poller.stop();
		}
		this.pollers = [];

		if (this.flushTimer) {
			clearInterval(this.flushTimer);
			this.flushTimer = null;
		}

		// Close WebSocket connections
		if (this.wsManager) {
			this.wsManager.close();
			this.wsManager = null;
		}

		if (this.httpServer) {
			await new Promise<void>((resolve) => {
				this.httpServer!.close(() => resolve());
			});
			this.httpServer = null;
		}

		if (this.rateLimiter) {
			this.rateLimiter.dispose();
		}

		this.persistence.close();
	}

	/** Whether the server is currently draining connections. */
	get isDraining(): boolean {
		return this.draining;
	}

	/** The port the server is listening on (available after start). */
	get port(): number {
		return this.resolvedPort || this.config.port;
	}

	/** The underlying SyncGateway instance for direct access. */
	get gatewayInstance(): SyncGateway {
		return this.gateway;
	}

	/** The Prometheus metrics registry. */
	get metricsRegistry(): MetricsRegistry {
		return this.metrics;
	}

	// -----------------------------------------------------------------------
	// Middleware pipeline construction
	// -----------------------------------------------------------------------

	/** Build the middleware pipeline executed for every request. */
	private buildPipeline(): Middleware[] {
		return [
			this.corsPreflight.bind(this),
			this.staticRoutes.bind(this),
			this.drainGuard.bind(this),
			this.requestTimeout.bind(this),
			this.activeRequestTracking.bind(this),
			this.routeMatching.bind(this),
			this.authMiddleware.bind(this),
			this.rateLimitMiddleware.bind(this),
			this.routeDispatch.bind(this),
		];
	}

	// -----------------------------------------------------------------------
	// Middleware: CORS preflight
	// -----------------------------------------------------------------------

	private async corsPreflight(ctx: RequestContext, next: () => Promise<void>): Promise<void> {
		if (handlePreflight(ctx.method, ctx.res, ctx.corsHeaders)) return;
		await next();
	}

	// -----------------------------------------------------------------------
	// Middleware: Static routes (no auth, available during drain)
	// -----------------------------------------------------------------------

	private async staticRoutes(ctx: RequestContext, next: () => Promise<void>): Promise<void> {
		const { pathname, method, res, corsHeaders: corsH } = ctx;

		if (pathname === "/health" && method === "GET") {
			sendJson(res, { status: "ok" }, 200, corsH);
			return;
		}

		if (pathname === "/ready" && method === "GET") {
			await this.handleReady(res, corsH);
			return;
		}

		if (pathname === "/metrics" && method === "GET") {
			this.updateBufferGauges();
			const body = this.metrics.expose();
			res.writeHead(200, {
				"Content-Type": "text/plain; version=0.0.4; charset=utf-8",
				...corsH,
			});
			res.end(body);
			return;
		}

		if (pathname === "/connectors/types" && method === "GET") {
			const result = handleListConnectorTypes();
			sendResult(res, result, corsH);
			return;
		}

		await next();
	}

	// -----------------------------------------------------------------------
	// Middleware: Drain guard
	// -----------------------------------------------------------------------

	private async drainGuard(ctx: RequestContext, next: () => Promise<void>): Promise<void> {
		if (this.draining) {
			sendError(ctx.res, "Service is shutting down", 503, ctx.corsHeaders);
			return;
		}
		await next();
	}

	// -----------------------------------------------------------------------
	// Middleware: Request timeout
	// -----------------------------------------------------------------------

	private async requestTimeout(ctx: RequestContext, next: () => Promise<void>): Promise<void> {
		const timeoutMs = this.config.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
		ctx.res.setTimeout(timeoutMs, () => {
			if (!ctx.res.writableEnded) {
				sendError(ctx.res, "Request timeout", 504, ctx.corsHeaders);
			}
		});
		await next();
	}

	// -----------------------------------------------------------------------
	// Middleware: Active request tracking (for graceful shutdown)
	// -----------------------------------------------------------------------

	private async activeRequestTracking(
		_ctx: RequestContext,
		next: () => Promise<void>,
	): Promise<void> {
		this.activeRequests++;
		try {
			await next();
		} finally {
			this.activeRequests--;
		}
	}

	// -----------------------------------------------------------------------
	// Middleware: Route matching
	// -----------------------------------------------------------------------

	private async routeMatching(ctx: RequestContext, next: () => Promise<void>): Promise<void> {
		const route = matchRoute(ctx.pathname, ctx.method);
		if (!route) {
			sendError(ctx.res, "Not found", 404, ctx.corsHeaders);
			return;
		}

		if (route.gatewayId !== this.config.gatewayId) {
			sendError(ctx.res, "Gateway ID mismatch", 404, ctx.corsHeaders);
			return;
		}

		ctx.route = route;
		await next();
	}

	// -----------------------------------------------------------------------
	// Middleware: Authentication
	// -----------------------------------------------------------------------

	private async authMiddleware(ctx: RequestContext, next: () => Promise<void>): Promise<void> {
		const route = ctx.route!;
		const authResult = await authenticateRequest(
			ctx.req,
			route.gatewayId,
			route.action,
			this.config.jwtSecret,
		);
		if (!authResult.authenticated) {
			sendError(ctx.res, authResult.message, authResult.status, ctx.corsHeaders);
			return;
		}
		ctx.auth = this.config.jwtSecret ? authResult.claims : undefined;
		await next();
	}

	// -----------------------------------------------------------------------
	// Middleware: Rate limiting
	// -----------------------------------------------------------------------

	private async rateLimitMiddleware(ctx: RequestContext, next: () => Promise<void>): Promise<void> {
		if (this.rateLimiter) {
			const clientKey = ctx.auth?.clientId ?? ctx.req.socket.remoteAddress ?? "unknown";
			if (!this.rateLimiter.tryConsume(clientKey)) {
				const retryAfter = this.rateLimiter.retryAfterSeconds(clientKey);
				sendError(ctx.res, "Too many requests", 429, {
					...ctx.corsHeaders,
					"Retry-After": String(retryAfter),
				});
				return;
			}
		}
		await next();
	}

	// -----------------------------------------------------------------------
	// Middleware: Route dispatch (terminal — looks up handler from map)
	// -----------------------------------------------------------------------

	private async routeDispatch(ctx: RequestContext): Promise<void> {
		const handler = this.routeHandlers[ctx.route!.action];
		if (!handler) {
			sendError(ctx.res, "Not found", 404, ctx.corsHeaders);
			return;
		}
		await handler(ctx);
	}

	// -----------------------------------------------------------------------
	// Data-driven route handler map
	// -----------------------------------------------------------------------

	/** Build the action -> handler map for route dispatch. */
	private buildRouteHandlers(): Record<string, RouteHandler> {
		return {
			push: (ctx) => this.handlePush(ctx),
			pull: (ctx) => this.handlePull(ctx),
			action: (ctx) => this.handleAction(ctx),
			"describe-actions": (ctx) => this.handleDescribeActions(ctx),
			flush: (ctx) => this.handleFlush(ctx),
			schema: (ctx) => this.handleSaveSchemaRoute(ctx),
			"sync-rules": (ctx) => this.handleSaveSyncRulesRoute(ctx),
			"register-connector": (ctx) => this.handleRegisterConnectorRoute(ctx),
			"unregister-connector": (ctx) => this.handleUnregisterConnectorRoute(ctx),
			"list-connectors": (ctx) => this.handleListConnectorsRoute(ctx),
			metrics: (ctx) => this.handleMetricsRoute(ctx),
		};
	}

	// -----------------------------------------------------------------------
	// Request entry point — builds context and runs the pipeline
	// -----------------------------------------------------------------------

	private async handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
		const method = req.method ?? "GET";
		const rawUrl = req.url ?? "/";
		const url = new URL(rawUrl, `http://${req.headers.host ?? "localhost"}`);
		const pathname = url.pathname;
		const origin = req.headers.origin ?? null;
		const requestId = crypto.randomUUID();
		const reqLogger = this.logger.child({ requestId, method, path: pathname });

		// Track active requests metric
		this.metrics.activeRequests.inc();
		res.on("close", () => {
			this.metrics.activeRequests.dec();
		});

		const ctx: RequestContext = {
			req,
			res,
			method,
			url,
			pathname,
			requestId,
			logger: reqLogger,
			corsHeaders: corsHeaders(origin, { allowedOrigins: this.config.allowedOrigins }),
		};

		await runPipeline(this.pipeline, ctx);
	}

	// -----------------------------------------------------------------------
	// Route handlers — thin wrappers delegating to shared handlers or modules
	// -----------------------------------------------------------------------

	private async handlePush(ctx: RequestContext): Promise<void> {
		const { req, res, corsHeaders: corsH, auth, logger: reqLogger } = ctx;
		const start = performance.now();
		const contentLength = Number(req.headers["content-length"] ?? "0");
		if (contentLength > MAX_PUSH_PAYLOAD_BYTES) {
			this.metrics.pushTotal.inc({ status: "error" });
			sendError(res, "Payload too large (max 1 MiB)", 413, corsH);
			return;
		}

		const raw = await readBody(req);
		const result = handlePushRequest(this.gateway, raw, auth?.clientId, {
			persistBatch: (deltas) => this.persistence.appendBatch(deltas),
			clearPersistence: () => this.persistence.clear(),
			broadcastFn: (deltas, serverHlc, excludeClientId) =>
				this.wsManager?.broadcastDeltas(deltas, serverHlc, excludeClientId),
		});

		// Shared buffer write-through for cross-instance visibility
		if (result.status === 200 && this.sharedBuffer) {
			const pushResult = result.body as { deltas: RowDelta[]; serverHlc: HLCTimestamp };
			if (pushResult.deltas.length > 0) {
				const writeResult = await this.sharedBuffer.writeThroughPush(pushResult.deltas);
				if (!writeResult.ok) {
					this.metrics.pushTotal.inc({ status: "error" });
					sendError(res, writeResult.error.message, 502, corsH);
					return;
				}
			}
		}

		const status = result.status === 200 ? "ok" : "error";
		const durationMs = Math.round(performance.now() - start);
		this.metrics.pushTotal.inc({ status });
		this.metrics.pushLatency.observe({}, performance.now() - start);
		this.updateBufferGauges();
		reqLogger.info("push completed", { status: result.status, durationMs });

		sendResult(res, result, corsH);
	}

	private async handlePull(ctx: RequestContext): Promise<void> {
		const { url, res, corsHeaders: corsH, auth, logger: reqLogger } = ctx;
		const syncRules = await this.configStore.getSyncRules(this.config.gatewayId);
		const result = await handlePullRequest(
			this.gateway,
			{
				since: url.searchParams.get("since"),
				clientId: url.searchParams.get("clientId"),
				limit: url.searchParams.get("limit"),
				source: url.searchParams.get("source"),
			},
			auth?.customClaims,
			syncRules,
		);

		// Merge with shared buffer for cross-instance visibility
		let body = result.body;
		if (result.status === 200 && this.sharedBuffer) {
			const sinceParam = url.searchParams.get("since");
			if (sinceParam) {
				try {
					const sinceHlc = BigInt(sinceParam) as HLCTimestamp;
					body = await this.sharedBuffer.mergePull(body as SyncResponse, sinceHlc);
				} catch {
					// If since parsing fails, pull handler already returned an error
				}
			}
		}

		const pullStatus = result.status === 200 ? "ok" : "error";
		this.metrics.pullTotal.inc({ status: pullStatus });
		reqLogger.info("pull completed", { status: result.status });

		sendJson(res, body, result.status, corsH);
	}

	private async handleAction(ctx: RequestContext): Promise<void> {
		const { req, res, corsHeaders: corsH, auth } = ctx;
		const raw = await readBody(req);
		const result = await handleActionRequest(this.gateway, raw, auth?.clientId, auth?.customClaims);
		sendResult(res, result, corsH);
	}

	private handleDescribeActions(ctx: RequestContext): void {
		sendJson(ctx.res, this.gateway.describeActions(), 200, ctx.corsHeaders);
	}

	private async handleFlush(ctx: RequestContext): Promise<void> {
		const { res, corsHeaders: corsH, logger: reqLogger } = ctx;
		const start = performance.now();
		const result = await handleFlushRequest(this.gateway, {
			clearPersistence: () => this.persistence.clear(),
		});
		const durationMs = Math.round(performance.now() - start);
		const status = result.status === 200 ? "ok" : "error";
		this.metrics.flushTotal.inc({ status });
		this.metrics.flushDuration.observe({}, performance.now() - start);
		this.updateBufferGauges();
		reqLogger.info("flush completed", { status: result.status, durationMs });
		sendResult(res, result, corsH);
	}

	private async handleSaveSchemaRoute(ctx: RequestContext): Promise<void> {
		const raw = await readBody(ctx.req);
		const result = await handleSaveSchema(raw, this.configStore, this.config.gatewayId);
		sendResult(ctx.res, result, ctx.corsHeaders);
	}

	private async handleSaveSyncRulesRoute(ctx: RequestContext): Promise<void> {
		const raw = await readBody(ctx.req);
		const result = await handleSaveSyncRules(raw, this.configStore, this.config.gatewayId);
		sendResult(ctx.res, result, ctx.corsHeaders);
	}

	// -----------------------------------------------------------------------
	// Connector management — delegates to ConnectorManager
	// -----------------------------------------------------------------------

	private async handleRegisterConnectorRoute(ctx: RequestContext): Promise<void> {
		const raw = await readBody(ctx.req);
		const result = await this.connectors.register(raw);
		sendResult(ctx.res, result, ctx.corsHeaders);
	}

	private async handleUnregisterConnectorRoute(ctx: RequestContext): Promise<void> {
		const result = await this.connectors.unregister(ctx.route!.connectorName!);
		sendResult(ctx.res, result, ctx.corsHeaders);
	}

	private async handleListConnectorsRoute(ctx: RequestContext): Promise<void> {
		const result = await this.connectors.list();
		sendJson(ctx.res, result.body, result.status, ctx.corsHeaders);
	}

	private handleMetricsRoute(ctx: RequestContext): void {
		const result = handleMetrics(this.gateway, { process: process.memoryUsage() });
		sendResult(ctx.res, result, ctx.corsHeaders);
	}

	// -----------------------------------------------------------------------
	// Buffer gauge helpers
	// -----------------------------------------------------------------------

	/** Synchronise buffer gauge metrics with the current buffer state. */
	private updateBufferGauges(): void {
		const stats = this.gateway.bufferStats;
		this.metrics.bufferBytes.set({}, stats.byteSize);
		this.metrics.bufferDeltas.set({}, stats.logSize);
	}

	// -----------------------------------------------------------------------
	// Readiness probe
	// -----------------------------------------------------------------------

	/** Handle GET /ready — checks draining status and adapter health. */
	private async handleReady(res: ServerResponse, corsH: Record<string, string>): Promise<void> {
		if (this.draining) {
			sendJson(res, { status: "not_ready", reason: "draining" }, 503, corsH);
			return;
		}

		const adapterHealthy = await this.checkAdapterHealth();
		if (!adapterHealthy) {
			sendJson(res, { status: "not_ready", reason: "adapter unreachable" }, 503, corsH);
			return;
		}

		sendJson(res, { status: "ready" }, 200, corsH);
	}

	/**
	 * Check whether the configured adapter is reachable.
	 *
	 * For a DatabaseAdapter, attempts a lightweight query with a timeout.
	 * For a LakeAdapter, attempts a headObject call (404 still means reachable).
	 * Returns true when no adapter is configured (stateless mode).
	 */
	private async checkAdapterHealth(): Promise<boolean> {
		const adapter = this.config.adapter;
		if (!adapter) return true;

		const timeoutMs = DEFAULT_ADAPTER_HEALTH_TIMEOUT_MS;
		const timeoutPromise = new Promise<false>((resolve) => {
			setTimeout(() => resolve(false), timeoutMs);
		});

		try {
			if (isDatabaseAdapter(adapter)) {
				const healthCheck = adapter
					.queryDeltasSince(0n as HLCTimestamp, [])
					.then((result) => result.ok);
				return await Promise.race([healthCheck, timeoutPromise]);
			}
			// LakeAdapter — try headObject on a known key
			const healthCheck = (adapter as LakeAdapter)
				.headObject("__health__")
				.then(() => true)
				.catch(() => true); // S3 404 is still "reachable"
			return await Promise.race([healthCheck, timeoutPromise]);
		} catch {
			return false;
		}
	}

	// -----------------------------------------------------------------------
	// Graceful shutdown — signal handlers
	// -----------------------------------------------------------------------

	/** Register SIGTERM/SIGINT handlers for graceful shutdown. */
	private setupSignalHandlers(): void {
		const shutdown = () => {
			void this.gracefulShutdown();
		};
		process.on("SIGTERM", shutdown);
		process.on("SIGINT", shutdown);
		this.signalCleanup = () => {
			process.off("SIGTERM", shutdown);
			process.off("SIGINT", shutdown);
		};
	}

	/** Graceful shutdown: stop accepting, drain, flush, exit. */
	private async gracefulShutdown(): Promise<void> {
		if (this.draining) return;
		this.draining = true;

		this.logger.info("Graceful shutdown initiated, draining requests...");

		// Stop accepting new connections
		if (this.httpServer) {
			this.httpServer.close();
		}

		// Wait for active requests to drain (up to drainTimeoutMs)
		const drainTimeout = this.config.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
		const start = Date.now();
		while (this.activeRequests > 0 && Date.now() - start < drainTimeout) {
			await new Promise((resolve) => setTimeout(resolve, 100));
		}

		// Final flush
		try {
			await this.gateway.flush();
		} catch {
			// Best-effort flush
		}

		await this.stop();
		process.exit(0);
	}

	// -----------------------------------------------------------------------
	// Periodic flush
	// -----------------------------------------------------------------------

	private async periodicFlush(): Promise<void> {
		if (this.gateway.bufferStats.logSize === 0) {
			return;
		}

		const lock = this.config.cluster?.lock;
		const lockKey = `flush:${this.config.gatewayId}`;
		if (lock) {
			const acquired = await lock.acquire(lockKey, 30_000);
			if (!acquired) {
				return;
			}
		}

		const flushTimeoutMs = this.config.flushTimeoutMs ?? DEFAULT_FLUSH_TIMEOUT_MS;
		const start = performance.now();
		try {
			const flushPromise = this.gateway.flush();
			const timeoutPromise = new Promise<{ ok: false; timedOut: true }>((resolve) => {
				setTimeout(() => resolve({ ok: false, timedOut: true }), flushTimeoutMs);
			});

			const result = await Promise.race([flushPromise, timeoutPromise]);

			if ("timedOut" in result) {
				this.logger.warn(`Periodic flush timed out after ${flushTimeoutMs}ms`);
				this.metrics.flushTotal.inc({ status: "error" });
				return;
			}

			if (result.ok) {
				this.persistence.clear();
				this.metrics.flushTotal.inc({ status: "ok" });
			} else {
				this.metrics.flushTotal.inc({ status: "error" });
			}
			this.metrics.flushDuration.observe({}, performance.now() - start);
			this.updateBufferGauges();
		} catch (err) {
			this.metrics.flushTotal.inc({ status: "error" });
			this.logger.error("periodic flush failed", {
				error: err instanceof Error ? err.message : String(err),
			});
		} finally {
			if (lock) {
				await lock.release(lockKey);
			}
		}
	}
}

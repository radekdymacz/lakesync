import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AdapterFactoryRegistry } from "@lakesync/adapter";
import { jiraPollerFactory } from "@lakesync/connector-jira";
import { salesforcePollerFactory } from "@lakesync/connector-salesforce";
import type {
	ConnectorFactoryRegistry,
	DatabaseAdapter,
	LakeAdapter,
	PollerRegistry,
	UsageRecorder,
} from "@lakesync/core";
import { createPollerRegistry } from "@lakesync/core";
import type { ConfigStore, FlushQueue } from "@lakesync/gateway";
import {
	DEFAULT_MAX_BUFFER_AGE_MS,
	DEFAULT_MAX_BUFFER_BYTES,
	MemoryConfigStore,
	SyncGateway,
} from "@lakesync/gateway";
import type { DistributedLock } from "./cluster";
import { ConnectorManager } from "./connector-manager";
import { corsHeaders } from "./cors-middleware";
import { SourcePoller } from "./ingest/poller";
import type { IngestSourceConfig } from "./ingest/types";
import { Logger, type LogLevel } from "./logger";
import { MetricsRegistry } from "./metrics";
import type { Middleware, RequestContext } from "./middleware";
import { runPipeline } from "./middleware";
import { type DeltaPersistence, MemoryPersistence, SqlitePersistence } from "./persistence";
import { buildServerPipeline, type PipelineState } from "./pipeline";
import { RateLimiter, type RateLimiterConfig } from "./rate-limiter";
import { buildServerRouteHandlers } from "./route-handlers";
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
	/** Previous JWT secret for zero-downtime rotation. When set alongside `jwtSecret`, tokens signed with either secret are accepted. */
	jwtSecretPrevious?: string;
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
	/** Unified connector factory registry. Takes precedence over `pollerRegistry` + `adapterRegistry`. */
	connectorRegistry?: ConnectorFactoryRegistry;
	/** @deprecated Use `connectorRegistry` instead. Custom poller registry for connector pollers. Defaults to Jira + Salesforce. */
	pollerRegistry?: PollerRegistry;
	/** @deprecated Use `connectorRegistry` instead. Custom adapter factory registry. Defaults to Postgres, MySQL, BigQuery. */
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
	/** Optional usage recorder for metering billable events. Passed to the gateway and WebSocket manager. */
	usageRecorder?: UsageRecorder;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_PORT = 3000;
const DEFAULT_FLUSH_INTERVAL_MS = 30_000;
const DEFAULT_DRAIN_TIMEOUT_MS = 10_000;
const DEFAULT_FLUSH_TIMEOUT_MS = 60_000;

// ---------------------------------------------------------------------------
// Server components — extracted from GatewayServer constructor
// ---------------------------------------------------------------------------

/** All independent components built from a {@link GatewayServerConfig}. */
export interface ServerComponents {
	gateway: SyncGateway;
	configStore: ConfigStore;
	persistence: DeltaPersistence;
	connectors: ConnectorManager;
	sharedBuffer: SharedBuffer | null;
	rateLimiter: RateLimiter | null;
	logger: Logger;
	metrics: MetricsRegistry;
	resolvedConfig: Required<Pick<GatewayServerConfig, "port" | "gatewayId" | "flushIntervalMs">> &
		GatewayServerConfig;
}

/**
 * Build all independent server components from configuration.
 *
 * This is a pure factory — no side effects, no server started.
 * Useful for testing or custom composition.
 */
export function buildServerComponents(config: GatewayServerConfig): ServerComponents {
	const resolvedConfig = {
		port: config.port ?? DEFAULT_PORT,
		flushIntervalMs: config.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS,
		...config,
	};

	const gateway = new SyncGateway(
		{
			gatewayId: config.gatewayId,
			maxBufferBytes: config.maxBufferBytes ?? DEFAULT_MAX_BUFFER_BYTES,
			maxBufferAgeMs: config.maxBufferAgeMs ?? DEFAULT_MAX_BUFFER_AGE_MS,
			flushQueue: config.flushQueue,
		},
		config.adapter,
	);

	const configStore = new MemoryConfigStore();

	const persistence =
		config.persistence === "sqlite"
			? new SqlitePersistence(config.sqlitePath ?? "./lakesync-buffer.sqlite")
			: new MemoryPersistence();

	const sharedBuffer = config.cluster
		? new SharedBuffer(config.cluster.sharedAdapter, config.cluster.sharedBufferConfig)
		: null;

	// Build connector manager — unified registry takes precedence over legacy separate registries
	const pollerRegistry =
		config.pollerRegistry ??
		createPollerRegistry()
			.with("jira", jiraPollerFactory)
			.with("salesforce", salesforcePollerFactory);

	const connectors = new ConnectorManager(configStore, gateway, {
		connectorRegistry: config.connectorRegistry,
		pollerRegistry,
		adapterRegistry: config.adapterRegistry,
		persistence,
	});

	const rateLimiter = config.rateLimiter ? new RateLimiter(config.rateLimiter) : null;
	const logger = new Logger(config.logLevel ?? "info");
	const metrics = new MetricsRegistry();

	return {
		gateway,
		configStore,
		persistence,
		connectors,
		sharedBuffer,
		rateLimiter,
		logger,
		metrics,
		resolvedConfig,
	};
}

/**
 * Create and start a {@link GatewayServer} in one call.
 *
 * Convenience factory that builds components, creates the server,
 * and starts it. Returns the running server instance.
 *
 * @example
 * ```ts
 * const server = await startServer({
 *   gatewayId: "my-gateway",
 *   adapter: new PostgresAdapter({ connectionString: "..." }),
 *   jwtSecret: process.env.JWT_SECRET,
 * });
 * // server is now accepting connections
 * await server.stop();
 * ```
 */
export async function startServer(config: GatewayServerConfig): Promise<GatewayServer> {
	const server = new GatewayServer(config);
	await server.start();
	return server;
}

// ---------------------------------------------------------------------------
// GatewayServer
// ---------------------------------------------------------------------------

/**
 * Self-hosted HTTP gateway server wrapping {@link SyncGateway}.
 *
 * Composes extracted modules: cors-middleware, auth-middleware, router,
 * ws-manager, and connector-manager. Request handling follows a
 * middleware pipeline built by {@link buildServerPipeline}: cors ->
 * static routes -> drain -> timeout -> route match -> auth -> rate
 * limit -> dispatch.
 *
 * Construction delegates to {@link buildServerComponents} for wiring.
 * The class is a thin lifecycle shell — start/stop/flush.
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
	private readonly pipelineState: PipelineState;

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
		const components = buildServerComponents(config);
		this.config = components.resolvedConfig;
		this.gateway = components.gateway;
		this.configStore = components.configStore;
		this.persistence = components.persistence;
		this.connectors = components.connectors;
		this.sharedBuffer = components.sharedBuffer;
		this.rateLimiter = components.rateLimiter;
		this.logger = components.logger;
		this.metrics = components.metrics;

		// Pipeline state — shared mutable object bridging the class fields
		// to the standalone pipeline functions. Reads/writes are proxied
		// so the pipeline always sees the current draining/activeRequests.
		this.pipelineState = {
			get draining() {
				return self.draining;
			},
			set draining(v: boolean) {
				self.draining = v;
			},
			get activeRequests() {
				return self.activeRequests;
			},
			set activeRequests(v: number) {
				self.activeRequests = v;
			},
		};
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const self = this;

		const updateBufferGauges = () => this.updateBufferGauges();

		// Build route handler map and middleware pipeline
		const routeHandlers = buildServerRouteHandlers({
			gateway: this.gateway,
			configStore: this.configStore,
			persistence: this.persistence,
			connectors: this.connectors,
			metrics: this.metrics,
			sharedBuffer: this.sharedBuffer,
			gatewayId: this.config.gatewayId,
			getWsManager: () => this.wsManager,
			updateBufferGauges,
		});

		// Build the resolved JWT secret — tuple when previous secret is configured
		const resolvedJwtSecret: string | [string, string] | undefined =
			config.jwtSecret && config.jwtSecretPrevious
				? [config.jwtSecret, config.jwtSecretPrevious]
				: config.jwtSecret;

		this.pipeline = buildServerPipeline(
			{
				allowedOrigins: config.allowedOrigins,
				jwtSecret: resolvedJwtSecret,
				requestTimeoutMs: config.requestTimeoutMs,
				gatewayId: this.config.gatewayId,
				rateLimiter: this.rateLimiter,
				adapter: config.adapter,
			},
			this.pipelineState,
			routeHandlers,
			this.metrics,
			updateBufferGauges,
		);
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

		// WebSocket manager — use resolved secret for dual-key rotation support
		const wsJwtSecret: string | [string, string] | undefined =
			this.config.jwtSecret && this.config.jwtSecretPrevious
				? [this.config.jwtSecret, this.config.jwtSecretPrevious]
				: this.config.jwtSecret;
		this.wsManager = new WebSocketManager(
			this.gateway,
			this.configStore,
			this.config.gatewayId,
			wsJwtSecret,
			this.config.wsLimits,
			this.config.usageRecorder,
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
	// Request entry point — builds context and runs the pipeline
	// -----------------------------------------------------------------------

	private async handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
		const method = req.method ?? "GET";
		const rawUrl = req.url ?? "/";
		const url = new URL(rawUrl, `http://${req.headers.host ?? "localhost"}`);
		const pathname = url.pathname;
		const origin = req.headers.origin ?? null;
		// Accept incoming X-Request-Id (pass-through from load balancer) or generate a new one
		const incomingRequestId = req.headers["x-request-id"];
		const requestId =
			(typeof incomingRequestId === "string" ? incomingRequestId : undefined) ??
			crypto.randomUUID();
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
			corsHeaders: {
				...corsHeaders(origin, { allowedOrigins: this.config.allowedOrigins }),
				"X-Request-Id": requestId,
			},
		};

		await runPipeline(this.pipeline, ctx);
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

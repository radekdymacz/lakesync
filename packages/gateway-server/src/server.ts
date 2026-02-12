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
import { bigintReplacer, createPollerRegistry } from "@lakesync/core";
import type { ConfigStore, HandlerResult } from "@lakesync/gateway";
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
import type { AuthClaims } from "./auth";
import { authenticateRequest } from "./auth-middleware";
import type { DistributedLock } from "./cluster";
import { ConnectorManager } from "./connector-manager";
import { corsHeaders, handlePreflight } from "./cors-middleware";
import { SourcePoller } from "./ingest/poller";
import type { IngestSourceConfig } from "./ingest/types";
import { type DeltaPersistence, MemoryPersistence, SqlitePersistence } from "./persistence";
import { matchRoute } from "./router";
import { SharedBuffer, type SharedBufferConfig } from "./shared-buffer";
import { WebSocketManager } from "./ws-manager";

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
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_PORT = 3000;
const DEFAULT_FLUSH_INTERVAL_MS = 30_000;

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
 * ws-manager, and connector-manager. Request handling follows the
 * pipeline: cors -> auth -> route -> handler.
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

	private httpServer: Server | null = null;
	private wsManager: WebSocketManager | null = null;
	private flushTimer: ReturnType<typeof setInterval> | null = null;
	private resolvedPort = 0;
	private pollers: SourcePoller[] = [];

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
		});
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
				poller.start();
				this.pollers.push(poller);
			}
		}
	}

	/** Stop the server, pollers, connectors, and WebSocket connections. */
	async stop(): Promise<void> {
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

		this.persistence.close();
	}

	/** The port the server is listening on (available after start). */
	get port(): number {
		return this.resolvedPort || this.config.port;
	}

	/** The underlying SyncGateway instance for direct access. */
	get gatewayInstance(): SyncGateway {
		return this.gateway;
	}

	// -----------------------------------------------------------------------
	// Request handling — cors -> auth -> route -> handler
	// -----------------------------------------------------------------------

	private async handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
		const method = req.method ?? "GET";
		const rawUrl = req.url ?? "/";
		const url = new URL(rawUrl, `http://${req.headers.host ?? "localhost"}`);
		const pathname = url.pathname;
		const origin = req.headers.origin ?? null;

		// Step 1: CORS headers
		const corsH = corsHeaders(origin, { allowedOrigins: this.config.allowedOrigins });

		// Step 2: CORS preflight
		if (handlePreflight(method, res, corsH)) return;

		// Step 3: Static routes (no auth)
		if (pathname === "/health" && method === "GET") {
			sendJson(res, { status: "ok" }, 200, corsH);
			return;
		}

		if (pathname === "/connectors/types" && method === "GET") {
			const result = handleListConnectorTypes();
			sendResult(res, result, corsH);
			return;
		}

		// Step 4: Route matching
		const route = matchRoute(pathname, method);
		if (!route) {
			sendError(res, "Not found", 404, corsH);
			return;
		}

		if (route.gatewayId !== this.config.gatewayId) {
			sendError(res, "Gateway ID mismatch", 404, corsH);
			return;
		}

		// Step 5: Authentication
		const authResult = await authenticateRequest(
			req,
			route.gatewayId,
			route.action,
			this.config.jwtSecret,
		);
		if (!authResult.authenticated) {
			sendError(res, authResult.message, authResult.status, corsH);
			return;
		}
		const auth: AuthClaims | undefined = this.config.jwtSecret ? authResult.claims : undefined;

		// Step 6: Route dispatch
		switch (route.action) {
			case "push":
				await this.handlePush(req, res, corsH, auth);
				break;
			case "pull":
				await this.handlePull(url, res, corsH, auth);
				break;
			case "action":
				await this.handleAction(req, res, corsH, auth);
				break;
			case "describe-actions":
				this.handleDescribeActions(res, corsH);
				break;
			case "flush":
				await this.handleFlush(res, corsH);
				break;
			case "schema":
				await this.handleSaveSchemaRoute(req, res, corsH);
				break;
			case "sync-rules":
				await this.handleSaveSyncRulesRoute(req, res, corsH);
				break;
			case "register-connector":
				await this.handleRegisterConnectorRoute(req, res, corsH);
				break;
			case "unregister-connector":
				await this.handleUnregisterConnectorRoute(route.connectorName!, res, corsH);
				break;
			case "list-connectors":
				await this.handleListConnectorsRoute(res, corsH);
				break;
			case "metrics":
				this.handleMetricsRoute(res, corsH);
				break;
			default:
				sendError(res, "Not found", 404, corsH);
		}
	}

	// -----------------------------------------------------------------------
	// Route handlers — thin wrappers delegating to shared handlers or modules
	// -----------------------------------------------------------------------

	private async handlePush(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
		auth?: AuthClaims,
	): Promise<void> {
		const contentLength = Number(req.headers["content-length"] ?? "0");
		if (contentLength > MAX_PUSH_PAYLOAD_BYTES) {
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
					sendError(res, writeResult.error.message, 502, corsH);
					return;
				}
			}
		}

		sendResult(res, result, corsH);
	}

	private async handlePull(
		url: URL,
		res: ServerResponse,
		corsH: Record<string, string>,
		auth?: AuthClaims,
	): Promise<void> {
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

		sendJson(res, body, result.status, corsH);
	}

	private async handleAction(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
		auth?: AuthClaims,
	): Promise<void> {
		const raw = await readBody(req);
		const result = await handleActionRequest(this.gateway, raw, auth?.clientId, auth?.customClaims);
		sendResult(res, result, corsH);
	}

	private handleDescribeActions(res: ServerResponse, corsH: Record<string, string>): void {
		sendJson(res, this.gateway.describeActions(), 200, corsH);
	}

	private async handleFlush(res: ServerResponse, corsH: Record<string, string>): Promise<void> {
		const result = await handleFlushRequest(this.gateway, {
			clearPersistence: () => this.persistence.clear(),
		});
		sendResult(res, result, corsH);
	}

	private async handleSaveSchemaRoute(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		const raw = await readBody(req);
		const result = await handleSaveSchema(raw, this.configStore, this.config.gatewayId);
		sendResult(res, result, corsH);
	}

	private async handleSaveSyncRulesRoute(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		const raw = await readBody(req);
		const result = await handleSaveSyncRules(raw, this.configStore, this.config.gatewayId);
		sendResult(res, result, corsH);
	}

	// -----------------------------------------------------------------------
	// Connector management — delegates to ConnectorManager
	// -----------------------------------------------------------------------

	private async handleRegisterConnectorRoute(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		const raw = await readBody(req);
		const result = await this.connectors.register(raw);
		sendResult(res, result, corsH);
	}

	private async handleUnregisterConnectorRoute(
		name: string,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		const result = await this.connectors.unregister(name);
		sendResult(res, result, corsH);
	}

	private async handleListConnectorsRoute(
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		const result = await this.connectors.list();
		sendJson(res, result.body, result.status, corsH);
	}

	private handleMetricsRoute(res: ServerResponse, corsH: Record<string, string>): void {
		const result = handleMetrics(this.gateway, { process: process.memoryUsage() });
		sendResult(res, result, corsH);
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

		try {
			const result = await this.gateway.flush();
			if (result.ok) {
				this.persistence.clear();
			}
		} finally {
			if (lock) {
				await lock.release(lockKey);
			}
		}
	}
}

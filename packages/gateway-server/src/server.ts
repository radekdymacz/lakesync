import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import {
	createDatabaseAdapter,
	createQueryFn,
	type DatabaseAdapter,
	type LakeAdapter,
} from "@lakesync/adapter";
import type {
	HLCTimestamp,
	ResolvedClaims,
	RowDelta,
	SyncResponse,
	SyncRulesContext,
} from "@lakesync/core";
import { bigintReplacer, filterDeltas, isActionHandler } from "@lakesync/core";
import type { ConfigStore } from "@lakesync/gateway";
import {
	DEFAULT_MAX_BUFFER_AGE_MS,
	DEFAULT_MAX_BUFFER_BYTES,
	type HandlerResult,
	handleActionRequest,
	handleFlushRequest,
	handleListConnectors,
	handleMetrics,
	handlePullRequest,
	handlePushRequest,
	handleRegisterConnector,
	handleSaveSchema,
	handleSaveSyncRules,
	handleUnregisterConnector,
	MAX_PUSH_PAYLOAD_BYTES,
	MemoryConfigStore,
	SyncGateway,
	type SyncPush,
} from "@lakesync/gateway";
import {
	decodeSyncPull,
	decodeSyncPush,
	encodeBroadcastFrame,
	encodeSyncResponse,
	TAG_SYNC_PULL,
	TAG_SYNC_PUSH,
} from "@lakesync/proto";
import { WebSocketServer, type WebSocket as WsWebSocket } from "ws";
import { type AuthClaims, verifyToken } from "./auth";
import type { DistributedLock } from "./cluster";
import { SourcePoller } from "./ingest/poller";
import type { IngestSourceConfig } from "./ingest/types";
import { type DeltaPersistence, MemoryPersistence, SqlitePersistence } from "./persistence";
import { SharedBuffer } from "./shared-buffer";

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
	};
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_PORT = 3000;
const DEFAULT_FLUSH_INTERVAL_MS = 30_000;
const ADMIN_ACTIONS = new Set([
	"flush",
	"schema",
	"sync-rules",
	"register-connector",
	"unregister-connector",
	"list-connectors",
	"metrics",
]);

// ---------------------------------------------------------------------------
// Route matching
// ---------------------------------------------------------------------------

interface RouteMatch {
	gatewayId: string;
	action: string;
	/** Extra route parameters (e.g. connector name from DELETE path). */
	connectorName?: string;
}

/** Route definitions: [method, pattern, action, captureConnectorName?] */
const ROUTES: Array<[string, RegExp, string, boolean?]> = [
	["POST", /^\/sync\/([^/]+)\/push$/, "push"],
	["GET", /^\/sync\/([^/]+)\/pull$/, "pull"],
	["POST", /^\/sync\/([^/]+)\/action$/, "action"],
	["GET", /^\/sync\/([^/]+)\/actions$/, "describe-actions"],
	["GET", /^\/sync\/([^/]+)\/ws$/, "ws"],
	["POST", /^\/admin\/flush\/([^/]+)$/, "flush"],
	["POST", /^\/admin\/schema\/([^/]+)$/, "schema"],
	["POST", /^\/admin\/sync-rules\/([^/]+)$/, "sync-rules"],
	["POST", /^\/admin\/connectors\/([^/]+)$/, "register-connector"],
	["GET", /^\/admin\/connectors\/([^/]+)$/, "list-connectors"],
	["DELETE", /^\/admin\/connectors\/([^/]+)\/([^/]+)$/, "unregister-connector", true],
	["GET", /^\/admin\/metrics\/([^/]+)$/, "metrics"],
];

function matchRoute(pathname: string, method: string): RouteMatch | null {
	for (const [m, pattern, action, hasConnector] of ROUTES) {
		if (method !== m) continue;
		const match = pathname.match(pattern);
		if (!match) continue;
		return {
			gatewayId: match[1]!,
			action,
			...(hasConnector ? { connectorName: match[2]! } : {}),
		};
	}
	return null;
}

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
// Poller interface — shared lifecycle contract for all pollers
// ---------------------------------------------------------------------------

/** Common lifecycle interface for source pollers (SQL or API-based). */
interface Poller {
	start(): void;
	stop(): void;
	readonly isRunning: boolean;
}

// ---------------------------------------------------------------------------
// GatewayServer
// ---------------------------------------------------------------------------

/**
 * Self-hosted HTTP gateway server wrapping {@link SyncGateway}.
 *
 * Provides the same route surface as the Cloudflare Workers gateway-worker
 * but runs as a standalone Node/Bun HTTP server. Supports optional JWT
 * authentication, CORS, periodic flush, and SQLite-based buffer persistence.
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
	private gateway: SyncGateway;
	private config: Required<Pick<GatewayServerConfig, "port" | "gatewayId" | "flushIntervalMs">> &
		GatewayServerConfig;
	private httpServer: Server | null = null;
	private flushTimer: ReturnType<typeof setInterval> | null = null;
	private configStore: ConfigStore;
	private persistence: DeltaPersistence;
	private resolvedPort = 0;
	private wss: WebSocketServer | null = null;
	private wsClients = new Map<WsWebSocket, { clientId: string; claims: Record<string, unknown> }>();
	private pollers: SourcePoller[] = [];
	private connectorAdapters = new Map<string, DatabaseAdapter>();
	private connectorPollers = new Map<string, Poller>();
	private sharedBuffer: SharedBuffer | null = null;

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

		if (config.cluster) {
			this.sharedBuffer = new SharedBuffer(config.cluster.sharedAdapter);
		}
	}

	/**
	 * Start the HTTP server and periodic flush timer.
	 *
	 * Rehydrates unflushed deltas from the persistence layer before
	 * accepting connections.
	 */
	async start(): Promise<void> {
		// Rehydrate unflushed deltas from persistence
		const persisted = this.persistence.loadAll();
		if (persisted.length > 0) {
			const push: SyncPush = {
				clientId: "__rehydrate__",
				deltas: persisted,
				lastSeenHlc: 0n as HLCTimestamp,
			};
			this.gateway.handlePush(push);
			this.persistence.clear();
		}

		this.httpServer = createServer((req, res) => {
			this.handleRequest(req, res);
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

		// WebSocket server (noServer mode — upgrade handled manually)
		this.wss = new WebSocketServer({ noServer: true });

		this.httpServer.on("upgrade", (req, socket, head) => {
			void this.handleUpgrade(req, socket, head);
		});

		// Periodic flush
		this.flushTimer = setInterval(() => {
			this.periodicFlush();
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

	/**
	 * Handle HTTP -> WebSocket upgrade.
	 *
	 * Authenticates via Bearer token in Authorization header or `?token=` query param.
	 */
	private async handleUpgrade(
		req: IncomingMessage,
		socket: import("node:stream").Duplex,
		head: Buffer,
	): Promise<void> {
		const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);

		// Authenticate
		let token = extractBearerToken(req);
		if (!token) {
			token = url.searchParams.get("token");
		}

		if (!token && this.config.jwtSecret) {
			socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
			socket.destroy();
			return;
		}

		let auth: AuthClaims | undefined;
		if (this.config.jwtSecret && token) {
			const authResult = await verifyToken(token, this.config.jwtSecret);
			if (!authResult.ok) {
				socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
				socket.destroy();
				return;
			}
			auth = authResult.value;

			// Verify gateway ID
			const gwMatch = url.pathname.match(/^\/sync\/([^/]+)\/ws$/);
			if (!gwMatch || gwMatch[1] !== auth.gatewayId) {
				socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
				socket.destroy();
				return;
			}
		}

		this.wss!.handleUpgrade(req, socket, head, (ws) => {
			const clientId = auth?.clientId ?? `anon-${crypto.randomUUID()}`;
			const claims: Record<string, unknown> = auth?.customClaims ?? {};

			this.wsClients.set(ws, { clientId, claims });

			ws.on("message", (data: Buffer) => {
				void this.handleWsMessage(ws, data, clientId, claims);
			});

			ws.on("close", () => {
				this.wsClients.delete(ws);
			});

			ws.on("error", () => {
				this.wsClients.delete(ws);
			});
		});
	}

	/**
	 * Handle an incoming WebSocket message (binary protobuf).
	 */
	private async handleWsMessage(
		ws: WsWebSocket,
		data: Buffer,
		clientId: string,
		claims: Record<string, unknown>,
	): Promise<void> {
		const bytes = new Uint8Array(data);
		if (bytes.length < 2) {
			ws.close(1002, "Message too short");
			return;
		}

		const tag = bytes[0];
		const payload = bytes.subarray(1);

		if (tag === TAG_SYNC_PUSH) {
			const decoded = decodeSyncPush(payload);
			if (!decoded.ok) {
				ws.close(1008, decoded.error.message);
				return;
			}

			const result = this.gateway.handlePush(decoded.value);
			if (!result.ok) {
				ws.close(1008, result.error.message);
				return;
			}

			// Send push response
			const response = encodeSyncResponse({
				deltas: [],
				serverHlc: result.value.serverHlc,
				hasMore: false,
			});
			if (response.ok) {
				ws.send(response.value);
			}

			// Broadcast to other clients
			this.broadcastDeltas(result.value.deltas, result.value.serverHlc, clientId);
		} else if (tag === TAG_SYNC_PULL) {
			const decoded = decodeSyncPull(payload);
			if (!decoded.ok) {
				ws.close(1008, decoded.error.message);
				return;
			}

			const context = await this.buildWsSyncRulesContext(claims);
			// WebSocket pull is always from the buffer (no source adapter)
			const pullResult = this.gateway.handlePull(
				decoded.value,
				context,
			) as import("@lakesync/core").Result<
				import("@lakesync/core").SyncResponse,
				{ message: string }
			>;
			if (!pullResult.ok) {
				ws.close(1008, pullResult.error.message);
				return;
			}

			const response = encodeSyncResponse(pullResult.value);
			if (response.ok) {
				ws.send(response.value);
			}
		} else {
			ws.close(1002, `Unknown message tag: 0x${tag!.toString(16).padStart(2, "0")}`);
		}
	}

	/**
	 * Build sync rules context from WebSocket client claims.
	 */
	private async buildWsSyncRulesContext(
		claims: Record<string, unknown>,
	): Promise<SyncRulesContext | undefined> {
		const rules = await this.configStore.getSyncRules(this.config.gatewayId);
		if (!rules || rules.buckets.length === 0) {
			return undefined;
		}
		return { claims: claims as ResolvedClaims, rules };
	}

	/**
	 * Broadcast ingested deltas to all connected WebSocket clients except the sender.
	 */
	private broadcastDeltas(
		deltas: RowDelta[],
		serverHlc: HLCTimestamp,
		excludeClientId: string,
	): void {
		if (deltas.length === 0) return;

		// Read sync rules synchronously from the config store's cache.
		// MemoryConfigStore's getSyncRules resolves immediately, but we need
		// to handle it as a microtask to avoid making broadcastDeltas async.
		void this.broadcastDeltasAsync(deltas, serverHlc, excludeClientId);
	}

	private async broadcastDeltasAsync(
		deltas: RowDelta[],
		serverHlc: HLCTimestamp,
		excludeClientId: string,
	): Promise<void> {
		const rules = await this.configStore.getSyncRules(this.config.gatewayId);

		for (const [ws, meta] of this.wsClients) {
			if (meta.clientId === excludeClientId) continue;

			try {
				let filtered: RowDelta[] = deltas;
				if (rules && rules.buckets.length > 0) {
					const context: SyncRulesContext = {
						claims: meta.claims as ResolvedClaims,
						rules,
					};
					filtered = filterDeltas(deltas, context);
				}

				if (filtered.length === 0) continue;

				const frame = encodeBroadcastFrame({
					deltas: filtered,
					serverHlc,
					hasMore: false,
				});

				if (!frame.ok) continue;
				ws.send(frame.value);
			} catch {
				// Socket may have closed -- silently skip
			}
		}
	}

	/** Stop the server and clear the flush timer. */
	async stop(): Promise<void> {
		// Stop dynamic connector pollers and close adapters
		for (const [, poller] of this.connectorPollers) {
			poller.stop();
		}
		this.connectorPollers.clear();

		for (const [, adapter] of this.connectorAdapters) {
			await adapter.close();
		}
		this.connectorAdapters.clear();

		// Stop ingest pollers
		for (const poller of this.pollers) {
			poller.stop();
		}
		this.pollers = [];

		if (this.flushTimer) {
			clearInterval(this.flushTimer);
			this.flushTimer = null;
		}
		// Close all WebSocket connections
		if (this.wss) {
			for (const ws of this.wsClients.keys()) {
				try {
					ws.close(1001, "Server shutting down");
				} catch {
					/* ignore */
				}
			}
			this.wsClients.clear();
			this.wss.close();
			this.wss = null;
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
	// Request handling
	// -----------------------------------------------------------------------

	private async handleRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
		const method = req.method ?? "GET";
		const rawUrl = req.url ?? "/";
		const url = new URL(rawUrl, `http://${req.headers.host ?? "localhost"}`);
		const pathname = url.pathname;
		const origin = req.headers.origin ?? null;
		const corsH = this.corsHeaders(origin);

		// CORS preflight
		if (method === "OPTIONS") {
			res.writeHead(204, corsH);
			res.end();
			return;
		}

		// Health check — unauthenticated
		if (pathname === "/health" && method === "GET") {
			sendJson(res, { status: "ok" }, 200, corsH);
			return;
		}

		// Route matching
		const route = matchRoute(pathname, method);
		if (!route) {
			sendError(res, "Not found", 404, corsH);
			return;
		}

		// Verify gateway ID matches
		if (route.gatewayId !== this.config.gatewayId) {
			sendError(res, "Gateway ID mismatch", 404, corsH);
			return;
		}

		// Authentication
		let auth: AuthClaims | undefined;
		if (this.config.jwtSecret) {
			const token = extractBearerToken(req);
			if (!token) {
				sendError(res, "Missing Bearer token", 401, corsH);
				return;
			}

			const authResult = await verifyToken(token, this.config.jwtSecret);
			if (!authResult.ok) {
				sendError(res, authResult.error.message, 401, corsH);
				return;
			}

			auth = authResult.value;

			// Verify JWT gateway ID matches the route
			if (auth.gatewayId !== route.gatewayId) {
				sendError(res, "Gateway ID mismatch: JWT authorises a different gateway", 403, corsH);
				return;
			}

			// Admin route protection
			if (ADMIN_ACTIONS.has(route.action) && auth.role !== "admin") {
				sendError(res, "Admin role required", 403, corsH);
				return;
			}
		}

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
				await this.handleRegisterConnector(req, res, corsH);
				break;
			case "unregister-connector":
				await this.handleUnregisterConnector(route.connectorName!, res, corsH);
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
	// Route handlers — thin wrappers around shared request handlers
	// -----------------------------------------------------------------------

	/**
	 * Handle `POST /sync/:gatewayId/push` -- ingest client deltas.
	 */
	private async handlePush(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
		auth?: AuthClaims,
	): Promise<void> {
		// Content-Length guard stays in server (before reading body)
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
				this.broadcastDeltas(deltas, serverHlc, excludeClientId),
		});

		// Shared buffer write-through for cross-instance visibility
		if (result.status === 200 && this.sharedBuffer) {
			const pushResult = result.body as { deltas: RowDelta[]; serverHlc: HLCTimestamp };
			if (pushResult.deltas.length > 0) {
				await this.sharedBuffer.writeThroughPush(pushResult.deltas);
			}
		}

		sendResult(res, result, corsH);
	}

	/**
	 * Handle `GET /sync/:gatewayId/pull` -- retrieve deltas since a given HLC.
	 */
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

	/**
	 * Handle `POST /sync/:gatewayId/action` -- execute imperative actions.
	 */
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

	/** Handle `GET /sync/:gatewayId/actions` -- describe available action handlers. */
	private handleDescribeActions(res: ServerResponse, corsH: Record<string, string>): void {
		sendJson(res, this.gateway.describeActions(), 200, corsH);
	}

	/** Handle `POST /admin/flush/:gatewayId` -- manual flush. */
	private async handleFlush(res: ServerResponse, corsH: Record<string, string>): Promise<void> {
		const result = await handleFlushRequest(this.gateway, {
			clearPersistence: () => this.persistence.clear(),
		});
		sendResult(res, result, corsH);
	}

	/** Handle `POST /admin/schema/:gatewayId` -- save table schema. */
	private async handleSaveSchemaRoute(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		const raw = await readBody(req);
		const result = await handleSaveSchema(raw, this.configStore, this.config.gatewayId);
		sendResult(res, result, corsH);
	}

	/** Handle `POST /admin/sync-rules/:gatewayId` -- save sync rules. */
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
	// Connector management
	// -----------------------------------------------------------------------

	/** Handle `POST /admin/connectors/:gatewayId` -- register a new connector. */
	private async handleRegisterConnector(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		const raw = await readBody(req);

		// Use shared handler for validation and ConfigStore registration
		const result = await handleRegisterConnector(raw, this.configStore);
		if (result.status !== 200) {
			sendResult(res, result, corsH);
			return;
		}

		// Extract the registered config from the ConfigStore
		const connectors = await this.configStore.getConnectors();
		const registeredName = (result.body as { name: string }).name;
		const config = connectors[registeredName];
		if (!config) {
			sendResult(res, result, corsH);
			return;
		}

		// Jira connectors use their own API-based poller — no DatabaseAdapter needed
		if (config.type === "jira" && config.jira) {
			try {
				const { JiraSourcePoller } = await import("@lakesync/connector-jira");
				const ingestConfig = config.ingest ? { intervalMs: config.ingest.intervalMs } : undefined;
				const poller = new JiraSourcePoller(config.jira, ingestConfig, config.name, this.gateway);
				poller.start();
				this.connectorPollers.set(config.name, poller);
				sendResult(res, result, corsH);
			} catch (err) {
				// Rollback ConfigStore registration on failure
				delete connectors[registeredName];
				await this.configStore.setConnectors(connectors);
				const message = err instanceof Error ? err.message : String(err);
				sendError(res, `Failed to load Jira connector: ${message}`, 500, corsH);
			}
			return;
		}

		// Salesforce connectors use their own API-based poller — no DatabaseAdapter needed
		if (config.type === "salesforce" && config.salesforce) {
			try {
				const { SalesforceSourcePoller } = await import("@lakesync/connector-salesforce");
				const ingestConfig = config.ingest ? { intervalMs: config.ingest.intervalMs } : undefined;
				const poller = new SalesforceSourcePoller(
					config.salesforce,
					ingestConfig,
					config.name,
					this.gateway,
				);
				poller.start();
				this.connectorPollers.set(config.name, poller);
				sendResult(res, result, corsH);
			} catch (err) {
				// Rollback ConfigStore registration on failure
				delete connectors[registeredName];
				await this.configStore.setConnectors(connectors);
				const message = err instanceof Error ? err.message : String(err);
				sendError(res, `Failed to load Salesforce connector: ${message}`, 500, corsH);
			}
			return;
		}

		// Create the database adapter
		const adapterResult = createDatabaseAdapter(config);
		if (!adapterResult.ok) {
			// Rollback ConfigStore registration on failure
			delete connectors[registeredName];
			await this.configStore.setConnectors(connectors);
			sendError(res, adapterResult.error.message, 500, corsH);
			return;
		}

		const adapter = adapterResult.value;

		// Register with the gateway
		this.gateway.registerSource(config.name, adapter);
		this.connectorAdapters.set(config.name, adapter);

		// Auto-register as action handler if the adapter supports actions
		if (isActionHandler(adapter)) {
			this.gateway.registerActionHandler(config.name, adapter);
		}

		// Start ingest poller if configured
		if (config.ingest) {
			const queryFn = await createQueryFn(config);
			if (queryFn) {
				const pollerConfig: IngestSourceConfig = {
					name: config.name,
					queryFn,
					tables: config.ingest.tables.map((t) => ({
						table: t.table,
						query: t.query,
						rowIdColumn: t.rowIdColumn,
						strategy: t.strategy,
					})),
					intervalMs: config.ingest.intervalMs,
				};
				const poller = new SourcePoller(pollerConfig, this.gateway);
				poller.start();
				this.connectorPollers.set(config.name, poller);
			}
		}

		sendResult(res, result, corsH);
	}

	/** Handle `DELETE /admin/connectors/:gatewayId/:name` -- unregister a connector. */
	private async handleUnregisterConnector(
		name: string,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		// Use shared handler for ConfigStore removal
		const result = await handleUnregisterConnector(name, this.configStore);
		if (result.status !== 200) {
			sendResult(res, result, corsH);
			return;
		}

		// Stop poller if running
		const poller = this.connectorPollers.get(name);
		if (poller) {
			poller.stop();
			this.connectorPollers.delete(name);
		}

		// Close adapter
		const adapter = this.connectorAdapters.get(name);
		if (adapter) {
			await adapter.close();
			this.connectorAdapters.delete(name);
		}

		// Unregister from gateway
		this.gateway.unregisterSource(name);
		this.gateway.unregisterActionHandler(name);

		sendResult(res, result, corsH);
	}

	/** Handle `GET /admin/connectors/:gatewayId` -- list registered connectors. */
	private async handleListConnectorsRoute(
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		// Use shared handler to get base list, then augment with polling status
		const result = await handleListConnectors(this.configStore);
		if (result.status !== 200) {
			sendResult(res, result, corsH);
			return;
		}

		// Augment the list with live polling status
		const list = result.body as Array<{ name: string; type: string; hasIngest: boolean }>;
		const augmented = list.map((c) => ({
			...c,
			isPolling: this.connectorPollers.get(c.name)?.isRunning ?? false,
		}));

		sendJson(res, augmented, 200, corsH);
	}

	/** Handle `GET /admin/metrics/:gatewayId` -- return buffer stats and process memory. */
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

		// Acquire distributed lock for coordinated flush (if clustering is enabled)
		const lock = this.config.cluster?.lock;
		const lockKey = `flush:${this.config.gatewayId}`;
		if (lock) {
			const acquired = await lock.acquire(lockKey, 30_000);
			if (!acquired) {
				return; // Another instance is flushing
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

	// -----------------------------------------------------------------------
	// CORS
	// -----------------------------------------------------------------------

	private corsHeaders(origin?: string | null): Record<string, string> {
		const allowedOrigins = this.config.allowedOrigins;
		let allowOrigin = "*";

		if (allowedOrigins && allowedOrigins.length > 0) {
			if (origin && allowedOrigins.includes(origin)) {
				allowOrigin = origin;
			} else {
				return {};
			}
		} else if (origin) {
			allowOrigin = origin;
		}

		return {
			"Access-Control-Allow-Origin": allowOrigin,
			"Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
			"Access-Control-Allow-Headers": "Authorization, Content-Type",
			"Access-Control-Max-Age": "86400",
		};
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Extract the Bearer token from an Authorization header.
 * Returns the raw token string, or null if missing/malformed.
 */
function extractBearerToken(req: IncomingMessage): string | null {
	const header = req.headers.authorization;
	if (!header) return null;
	const match = header.match(/^Bearer\s+(\S+)$/);
	return match?.[1] ?? null;
}

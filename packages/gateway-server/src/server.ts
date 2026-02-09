import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import {
	createDatabaseAdapter,
	createQueryFn,
	type DatabaseAdapter,
	type LakeAdapter,
} from "@lakesync/adapter";
import type {
	ConnectorConfig,
	HLCTimestamp,
	ResolvedClaims,
	RowDelta,
	SyncRulesConfig,
	SyncRulesContext,
	TableSchema,
} from "@lakesync/core";
import {
	bigintReplacer,
	bigintReviver,
	filterDeltas,
	validateConnectorConfig,
	validateSyncRules,
} from "@lakesync/core";
import { SyncGateway, type SyncPull, type SyncPush } from "@lakesync/gateway";
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
import { SourcePoller } from "./ingest/poller";
import type { IngestSourceConfig } from "./ingest/types";
import { type DeltaPersistence, MemoryPersistence, SqlitePersistence } from "./persistence";

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
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_PORT = 3000;
const DEFAULT_MAX_BUFFER_BYTES = 4 * 1024 * 1024;
const DEFAULT_MAX_BUFFER_AGE_MS = 30_000;
const DEFAULT_FLUSH_INTERVAL_MS = 30_000;
const MAX_PUSH_PAYLOAD_BYTES = 1_048_576;
const MAX_DELTAS_PER_PUSH = 10_000;
const MAX_PULL_LIMIT = 10_000;
const DEFAULT_PULL_LIMIT = 100;
const VALID_COLUMN_TYPES = new Set(["string", "number", "boolean", "json", "null"]);

// ---------------------------------------------------------------------------
// Route matching
// ---------------------------------------------------------------------------

interface RouteMatch {
	gatewayId: string;
	action: string;
	/** Extra route parameters (e.g. connector name from DELETE path). */
	connectorName?: string;
}

function matchRoute(pathname: string, method: string): RouteMatch | null {
	// POST /sync/:gatewayId/push
	{
		const match = pathname.match(/^\/sync\/([^/]+)\/push$/);
		if (match && method === "POST") {
			return { gatewayId: match[1]!, action: "push" };
		}
	}
	// GET /sync/:gatewayId/pull
	{
		const match = pathname.match(/^\/sync\/([^/]+)\/pull$/);
		if (match && method === "GET") {
			return { gatewayId: match[1]!, action: "pull" };
		}
	}
	// POST /admin/flush/:gatewayId
	{
		const match = pathname.match(/^\/admin\/flush\/([^/]+)$/);
		if (match && method === "POST") {
			return { gatewayId: match[1]!, action: "flush" };
		}
	}
	// POST /admin/schema/:gatewayId
	{
		const match = pathname.match(/^\/admin\/schema\/([^/]+)$/);
		if (match && method === "POST") {
			return { gatewayId: match[1]!, action: "schema" };
		}
	}
	// POST /admin/sync-rules/:gatewayId
	{
		const match = pathname.match(/^\/admin\/sync-rules\/([^/]+)$/);
		if (match && method === "POST") {
			return { gatewayId: match[1]!, action: "sync-rules" };
		}
	}
	// POST /admin/connectors/:gatewayId
	{
		const match = pathname.match(/^\/admin\/connectors\/([^/]+)$/);
		if (match && method === "POST") {
			return { gatewayId: match[1]!, action: "register-connector" };
		}
	}
	// GET /admin/connectors/:gatewayId
	{
		const match = pathname.match(/^\/admin\/connectors\/([^/]+)$/);
		if (match && method === "GET") {
			return { gatewayId: match[1]!, action: "list-connectors" };
		}
	}
	// DELETE /admin/connectors/:gatewayId/:name
	{
		const match = pathname.match(/^\/admin\/connectors\/([^/]+)\/([^/]+)$/);
		if (match && method === "DELETE") {
			return { gatewayId: match[1]!, action: "unregister-connector", connectorName: match[2]! };
		}
	}
	// WebSocket /sync/:gatewayId/ws
	{
		const match = pathname.match(/^\/sync\/([^/]+)\/ws$/);
		if (match && method === "GET") {
			return { gatewayId: match[1]!, action: "ws" };
		}
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
	private schemas = new Map<string, TableSchema>();
	private syncRules = new Map<string, SyncRulesConfig>();
	private persistence: DeltaPersistence;
	private resolvedPort = 0;
	private wss: WebSocketServer | null = null;
	private wsClients = new Map<WsWebSocket, { clientId: string; claims: Record<string, unknown> }>();
	private pollers: SourcePoller[] = [];
	private connectorConfigs = new Map<string, ConnectorConfig>();
	private connectorAdapters = new Map<string, DatabaseAdapter>();
	private connectorPollers = new Map<string, Poller>();

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

		this.persistence =
			config.persistence === "sqlite"
				? new SqlitePersistence(config.sqlitePath ?? "./lakesync-buffer.sqlite")
				: new MemoryPersistence();
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

			const context = this.buildWsSyncRulesContext(claims);
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
	private buildWsSyncRulesContext(claims: Record<string, unknown>): SyncRulesContext | undefined {
		const rules = this.syncRules.get(this.config.gatewayId);
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

		const rules = this.syncRules.get(this.config.gatewayId);

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
		this.connectorConfigs.clear();

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
			if (
				route.action === "flush" ||
				route.action === "schema" ||
				route.action === "sync-rules" ||
				route.action === "register-connector" ||
				route.action === "unregister-connector" ||
				route.action === "list-connectors"
			) {
				if (auth.role !== "admin") {
					sendError(res, "Admin role required", 403, corsH);
					return;
				}
			}
		}

		switch (route.action) {
			case "push":
				await this.handlePush(req, res, corsH, auth);
				break;
			case "pull":
				await this.handlePull(url, res, corsH, auth);
				break;
			case "flush":
				await this.handleFlush(res, corsH);
				break;
			case "schema":
				await this.handleSaveSchema(req, res, corsH);
				break;
			case "sync-rules":
				await this.handleSaveSyncRules(req, res, corsH);
				break;
			case "register-connector":
				await this.handleRegisterConnector(req, res, corsH);
				break;
			case "unregister-connector":
				await this.handleUnregisterConnector(route.connectorName!, res, corsH);
				break;
			case "list-connectors":
				this.handleListConnectors(res, corsH);
				break;
			default:
				sendError(res, "Not found", 404, corsH);
		}
	}

	// -----------------------------------------------------------------------
	// Route handlers
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
		const contentLength = Number(req.headers["content-length"] ?? "0");
		if (contentLength > MAX_PUSH_PAYLOAD_BYTES) {
			sendError(res, "Payload too large (max 1 MiB)", 413, corsH);
			return;
		}

		let body: SyncPush;
		try {
			const raw = await readBody(req);
			body = JSON.parse(raw, bigintReviver) as SyncPush;
		} catch {
			sendError(res, "Invalid JSON body", 400, corsH);
			return;
		}

		if (!body.clientId || !Array.isArray(body.deltas)) {
			sendError(res, "Missing required fields: clientId, deltas", 400, corsH);
			return;
		}

		if (auth && body.clientId !== auth.clientId) {
			sendError(
				res,
				"Client ID mismatch: push clientId does not match authenticated identity",
				403,
				corsH,
			);
			return;
		}

		if (body.deltas.length > MAX_DELTAS_PER_PUSH) {
			sendError(res, "Too many deltas in a single push (max 10,000)", 400, corsH);
			return;
		}

		// Persist before processing (WAL-style)
		this.persistence.appendBatch(body.deltas);

		const result = this.gateway.handlePush(body);

		if (!result.ok) {
			const err = result.error;
			if (err.code === "CLOCK_DRIFT") {
				sendError(res, err.message, 409, corsH);
				return;
			}
			if (err.code === "SCHEMA_MISMATCH") {
				sendError(res, err.message, 422, corsH);
				return;
			}
			sendError(res, err.message, 500, corsH);
			return;
		}

		// Clear persisted deltas on success — they are now in the in-memory buffer
		this.persistence.clear();

		// Broadcast to connected WebSocket clients
		this.broadcastDeltas(result.value.deltas, result.value.serverHlc, body.clientId);

		sendJson(res, result.value, 200, corsH);
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
		const sinceParam = url.searchParams.get("since");
		const clientId = url.searchParams.get("clientId");
		const limitParam = url.searchParams.get("limit");
		const source = url.searchParams.get("source");

		if (!sinceParam || !clientId) {
			sendError(res, "Missing required query params: since, clientId", 400, corsH);
			return;
		}

		let sinceHlc: HLCTimestamp;
		try {
			sinceHlc = BigInt(sinceParam) as HLCTimestamp;
		} catch {
			sendError(res, "Invalid 'since' parameter — must be a decimal integer", 400, corsH);
			return;
		}

		const rawLimit = limitParam ? Number.parseInt(limitParam, 10) : DEFAULT_PULL_LIMIT;
		if (Number.isNaN(rawLimit) || rawLimit < 1) {
			sendError(res, "Invalid 'limit' parameter — must be a positive integer", 400, corsH);
			return;
		}
		const maxDeltas = Math.min(rawLimit, MAX_PULL_LIMIT);

		const msg: SyncPull = { clientId, sinceHlc, maxDeltas, ...(source ? { source } : {}) };
		const context = this.buildSyncRulesContext(auth);

		const result = source
			? await this.gateway.handlePull(msg as SyncPull & { source: string }, context)
			: this.gateway.handlePull(msg, context);

		if (!result.ok) {
			const err = result.error;
			if (err.code === "ADAPTER_NOT_FOUND") {
				sendError(res, err.message, 404, corsH);
				return;
			}
			sendError(res, err.message, 500, corsH);
			return;
		}

		sendJson(res, result.value, 200, corsH);
	}

	/** Handle `POST /admin/flush/:gatewayId` -- manual flush. */
	private async handleFlush(res: ServerResponse, corsH: Record<string, string>): Promise<void> {
		const result = await this.gateway.flush();
		if (!result.ok) {
			sendError(res, result.error.message, 500, corsH);
			return;
		}
		this.persistence.clear();
		sendJson(res, { flushed: true }, 200, corsH);
	}

	/** Handle `POST /admin/schema/:gatewayId` -- save table schema. */
	private async handleSaveSchema(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		let schema: TableSchema;
		try {
			const raw = await readBody(req);
			schema = JSON.parse(raw) as TableSchema;
		} catch {
			sendError(res, "Invalid JSON body", 400, corsH);
			return;
		}

		if (!schema.table || !Array.isArray(schema.columns)) {
			sendError(res, "Missing required fields: table, columns", 400, corsH);
			return;
		}

		for (const col of schema.columns) {
			if (typeof col.name !== "string" || col.name.length === 0) {
				sendError(res, "Each column must have a non-empty 'name' string", 400, corsH);
				return;
			}
			if (!VALID_COLUMN_TYPES.has(col.type)) {
				sendError(
					res,
					`Invalid column type "${col.type}" for column "${col.name}". Allowed: string, number, boolean, json, null`,
					400,
					corsH,
				);
				return;
			}
		}

		this.schemas.set(this.config.gatewayId, schema);
		sendJson(res, { saved: true }, 200, corsH);
	}

	/** Handle `POST /admin/sync-rules/:gatewayId` -- save sync rules. */
	private async handleSaveSyncRules(
		req: IncomingMessage,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		let config: unknown;
		try {
			const raw = await readBody(req);
			config = JSON.parse(raw);
		} catch {
			sendError(res, "Invalid JSON body", 400, corsH);
			return;
		}

		const validation = validateSyncRules(config);
		if (!validation.ok) {
			sendError(res, validation.error.message, 400, corsH);
			return;
		}

		this.syncRules.set(this.config.gatewayId, config as SyncRulesConfig);
		sendJson(res, { saved: true }, 200, corsH);
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
		let body: unknown;
		try {
			const raw = await readBody(req);
			body = JSON.parse(raw);
		} catch {
			sendError(res, "Invalid JSON body", 400, corsH);
			return;
		}

		const validation = validateConnectorConfig(body);
		if (!validation.ok) {
			sendError(res, validation.error.message, 400, corsH);
			return;
		}

		const config = validation.value;

		if (this.connectorConfigs.has(config.name)) {
			sendError(res, `Connector "${config.name}" already exists`, 409, corsH);
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
				this.connectorConfigs.set(config.name, config);
				sendJson(res, { registered: true, name: config.name }, 200, corsH);
			} catch (err) {
				const message = err instanceof Error ? err.message : String(err);
				sendError(res, `Failed to load Jira connector: ${message}`, 500, corsH);
			}
			return;
		}

		// Create the database adapter
		const adapterResult = createDatabaseAdapter(config);
		if (!adapterResult.ok) {
			sendError(res, adapterResult.error.message, 500, corsH);
			return;
		}

		const adapter = adapterResult.value;

		// Register with the gateway
		this.gateway.registerSource(config.name, adapter);
		this.connectorConfigs.set(config.name, config);
		this.connectorAdapters.set(config.name, adapter);

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

		sendJson(res, { registered: true, name: config.name }, 200, corsH);
	}

	/** Handle `DELETE /admin/connectors/:gatewayId/:name` -- unregister a connector. */
	private async handleUnregisterConnector(
		name: string,
		res: ServerResponse,
		corsH: Record<string, string>,
	): Promise<void> {
		if (!this.connectorConfigs.has(name)) {
			sendError(res, `Connector "${name}" not found`, 404, corsH);
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
		this.connectorConfigs.delete(name);

		sendJson(res, { unregistered: true, name }, 200, corsH);
	}

	/** Handle `GET /admin/connectors/:gatewayId` -- list registered connectors. */
	private handleListConnectors(res: ServerResponse, corsH: Record<string, string>): void {
		const list = Array.from(this.connectorConfigs.values()).map((c) => ({
			name: c.name,
			type: c.type,
			hasIngest: c.ingest !== undefined,
			isPolling: this.connectorPollers.get(c.name)?.isRunning ?? false,
		}));

		sendJson(res, list, 200, corsH);
	}

	// -----------------------------------------------------------------------
	// Periodic flush
	// -----------------------------------------------------------------------

	private async periodicFlush(): Promise<void> {
		if (this.gateway.bufferStats.logSize === 0) {
			return;
		}

		const result = await this.gateway.flush();
		if (result.ok) {
			this.persistence.clear();
		}
	}

	// -----------------------------------------------------------------------
	// Sync rules context
	// -----------------------------------------------------------------------

	private buildSyncRulesContext(auth?: AuthClaims): SyncRulesContext | undefined {
		const rules = this.syncRules.get(this.config.gatewayId);
		if (!rules || rules.buckets.length === 0) {
			return undefined;
		}

		const claims = auth?.customClaims ?? {};
		return { claims, rules };
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

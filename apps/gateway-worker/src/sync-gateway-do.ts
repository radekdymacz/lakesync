import { DurableObject } from "cloudflare:workers";
import type {
	ConnectorConfig,
	HLCTimestamp,
	ResolvedClaims,
	RowDelta,
	SyncRulesConfig,
	TableSchema,
} from "@lakesync/core";
import { bigintReplacer, bigintReviver, filterDeltas } from "@lakesync/core";
import {
	buildSyncRulesContext,
	type ConfigStore,
	DEFAULT_MAX_BUFFER_AGE_MS,
	DEFAULT_MAX_BUFFER_BYTES,
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
	SyncGateway,
} from "@lakesync/gateway";
import { decodeSyncResponse, encodeSyncResponse } from "@lakesync/proto";
import { CloudflareFlushQueue } from "./cf-flush-queue";
import type { Env } from "./env";
import { logger } from "./logger";
import { R2Adapter } from "./r2-adapter";
import {
	broadcastDeltasToSockets,
	deserializeWsAttachment,
	handleWebSocketMessage,
} from "./ws-handlers";

// ---------------------------------------------------------------------------
// Response factories
// ---------------------------------------------------------------------------

/** Create a JSON response with the given status and body. */
function jsonResponse(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body, bigintReplacer), {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

/** Create a JSON error response. */
function errorResponse(message: string, status: number): Response {
	return jsonResponse({ error: message }, status);
}

// ---------------------------------------------------------------------------
// Constants (DO-specific — shared constants imported from gateway package)
// ---------------------------------------------------------------------------

/** Maximum backoff delay for flush retries (30 seconds). */
const MAX_RETRY_BACKOFF_MS = 30_000;

/** Base backoff delay for flush retries (1 second). */
const BASE_RETRY_BACKOFF_MS = 1_000;

// ---------------------------------------------------------------------------
// DurableStorageConfigStore
// ---------------------------------------------------------------------------

/**
 * ConfigStore implementation backed by Durable Object storage.
 *
 * Wraps the Cloudflare Workers DurableObjectStorage API to satisfy
 * the platform-agnostic ConfigStore interface from @lakesync/gateway.
 */
class DurableStorageConfigStore implements ConfigStore {
	constructor(private storage: DurableObjectStorage) {}

	async getSchema(_gatewayId: string): Promise<TableSchema | undefined> {
		return (await this.storage.get<TableSchema>("tableSchema")) ?? undefined;
	}

	async setSchema(_gatewayId: string, schema: TableSchema): Promise<void> {
		await this.storage.put("tableSchema", schema);
	}

	async getSyncRules(_gatewayId: string): Promise<SyncRulesConfig | undefined> {
		return (await this.storage.get<SyncRulesConfig>("syncRules")) ?? undefined;
	}

	async setSyncRules(_gatewayId: string, rules: SyncRulesConfig): Promise<void> {
		await this.storage.put("syncRules", rules);
	}

	async getConnectors(): Promise<Record<string, ConnectorConfig>> {
		return (await this.storage.get<Record<string, ConnectorConfig>>("connectors")) ?? {};
	}

	async setConnectors(connectors: Record<string, ConnectorConfig>): Promise<void> {
		await this.storage.put("connectors", connectors);
	}
}

// ---------------------------------------------------------------------------
// SyncGatewayDO
// ---------------------------------------------------------------------------

/**
 * Durable Object that wraps a {@link SyncGateway} instance.
 *
 * Each DO instance manages one logical gateway's delta buffer and flush
 * lifecycle. It exposes HTTP endpoints for push, pull, flush, schema,
 * sync rules, connectors, metrics, and checkpoint streaming, and supports
 * WebSocket connections for binary protobuf transport.
 *
 * Route handling delegates to shared request handlers from @lakesync/gateway,
 * keeping the DO layer focused on platform-specific concerns: Durable Storage,
 * alarms, WebSocket hibernation, and checkpoint streaming.
 */
export class SyncGatewayDO extends DurableObject<Env> {
	/** Lazily initialised gateway instance. */
	private gateway: SyncGateway | null = null;

	/** Platform-agnostic config store backed by Durable Storage. */
	private configStore: DurableStorageConfigStore;

	/** Consecutive flush failure count for exponential backoff. Reset on success. */
	private flushRetryCount = 0;

	/** High-water mark for buffer byte usage. */
	private peakBufferBytes = 0;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.configStore = new DurableStorageConfigStore(ctx.storage);
	}

	/**
	 * Return the gateway instance, creating it on first access.
	 *
	 * Creates an {@link R2Adapter} from the environment's LAKE_BUCKET
	 * binding and passes it to the gateway. Uses Parquet flush format
	 * when a {@link TableSchema} is stored in Durable Storage, falling
	 * back to JSON when no schema is configured.
	 */
	private async getGateway(): Promise<SyncGateway> {
		if (this.gateway) {
			return this.gateway;
		}

		const adapter = new R2Adapter(this.env.LAKE_BUCKET);
		const tableSchema = await this.configStore.getSchema(this.ctx.id.toString());

		// Build optional CloudflareFlushQueue when the MATERIALISE_QUEUE binding exists
		const flushQueue = this.env.MATERIALISE_QUEUE
			? new CloudflareFlushQueue(adapter, this.env.MATERIALISE_QUEUE)
			: undefined;

		this.gateway = new SyncGateway(
			{
				gatewayId: this.ctx.id.toString(),
				maxBufferBytes: this.env.MAX_BUFFER_BYTES
					? Number(this.env.MAX_BUFFER_BYTES)
					: DEFAULT_MAX_BUFFER_BYTES,
				maxBufferAgeMs: DEFAULT_MAX_BUFFER_AGE_MS,
				flushFormat: tableSchema ? "parquet" : "json",
				tableSchema,
				flushQueue,
			},
			adapter,
		);

		return this.gateway;
	}

	/**
	 * Extract JWT claims from the X-Auth-Claims header.
	 *
	 * The worker entry point forwards decoded JWT claims as a JSON string
	 * in this header. Returns an empty object if the header is absent or
	 * contains invalid JSON.
	 */
	private extractClaims(request: Request): ResolvedClaims {
		const claimsHeader = request.headers.get("X-Auth-Claims");
		if (!claimsHeader) return {};

		try {
			return JSON.parse(claimsHeader) as ResolvedClaims;
		} catch {
			return {};
		}
	}

	/**
	 * Handle incoming HTTP requests routed from the Worker fetch handler.
	 *
	 * Supported routes:
	 * - `POST /push`  -- ingest client deltas
	 * - `GET  /pull`  -- retrieve deltas since a given HLC
	 * - `POST /action` -- execute imperative actions
	 * - `GET  /actions` -- describe available action handlers
	 * - `POST /flush` -- flush the buffer to the lake
	 * - `POST /admin/schema` -- save table schema
	 * - `POST /admin/sync-rules` -- save sync rules
	 * - `POST|GET /admin/connectors` -- register or list connectors
	 * - `DELETE /admin/connectors/:name` -- unregister a connector
	 * - `GET  /admin/metrics` -- return buffer statistics
	 * - `GET  /checkpoint` -- serve checkpoint for initial sync
	 * - `POST /internal/broadcast` -- broadcast deltas from another shard
	 * - Upgrade to WebSocket for binary protobuf transport
	 */
	async fetch(request: Request): Promise<Response> {
		// WebSocket upgrade
		const upgradeHeader = request.headers.get("Upgrade");
		if (upgradeHeader?.toLowerCase() === "websocket") {
			return this.handleWebSocketUpgrade(request);
		}

		const url = new URL(request.url);

		// Handle DELETE /admin/connectors/:name (parametric route)
		if (url.pathname.startsWith("/admin/connectors/")) {
			const name = url.pathname.slice("/admin/connectors/".length);
			if (name && request.method === "DELETE") {
				return this.handleUnregisterConnector(name);
			}
		}

		const routeMap: Record<string, (request: Request, url: URL) => Promise<Response>> = {
			"/push": (req) => this.handlePush(req),
			"/pull": (req, u) => this.handlePull(u, req),
			"/action": (req) => this.handleAction(req),
			"/actions": () => this.handleDescribeActions(),
			"/flush": () => this.handleFlush(),
			"/admin/schema": (req) => this.handleSchema(req),
			"/admin/sync-rules": (req) => this.handleSyncRules(req),
			"/admin/connectors": (req) => this.handleConnectors(req),
			"/checkpoint": (req) => this.handleCheckpoint(req),
			"/admin/metrics": () => this.handleMetrics(),
			"/internal/broadcast": (req) => this.handleInternalBroadcast(req),
		};

		const handler = routeMap[url.pathname];
		return handler ? handler(request, url) : errorResponse("Not found", 404);
	}

	// -----------------------------------------------------------------------
	// HTTP route handlers
	// -----------------------------------------------------------------------

	/**
	 * Handle `POST /push` -- ingest client deltas via JSON.
	 *
	 * Delegates validation and processing to the shared handlePushRequest
	 * handler. Retains DO-specific concerns: Content-Length pre-check,
	 * peak buffer tracking, per-table budget auto-flush, and alarm scheduling.
	 */
	private async handlePush(request: Request): Promise<Response> {
		if (request.method !== "POST") {
			return errorResponse("Method not allowed", 405);
		}

		const contentLength = Number(request.headers.get("Content-Length") ?? "0");
		if (contentLength > MAX_PUSH_PAYLOAD_BYTES) {
			return errorResponse("Payload too large (max 1 MiB)", 413);
		}

		const raw = await request.text();
		const headerClientId = request.headers.get("X-Client-Id");
		const gateway = await this.getGateway();

		const result = handlePushRequest(gateway, raw, headerClientId, {
			broadcastFn: (deltas, serverHlc, excludeClientId) => {
				this.broadcastDeltas(deltas, serverHlc, excludeClientId);
			},
		});

		if (result.status === 200) {
			// Track peak buffer usage
			this.peakBufferBytes = Math.max(this.peakBufferBytes, gateway.bufferStats.byteSize);

			// Auto-flush tables that exceed per-table budget
			const hotTables = gateway.getTablesExceedingBudget();
			for (const table of hotTables) {
				await gateway.flushTable(table);
			}

			await this.scheduleFlushAlarm(gateway);
		}

		return jsonResponse(result.body, result.status);
	}

	/**
	 * Handle `GET /pull` -- retrieve deltas since a given HLC.
	 *
	 * Delegates validation and filtering to the shared handlePullRequest handler.
	 */
	private async handlePull(url: URL, request: Request): Promise<Response> {
		const gateway = await this.getGateway();
		const gatewayId = this.ctx.id.toString();
		const syncRules = await this.configStore.getSyncRules(gatewayId);
		const claims = this.extractClaims(request);

		const result = await handlePullRequest(
			gateway,
			{
				since: url.searchParams.get("since"),
				clientId: url.searchParams.get("clientId"),
				limit: url.searchParams.get("limit"),
				source: url.searchParams.get("source"),
			},
			claims,
			syncRules,
		);

		return jsonResponse(result.body, result.status);
	}

	/**
	 * Handle `POST /action` -- execute imperative actions.
	 *
	 * Delegates validation and dispatch to the shared handleActionRequest handler.
	 */
	private async handleAction(request: Request): Promise<Response> {
		if (request.method !== "POST") {
			return errorResponse("Method not allowed", 405);
		}

		const raw = await request.text();
		const headerClientId = request.headers.get("X-Client-Id");
		const claims = this.extractClaims(request);
		const gateway = await this.getGateway();

		const result = await handleActionRequest(gateway, raw, headerClientId, claims);
		return jsonResponse(result.body, result.status);
	}

	/**
	 * Handle `GET /actions` -- describe available action handlers.
	 *
	 * Returns a map of connector names to their supported action descriptors,
	 * enabling frontend discovery of available actions.
	 */
	private async handleDescribeActions(): Promise<Response> {
		const gateway = await this.getGateway();
		return jsonResponse(gateway.describeActions());
	}

	/**
	 * Handle `POST /flush` -- flush the buffer to the lake adapter.
	 *
	 * Delegates to the shared handleFlushRequest handler.
	 */
	private async handleFlush(): Promise<Response> {
		const gateway = await this.getGateway();
		const result = await handleFlushRequest(gateway);
		return jsonResponse(result.body, result.status);
	}

	/**
	 * Handle `POST /admin/schema` -- save a table schema.
	 *
	 * Delegates validation and persistence to the shared handleSaveSchema handler.
	 * Resets the cached gateway on success so the next getGateway() call picks up
	 * the new schema and flush format.
	 */
	private async handleSchema(request: Request): Promise<Response> {
		if (request.method !== "POST") {
			return errorResponse("Method not allowed", 405);
		}

		const raw = await request.text();
		const gatewayId = this.ctx.id.toString();
		const result = await handleSaveSchema(raw, this.configStore, gatewayId);

		if (result.status === 200) {
			// Reset gateway so next getGateway() picks up the new schema
			this.gateway = null;
			logger.info("schema_saved", { gatewayId });
		}

		return jsonResponse(result.body, result.status);
	}

	/**
	 * Handle `POST /admin/sync-rules` -- save sync rules.
	 *
	 * Delegates validation and persistence to the shared handleSaveSyncRules handler.
	 */
	private async handleSyncRules(request: Request): Promise<Response> {
		if (request.method !== "POST") {
			return errorResponse("Method not allowed", 405);
		}

		const raw = await request.text();
		const gatewayId = this.ctx.id.toString();
		const result = await handleSaveSyncRules(raw, this.configStore, gatewayId);

		if (result.status === 200) {
			logger.info("sync_rules_saved", { gatewayId });
		}

		return jsonResponse(result.body, result.status);
	}

	/**
	 * Route connector admin requests by method.
	 */
	private async handleConnectors(request: Request): Promise<Response> {
		if (request.method === "POST") {
			const raw = await request.text();
			const result = await handleRegisterConnector(raw, this.configStore);

			if (result.status === 200) {
				const body = result.body as { name: string };
				logger.info("connector_registered", { name: body.name });
			}

			return jsonResponse(result.body, result.status);
		}

		if (request.method === "GET") {
			const result = await handleListConnectors(this.configStore);
			return jsonResponse(result.body, result.status);
		}

		return errorResponse("Method not allowed", 405);
	}

	/**
	 * Handle `DELETE /admin/connectors/:name` -- unregister a connector.
	 */
	private async handleUnregisterConnector(name: string): Promise<Response> {
		const result = await handleUnregisterConnector(name, this.configStore);

		if (result.status === 200) {
			logger.info("connector_unregistered", { name });
		}

		return jsonResponse(result.body, result.status);
	}

	/**
	 * Stream checkpoint chunks as length-prefixed proto frames.
	 *
	 * Each frame is: 4-byte big-endian length prefix + proto-encoded SyncResponse bytes.
	 * Only chunks with deltas surviving sync-rules filtering are emitted.
	 */
	private async handleCheckpoint(request: Request): Promise<Response> {
		const adapter = new R2Adapter(this.env.LAKE_BUCKET);
		const gatewayId = this.ctx.id.toString();
		const manifestKey = `checkpoints/${gatewayId}/manifest.json`;

		// Read manifest
		const manifestResult = await adapter.getObject(manifestKey);
		if (!manifestResult.ok) {
			return errorResponse("No checkpoint available", 404);
		}

		let manifest: { snapshotHlc: string; chunks: string[]; chunkCount: number };
		try {
			manifest = JSON.parse(new TextDecoder().decode(manifestResult.value));
		} catch {
			return errorResponse("Corrupt checkpoint manifest", 500);
		}

		const syncRules = await this.configStore.getSyncRules(gatewayId);
		const claims = this.extractClaims(request);
		const context = buildSyncRulesContext(syncRules, claims);
		const snapshotHlc = BigInt(manifest.snapshotHlc) as HLCTimestamp;

		const { readable, writable } = new TransformStream<Uint8Array>();
		const writer = writable.getWriter();

		// Process chunks in the background — the stream handles backpressure
		const processChunks = async () => {
			try {
				for (const chunkName of manifest.chunks) {
					const chunkKey = `checkpoints/${gatewayId}/${chunkName}`;
					const chunkResult = await adapter.getObject(chunkKey);
					if (!chunkResult.ok) continue;

					const decoded = decodeSyncResponse(chunkResult.value);
					if (!decoded.ok) continue;

					const deltas = context
						? filterDeltas(decoded.value.deltas, context)
						: decoded.value.deltas;
					if (deltas.length === 0) continue;

					// Encode filtered deltas as a SyncResponse frame
					const frame = encodeSyncResponse({
						deltas,
						serverHlc: snapshotHlc,
						hasMore: false,
					});
					if (!frame.ok) continue;

					// Write 4-byte BE length prefix + frame bytes
					const lengthPrefix = new Uint8Array(4);
					new DataView(lengthPrefix.buffer).setUint32(0, frame.value.byteLength, false);
					await writer.write(lengthPrefix);
					await writer.write(frame.value);
				}
			} finally {
				await writer.close();
			}
		};

		// Fire and forget — the stream will handle backpressure
		processChunks();

		return new Response(readable, {
			status: 200,
			headers: {
				"Content-Type": "application/x-lakesync-checkpoint-stream",
				"X-Checkpoint-Hlc": manifest.snapshotHlc,
			},
		});
	}

	/**
	 * Handle `GET /admin/metrics` -- return buffer statistics.
	 *
	 * Delegates to the shared handleMetrics handler, adding the
	 * DO-specific peakBufferBytes high-water mark.
	 */
	private async handleMetrics(): Promise<Response> {
		const gateway = await this.getGateway();
		const result = handleMetrics(gateway, { peakBufferBytes: this.peakBufferBytes });
		return jsonResponse(result.body, result.status);
	}

	/**
	 * Handle `POST /internal/broadcast` -- broadcast deltas from another shard's push.
	 *
	 * This is an internal DO-to-DO endpoint, not exposed to the public internet.
	 * The worker entry point never routes to this path; only other DOs call it
	 * via stub.fetch().
	 */
	private async handleInternalBroadcast(request: Request): Promise<Response> {
		if (request.method !== "POST") {
			return errorResponse("Method not allowed", 405);
		}

		let body: { deltas: RowDelta[]; serverHlc: HLCTimestamp; excludeClientId: string };
		try {
			const raw = await request.text();
			body = JSON.parse(raw, bigintReviver) as typeof body;
		} catch {
			return errorResponse("Invalid JSON body", 400);
		}

		if (!Array.isArray(body.deltas) || body.deltas.length === 0) {
			return jsonResponse({ broadcast: true, clients: 0 });
		}

		await this.broadcastDeltas(body.deltas, body.serverHlc, body.excludeClientId);

		return jsonResponse({ broadcast: true });
	}

	// -----------------------------------------------------------------------
	// WebSocket (hibernatable)
	// -----------------------------------------------------------------------

	/**
	 * Upgrade an HTTP request to a WebSocket connection.
	 *
	 * Stores authenticated JWT claims and client ID on the server WebSocket
	 * via `serializeAttachment` so they are available during message handling.
	 * Uses the Durable Object's hibernatable WebSocket API so the DO can
	 * sleep between messages without losing the connection.
	 */
	private handleWebSocketUpgrade(request: Request): Response {
		const pair = new WebSocketPair();
		const [client, server] = [pair[0], pair[1]];

		// Store claims and clientId from the authenticated request
		const claims = this.extractClaims(request);
		const clientId = request.headers.get("X-Client-Id");

		this.ctx.acceptWebSocket(server);
		(server as unknown as { serializeAttachment: (data: unknown) => void }).serializeAttachment({
			claims,
			clientId,
		});

		logger.info("ws_connect", { clientId: clientId ?? "unknown" });

		return new Response(null, {
			status: 101,
			webSocket: client,
		});
	}

	/**
	 * Handle an incoming WebSocket message (binary protobuf).
	 *
	 * Delegates to the standalone {@link handleWebSocketMessage} function
	 * which uses a data-driven dispatch map for tag-based routing.
	 */
	async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
		const gateway = await this.getGateway();
		const attachment = deserializeWsAttachment(ws);

		await handleWebSocketMessage(ws, message, {
			gateway,
			attachment,
			getSyncRules: () => this.configStore.getSyncRules(this.ctx.id.toString()),
			scheduleFlushAlarm: () => this.scheduleFlushAlarm(gateway),
			broadcastDeltas: (deltas, serverHlc, excludeClientId) =>
				this.broadcastDeltas(deltas, serverHlc, excludeClientId),
		});
	}

	/**
	 * Handle WebSocket close — clean up resources.
	 *
	 * Currently a no-op; resource cleanup will be added in Phase 2
	 * when per-client state is tracked.
	 */
	async webSocketClose(
		_ws: WebSocket,
		code: number,
		reason: string,
		_wasClean: boolean,
	): Promise<void> {
		logger.info("ws_close", { code, reason });
	}

	/**
	 * Handle WebSocket errors — log and clean up.
	 */
	async webSocketError(_ws: WebSocket, _error: unknown): Promise<void> {
		logger.error("ws_error", {});
	}

	// -----------------------------------------------------------------------
	// WebSocket broadcast
	// -----------------------------------------------------------------------

	/**
	 * Broadcast ingested deltas to all connected WebSocket clients except the sender.
	 *
	 * Delegates to the standalone {@link broadcastDeltasToSockets} function.
	 */
	private async broadcastDeltas(
		deltas: RowDelta[],
		serverHlc: HLCTimestamp,
		excludeClientId: string,
	): Promise<void> {
		const sockets = this.ctx.getWebSockets();
		const rules = await this.configStore.getSyncRules(this.ctx.id.toString());
		await broadcastDeltasToSockets(sockets, deltas, serverHlc, excludeClientId, rules);
	}

	// -----------------------------------------------------------------------
	// Alarm
	// -----------------------------------------------------------------------

	/**
	 * Alarm handler — triggered by the Durable Object runtime.
	 *
	 * Flushes the delta buffer to the lake via R2. On failure, reschedules
	 * with exponential backoff (1s, 2s, 4s, ... up to 30s). On success,
	 * reschedules immediately if the buffer still contains data, otherwise
	 * leaves rescheduling to the next push.
	 */
	async alarm(): Promise<void> {
		const gateway = await this.getGateway();
		const stats = gateway.bufferStats;

		if (stats.logSize === 0) {
			// Nothing to flush — next push will schedule an alarm
			return;
		}

		logger.info("flush_start", { bufferSize: stats.logSize });

		const startMs = Date.now();
		const result = await gateway.flush();

		if (!result.ok) {
			this.flushRetryCount++;
			const backoffMs = Math.min(
				BASE_RETRY_BACKOFF_MS * 2 ** (this.flushRetryCount - 1),
				MAX_RETRY_BACKOFF_MS,
			);

			logger.error("flush_failed", {
				error: result.error.message,
				retryCount: this.flushRetryCount,
				nextBackoffMs: backoffMs,
			});

			await this.ctx.storage.setAlarm(Date.now() + backoffMs);
			return;
		}

		// Flush succeeded — reset retry counter and log metrics
		logger.info("flush_success", {
			bufferSize: stats.logSize,
			bufferBytes: stats.byteSize,
			flushDurationMs: Date.now() - startMs,
		});
		this.flushRetryCount = 0;

		// If the buffer still has data (e.g. pushes arrived during flush),
		// reschedule immediately to drain it
		const postFlushStats = gateway.bufferStats;
		if (postFlushStats.logSize > 0) {
			await this.ctx.storage.setAlarm(Date.now());
		}
	}

	// -----------------------------------------------------------------------
	// Internal helpers
	// -----------------------------------------------------------------------

	/**
	 * Schedule a flush alarm after a successful push.
	 *
	 * If the buffer has exceeded its size threshold, schedules an immediate
	 * alarm. Otherwise, schedules a deferred alarm at `maxBufferAgeMs` to
	 * handle time-based flushing. The DO runtime coalesces alarms, so
	 * setting a nearer alarm replaces a further one automatically.
	 */
	private async scheduleFlushAlarm(gateway: SyncGateway): Promise<void> {
		if (gateway.shouldFlush()) {
			// Buffer thresholds exceeded — flush as soon as possible
			await this.ctx.storage.setAlarm(Date.now());
		} else {
			// Schedule a periodic flush for time-based draining
			await this.ctx.storage.setAlarm(Date.now() + DEFAULT_MAX_BUFFER_AGE_MS);
		}
	}
}

import { DurableObject } from "cloudflare:workers";
import type {
	HLCTimestamp,
	ResolvedClaims,
	SyncRulesConfig,
	SyncRulesContext,
	TableSchema,
} from "@lakesync/core";
import { bigintReplacer, bigintReviver, filterDeltas, validateSyncRules } from "@lakesync/core";
import { SyncGateway, type SyncPull, type SyncPush, type SyncResponse } from "@lakesync/gateway";
import {
	type CodecError,
	decodeSyncPull,
	decodeSyncPush,
	decodeSyncResponse,
	encodeSyncResponse,
} from "@lakesync/proto";
import type { Env } from "./env";
import { logger } from "./logger";
import { R2Adapter } from "./r2-adapter";

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
// Constants
// ---------------------------------------------------------------------------

/** Default maximum buffer size before triggering flush (4 MiB). */
const DEFAULT_MAX_BUFFER_BYTES = 4 * 1024 * 1024;

/** Default maximum buffer age before triggering flush (30 seconds). */
const DEFAULT_MAX_BUFFER_AGE_MS = 30_000;

/** Maximum backoff delay for flush retries (30 seconds). */
const MAX_RETRY_BACKOFF_MS = 30_000;

/** Base backoff delay for flush retries (1 second). */
const BASE_RETRY_BACKOFF_MS = 1_000;

/** Maximum push payload size (1 MiB). */
const MAX_PUSH_PAYLOAD_BYTES = 1_048_576;

/** Maximum number of deltas allowed in a single push. */
const MAX_DELTAS_PER_PUSH = 10_000;

/** Maximum number of deltas returned in a single pull. */
const MAX_PULL_LIMIT = 10_000;

/** Default number of deltas returned in a pull when no limit is specified. */
const DEFAULT_PULL_LIMIT = 100;

/** Allowed column types for schema validation. */
const VALID_COLUMN_TYPES = new Set(["string", "number", "boolean", "json", "null"]);

// ---------------------------------------------------------------------------
// SyncGatewayDO
// ---------------------------------------------------------------------------

/**
 * Durable Object that wraps a {@link SyncGateway} instance.
 *
 * Each DO instance manages one logical gateway's delta buffer and flush
 * lifecycle. It exposes three HTTP endpoints (`/push`, `/pull`, `/flush`)
 * and supports WebSocket connections for binary protobuf transport.
 *
 * The alarm-based flush mechanism ensures deltas are periodically written
 * to the lake via R2, with exponential backoff on failure.
 */
export class SyncGatewayDO extends DurableObject<Env> {
	/** Lazily initialised gateway instance. */
	private gateway: SyncGateway | null = null;

	/** Consecutive flush failure count for exponential backoff. Reset on success. */
	private flushRetryCount = 0;

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
		const tableSchema = await this.loadTableSchema();

		this.gateway = new SyncGateway(
			{
				gatewayId: this.ctx.id.toString(),
				maxBufferBytes: DEFAULT_MAX_BUFFER_BYTES,
				maxBufferAgeMs: DEFAULT_MAX_BUFFER_AGE_MS,
				flushFormat: tableSchema ? "parquet" : "json",
				tableSchema,
			},
			adapter,
		);

		return this.gateway;
	}

	/**
	 * Load a previously saved {@link TableSchema} from Durable Storage.
	 *
	 * @returns The stored schema, or `undefined` if none has been saved.
	 */
	private async loadTableSchema(): Promise<TableSchema | undefined> {
		const schema = await this.ctx.storage.get<TableSchema>("tableSchema");
		return schema ?? undefined;
	}

	/**
	 * Persist a {@link TableSchema} to Durable Storage and reset the
	 * cached gateway so the next {@link getGateway} call picks up the
	 * new schema and flush format.
	 */
	private async saveTableSchema(schema: TableSchema): Promise<void> {
		await this.ctx.storage.put("tableSchema", schema);
		// Reset gateway so next getGateway() picks up the new schema
		this.gateway = null;
	}

	/**
	 * Load sync rules from Durable Storage.
	 *
	 * @returns The stored sync rules, or `undefined` if none configured.
	 */
	private async loadSyncRules(): Promise<SyncRulesConfig | undefined> {
		const rules = await this.ctx.storage.get<SyncRulesConfig>("syncRules");
		return rules ?? undefined;
	}

	/**
	 * Persist sync rules to Durable Storage.
	 */
	private async saveSyncRules(rules: SyncRulesConfig): Promise<void> {
		await this.ctx.storage.put("syncRules", rules);
	}

	/**
	 * Build a SyncRulesContext from stored rules and forwarded JWT claims.
	 * Returns undefined if no sync rules are configured (backward compat).
	 */
	private async buildSyncRulesContext(
		request: Request | URL,
	): Promise<SyncRulesContext | undefined> {
		const rules = await this.loadSyncRules();
		if (!rules || rules.buckets.length === 0) {
			return undefined;
		}

		// Extract claims from X-Auth-Claims header (forwarded by the worker)
		let claims: ResolvedClaims = {};
		const claimsHeader = request instanceof Request ? request.headers.get("X-Auth-Claims") : null;

		if (claimsHeader) {
			try {
				claims = JSON.parse(claimsHeader) as ResolvedClaims;
			} catch {
				// Invalid claims header — use empty claims
			}
		}

		return { claims, rules };
	}

	/**
	 * Handle incoming HTTP requests routed from the Worker fetch handler.
	 *
	 * Supported routes:
	 * - `POST /push`  -- ingest client deltas
	 * - `GET  /pull`  -- retrieve deltas since a given HLC
	 * - `POST /flush` -- flush the buffer to the lake
	 * - `POST /admin/schema` -- save table schema
	 * - `POST /admin/sync-rules` -- save sync rules
	 * - `GET  /checkpoint` -- serve checkpoint for initial sync
	 * - Upgrade to WebSocket for binary protobuf transport
	 *
	 * @param request - The incoming HTTP request.
	 * @returns An HTTP response with a JSON body, or a WebSocket upgrade.
	 */
	async fetch(request: Request): Promise<Response> {
		// WebSocket upgrade
		const upgradeHeader = request.headers.get("Upgrade");
		if (upgradeHeader?.toLowerCase() === "websocket") {
			return this.handleWebSocketUpgrade(request);
		}

		const url = new URL(request.url);

		switch (url.pathname) {
			case "/push":
				return this.handlePush(request);
			case "/pull":
				return this.handlePull(url, request);
			case "/flush":
				return this.handleFlush();
			case "/admin/schema":
				return this.handleSaveSchema(request);
			case "/admin/sync-rules":
				return this.handleSaveSyncRules(request);
			case "/checkpoint":
				return this.handleCheckpoint(request);
			default:
				return errorResponse("Not found", 404);
		}
	}

	// -----------------------------------------------------------------------
	// HTTP route handlers
	// -----------------------------------------------------------------------

	/**
	 * Handle `POST /push` -- ingest client deltas via JSON.
	 *
	 * Expects a JSON body matching `{ clientId, deltas, lastSeenHlc }`.
	 * Returns the server HLC and accepted delta count on success.
	 * Schedules a flush alarm after each successful push.
	 */
	private async handlePush(request: Request): Promise<Response> {
		if (request.method !== "POST") {
			return errorResponse("Method not allowed", 405);
		}

		const contentLength = Number(request.headers.get("Content-Length") ?? "0");
		if (contentLength > MAX_PUSH_PAYLOAD_BYTES) {
			return errorResponse("Payload too large (max 1 MiB)", 413);
		}

		let body: SyncPush;
		try {
			const raw = await request.text();
			body = JSON.parse(raw, bigintReviver) as SyncPush;
		} catch {
			return errorResponse("Invalid JSON body", 400);
		}

		if (!body.clientId || !Array.isArray(body.deltas)) {
			return errorResponse("Missing required fields: clientId, deltas", 400);
		}

		const headerClientId = request.headers.get("X-Client-Id");
		if (headerClientId && body.clientId !== headerClientId) {
			return errorResponse(
				"Client ID mismatch: push clientId does not match authenticated identity",
				403,
			);
		}

		if (body.deltas.length > MAX_DELTAS_PER_PUSH) {
			return errorResponse("Too many deltas in a single push (max 10,000)", 400);
		}

		const gateway = await this.getGateway();

		logger.info("push", {
			clientId: body.clientId,
			deltaCount: body.deltas.length,
			contentLength,
		});

		const result = gateway.handlePush(body);

		if (!result.ok) {
			const err = result.error;
			logger.warn("push_error", { code: err.code, message: err.message });
			if (err.code === "CLOCK_DRIFT") {
				return errorResponse(err.message, 409);
			}
			if (err.code === "SCHEMA_MISMATCH") {
				return errorResponse(err.message, 422);
			}
			return errorResponse(err.message, 500);
		}

		await this.scheduleFlushAlarm(gateway);

		return jsonResponse(result.value);
	}

	/**
	 * Handle `GET /pull` -- retrieve deltas since a given HLC.
	 *
	 * When sync rules are configured, the pull is filtered by the client's
	 * JWT claims. The filtering is performed by the gateway's handlePull
	 * with a SyncRulesContext.
	 *
	 * Query parameters:
	 * - `since` (required) -- HLC timestamp as a decimal string
	 * - `limit` (optional) -- maximum number of deltas (default 100)
	 * - `clientId` (required) -- requesting client identifier
	 */
	private async handlePull(url: URL, request: Request): Promise<Response> {
		const sinceParam = url.searchParams.get("since");
		const clientId = url.searchParams.get("clientId");
		const limitParam = url.searchParams.get("limit");

		if (!sinceParam || !clientId) {
			return errorResponse("Missing required query params: since, clientId", 400);
		}

		let sinceHlc: HLCTimestamp;
		try {
			sinceHlc = BigInt(sinceParam) as HLCTimestamp;
		} catch {
			return errorResponse("Invalid 'since' parameter — must be a decimal integer", 400);
		}

		const rawLimit = limitParam ? Number.parseInt(limitParam, 10) : DEFAULT_PULL_LIMIT;
		if (Number.isNaN(rawLimit) || rawLimit < 1) {
			return errorResponse("Invalid 'limit' parameter — must be a positive integer", 400);
		}
		const maxDeltas = Math.min(rawLimit, MAX_PULL_LIMIT);

		const msg: SyncPull = { clientId, sinceHlc, maxDeltas };
		const gateway = await this.getGateway();
		const context = await this.buildSyncRulesContext(request);
		const result = gateway.handlePull(msg, context);

		// handlePull never fails (Result<SyncResponse, never>), but guard for safety
		if (!result.ok) {
			return errorResponse("Internal pull error", 500);
		}

		logger.info("pull", {
			clientId,
			deltaCount: result.value.deltas.length,
			sinceHlc: sinceHlc.toString(),
		});

		return jsonResponse(result.value);
	}

	/**
	 * Handle `POST /flush` -- flush the buffer to the lake adapter.
	 *
	 * Returns 200 on success, or the appropriate error status if the flush
	 * fails (e.g. already in progress, no adapter configured).
	 */
	private async handleFlush(): Promise<Response> {
		const gateway = await this.getGateway();
		const result = await gateway.flush();

		if (!result.ok) {
			return errorResponse(result.error.message, 500);
		}

		return jsonResponse({ flushed: true });
	}

	/**
	 * Handle `POST /admin/schema` -- save a table schema to Durable Storage.
	 *
	 * Expects a JSON body matching {@link TableSchema} with at least
	 * `table` (string) and `columns` (array) fields. Once saved, future
	 * flushes will use the Parquet format instead of JSON.
	 */
	private async handleSaveSchema(request: Request): Promise<Response> {
		if (request.method !== "POST") {
			return errorResponse("Method not allowed", 405);
		}

		let schema: TableSchema;
		try {
			schema = (await request.json()) as TableSchema;
		} catch {
			return errorResponse("Invalid JSON body", 400);
		}

		if (!schema.table || !Array.isArray(schema.columns)) {
			return errorResponse("Missing required fields: table, columns", 400);
		}

		for (const col of schema.columns) {
			if (typeof col.name !== "string" || col.name.length === 0) {
				return errorResponse("Each column must have a non-empty 'name' string", 400);
			}
			if (!VALID_COLUMN_TYPES.has(col.type)) {
				return errorResponse(
					`Invalid column type "${col.type}" for column "${col.name}". Allowed: string, number, boolean, json, null`,
					400,
				);
			}
		}

		await this.saveTableSchema(schema);
		logger.info("schema_saved", { table: schema.table });
		return jsonResponse({ saved: true });
	}

	/**
	 * Handle `POST /admin/sync-rules` -- save sync rules to Durable Storage.
	 *
	 * Validates the sync rules configuration before persisting.
	 */
	private async handleSaveSyncRules(request: Request): Promise<Response> {
		if (request.method !== "POST") {
			return errorResponse("Method not allowed", 405);
		}

		let config: unknown;
		try {
			config = await request.json();
		} catch {
			return errorResponse("Invalid JSON body", 400);
		}

		const validation = validateSyncRules(config);
		if (!validation.ok) {
			return errorResponse(validation.error.message, 400);
		}

		await this.saveSyncRules(config as SyncRulesConfig);
		logger.info("sync_rules_saved", {
			bucketCount: (config as SyncRulesConfig).buckets.length,
		});
		return jsonResponse({ saved: true });
	}

	/**
	 * Handle `GET /checkpoint` -- serve checkpoint chunks with serve-time filtering.
	 *
	 * Reads checkpoint manifest and chunks from R2, filters each chunk by
	 * the client's sync rules and JWT claims, and returns a single proto
	 * SyncResponse containing all matching deltas.
	 *
	 * Peak memory: one decoded chunk at a time.
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

		// Build sync rules context for filtering
		const context = await this.buildSyncRulesContext(request);

		// Process chunks one at a time, collect filtered deltas
		const allFilteredDeltas: import("@lakesync/core").RowDelta[] = [];

		for (const chunkName of manifest.chunks) {
			const chunkKey = `checkpoints/${gatewayId}/${chunkName}`;
			const chunkResult = await adapter.getObject(chunkKey);
			if (!chunkResult.ok) {
				continue; // Skip missing chunks
			}

			const decoded = decodeSyncResponse(chunkResult.value);
			if (!decoded.ok) {
				continue; // Skip corrupt chunks
			}

			const deltas = context ? filterDeltas(decoded.value.deltas, context) : decoded.value.deltas;

			allFilteredDeltas.push(...deltas);
		}

		// Encode all filtered deltas as a single SyncResponse
		const snapshotHlc = BigInt(manifest.snapshotHlc) as HLCTimestamp;
		const responsePayload = encodeSyncResponse({
			deltas: allFilteredDeltas,
			serverHlc: snapshotHlc,
			hasMore: false,
		});

		if (!responsePayload.ok) {
			return errorResponse("Failed to encode checkpoint response", 500);
		}

		logger.info("checkpoint", {
			chunkCount: manifest.chunks.length,
			deltaCount: allFilteredDeltas.length,
			filtered: !!context,
		});

		return new Response(responsePayload.value, {
			status: 200,
			headers: {
				"Content-Type": "application/octet-stream",
				"X-Checkpoint-Hlc": manifest.snapshotHlc,
			},
		});
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
		const claimsHeader = request.headers.get("X-Auth-Claims");
		const clientId = request.headers.get("X-Client-Id");
		let claims: ResolvedClaims = {};
		if (claimsHeader) {
			try {
				claims = JSON.parse(claimsHeader) as ResolvedClaims;
			} catch {
				// Invalid claims header — use empty claims
			}
		}

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
	 * Messages are expected as `ArrayBuffer` containing either a
	 * `SyncPush` or `SyncPull` protobuf. The first byte acts as a
	 * discriminator:
	 * - `0x01` = SyncPush
	 * - `0x02` = SyncPull
	 *
	 * Responses are sent back as binary protobuf `SyncResponse`.
	 */
	async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
		if (typeof message === "string") {
			// Text frames are not supported — close gracefully
			ws.close(1003, "Binary frames only");
			return;
		}

		const bytes = new Uint8Array(message);
		if (bytes.length < 2) {
			ws.close(1002, "Message too short");
			return;
		}

		const tag = bytes[0];
		const payload = bytes.subarray(1);

		const gateway = await this.getGateway();

		// Retrieve per-connection state stored during upgrade
		const attachment = (
			ws as unknown as {
				deserializeAttachment: () => { claims?: ResolvedClaims; clientId?: string } | null;
			}
		).deserializeAttachment();
		const storedClaims = attachment?.claims ?? {};
		const storedClientId = attachment?.clientId ?? null;

		logger.info("ws_message", { tag, clientId: storedClientId ?? "unknown" });

		if (tag === 0x01) {
			// SyncPush — validate payload size before decoding
			if (payload.byteLength > MAX_PUSH_PAYLOAD_BYTES) {
				ws.close(1009, "Payload too large (max 1 MiB)");
				return;
			}

			const decoded = decodeSyncPush(payload);
			if (!decoded.ok) {
				this.sendProtoError(ws, decoded.error);
				return;
			}

			// Verify client ID matches the authenticated identity
			if (storedClientId && decoded.value.clientId !== storedClientId) {
				ws.close(1008, "Client ID mismatch: push clientId does not match authenticated identity");
				return;
			}

			if (decoded.value.deltas.length > MAX_DELTAS_PER_PUSH) {
				ws.close(1008, "Too many deltas in a single push (max 10,000)");
				return;
			}

			const pushResult = gateway.handlePush(decoded.value);
			if (!pushResult.ok) {
				this.sendProtoError(ws, pushResult.error);
				return;
			}

			// Schedule flush alarm after successful push
			await this.scheduleFlushAlarm(gateway);

			// Build a SyncResponse echoing the server HLC and no deltas
			const response: SyncResponse = {
				deltas: [],
				serverHlc: pushResult.value.serverHlc,
				hasMore: false,
			};
			this.sendSyncResponse(ws, response);
		} else if (tag === 0x02) {
			// SyncPull — apply sync rules using stored claims
			const decoded = decodeSyncPull(payload);
			if (!decoded.ok) {
				this.sendProtoError(ws, decoded.error);
				return;
			}

			// Build sync rules context from stored claims
			const rules = await this.loadSyncRules();
			const context: SyncRulesContext | undefined =
				rules && rules.buckets.length > 0 ? { claims: storedClaims, rules } : undefined;

			const pullResult = gateway.handlePull(decoded.value, context);
			if (!pullResult.ok) {
				this.sendProtoError(ws, pullResult.error);
				return;
			}

			this.sendSyncResponse(ws, pullResult.value);
		} else {
			ws.close(1002, `Unknown message tag: 0x${tag!.toString(16).padStart(2, "0")}`);
		}
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
		logger.info("flush_success", { bufferSize: stats.logSize });
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

	/** Encode and send a `SyncResponse` over the WebSocket as binary. */
	private sendSyncResponse(ws: WebSocket, response: SyncResponse): void {
		const encoded = encodeSyncResponse(response);

		if (!encoded.ok) {
			ws.close(1011, "Failed to encode response");
			return;
		}

		ws.send(encoded.value);
	}

	/** Send an error frame over the WebSocket and close the connection. */
	private sendProtoError(ws: WebSocket, error: CodecError | { message: string }): void {
		ws.close(1008, error.message);
	}
}

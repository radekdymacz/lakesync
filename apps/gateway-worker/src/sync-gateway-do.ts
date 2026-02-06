import { DurableObject } from "cloudflare:workers";
import type { HLCTimestamp, TableSchema } from "@lakesync/core";
import { SyncGateway, type SyncPull, type SyncPush, type SyncResponse } from "@lakesync/gateway";
import {
	type CodecError,
	decodeSyncPull,
	decodeSyncPush,
	encodeSyncResponse,
} from "@lakesync/proto";
import type { Env } from "./env";
import { R2Adapter } from "./r2-adapter";

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

/**
 * BigInt-safe JSON replacer.
 *
 * Converts BigInt values to strings so they survive `JSON.stringify`,
 * which otherwise throws on BigInt.
 */
function bigintReplacer(_key: string, value: unknown): unknown {
	return typeof value === "bigint" ? value.toString() : value;
}

/**
 * BigInt-aware JSON reviver.
 *
 * Restores string-encoded HLC timestamps (fields ending in `Hlc` or `hlc`)
 * back to BigInt so they match the branded `HLCTimestamp` type.
 */
function bigintReviver(key: string, value: unknown): unknown {
	if (typeof value === "string" && /hlc$/i.test(key)) {
		return BigInt(value);
	}
	return value;
}

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
	 * Handle incoming HTTP requests routed from the Worker fetch handler.
	 *
	 * Supported routes:
	 * - `POST /push`  -- ingest client deltas
	 * - `GET  /pull`  -- retrieve deltas since a given HLC
	 * - `POST /flush` -- flush the buffer to the lake
	 * - Upgrade to WebSocket for binary protobuf transport
	 *
	 * @param request - The incoming HTTP request.
	 * @returns An HTTP response with a JSON body, or a WebSocket upgrade.
	 */
	async fetch(request: Request): Promise<Response> {
		// WebSocket upgrade
		const upgradeHeader = request.headers.get("Upgrade");
		if (upgradeHeader?.toLowerCase() === "websocket") {
			return this.handleWebSocketUpgrade();
		}

		const url = new URL(request.url);

		switch (url.pathname) {
			case "/push":
				return this.handlePush(request);
			case "/pull":
				return this.handlePull(url);
			case "/flush":
				return this.handleFlush();
			case "/admin/schema":
				return this.handleSaveSchema(request);
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

		const gateway = await this.getGateway();
		const result = gateway.handlePush(body);

		if (!result.ok) {
			const err = result.error;
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
	 * Query parameters:
	 * - `since` (required) -- HLC timestamp as a decimal string
	 * - `limit` (optional) -- maximum number of deltas (default 100)
	 * - `clientId` (required) -- requesting client identifier
	 */
	private async handlePull(url: URL): Promise<Response> {
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

		const maxDeltas = limitParam ? Number.parseInt(limitParam, 10) : 100;
		if (Number.isNaN(maxDeltas) || maxDeltas < 1) {
			return errorResponse("Invalid 'limit' parameter — must be a positive integer", 400);
		}

		const msg: SyncPull = { clientId, sinceHlc, maxDeltas };
		const gateway = await this.getGateway();
		const result = gateway.handlePull(msg);

		// handlePull never fails (Result<SyncResponse, never>), but guard for safety
		if (!result.ok) {
			return errorResponse("Internal pull error", 500);
		}

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

		await this.saveTableSchema(schema);
		return jsonResponse({ saved: true });
	}

	// -----------------------------------------------------------------------
	// WebSocket (hibernatable)
	// -----------------------------------------------------------------------

	/**
	 * Upgrade an HTTP request to a WebSocket connection.
	 *
	 * Uses the Durable Object's hibernatable WebSocket API so the DO can
	 * sleep between messages without losing the connection.
	 */
	private handleWebSocketUpgrade(): Response {
		const pair = new WebSocketPair();
		const [client, server] = [pair[0], pair[1]];

		this.ctx.acceptWebSocket(server);

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

		if (tag === 0x01) {
			// SyncPush
			const decoded = decodeSyncPush(payload);
			if (!decoded.ok) {
				this.sendProtoError(ws, decoded.error);
				return;
			}

			const pushMsg: SyncPush = {
				clientId: decoded.value.clientId,
				deltas: decoded.value.deltas,
				lastSeenHlc: decoded.value.lastSeenHlc,
			};

			const pushResult = gateway.handlePush(pushMsg);
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
			// SyncPull
			const decoded = decodeSyncPull(payload);
			if (!decoded.ok) {
				this.sendProtoError(ws, decoded.error);
				return;
			}

			const pullMsg: SyncPull = {
				clientId: decoded.value.clientId,
				sinceHlc: decoded.value.sinceHlc,
				maxDeltas: decoded.value.maxDeltas,
			};

			const pullResult = gateway.handlePull(pullMsg);
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
		wasClean: boolean,
	): Promise<void> {
		console.log(
			`[SyncGatewayDO] WebSocket closed: code=${code}, reason="${reason}", clean=${wasClean}`,
		);
	}

	/**
	 * Handle WebSocket errors — log and clean up.
	 */
	async webSocketError(_ws: WebSocket, error: unknown): Promise<void> {
		console.log(
			`[SyncGatewayDO] WebSocket error: ${error instanceof Error ? error.message : String(error)}`,
		);
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

		const start = Date.now();
		const result = await gateway.flush();
		const durationMs = Date.now() - start;

		if (!result.ok) {
			this.flushRetryCount++;
			const backoffMs = Math.min(
				BASE_RETRY_BACKOFF_MS * 2 ** (this.flushRetryCount - 1),
				MAX_RETRY_BACKOFF_MS,
			);

			console.log(
				`[SyncGatewayDO] Flush failed (attempt ${this.flushRetryCount}): ${result.error.message}. ` +
					`Retrying in ${backoffMs}ms.`,
			);

			await this.ctx.storage.setAlarm(Date.now() + backoffMs);
			return;
		}

		// Flush succeeded — reset retry counter and log metrics
		this.flushRetryCount = 0;

		console.log(
			`[SyncGatewayDO] Flush succeeded: ${stats.logSize} deltas, ` +
				`${stats.byteSize} bytes, ${durationMs}ms.`,
		);

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
		const encoded = encodeSyncResponse({
			deltas: response.deltas,
			serverHlc: response.serverHlc,
			hasMore: response.hasMore,
		});

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

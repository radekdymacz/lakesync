import type {
	HLCTimestamp,
	ResolvedClaims,
	RowDelta,
	SyncRulesConfig,
	SyncRulesContext,
} from "@lakesync/core";
import { filterDeltas } from "@lakesync/core";
import type { SyncResponse } from "@lakesync/gateway";
import { buildSyncRulesContext, MAX_PUSH_PAYLOAD_BYTES, type SyncGateway } from "@lakesync/gateway";
import {
	type CodecError,
	decodeSyncPull,
	decodeSyncPush,
	encodeBroadcastFrame,
	encodeSyncResponse,
} from "@lakesync/proto";
import { logger } from "./logger";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Per-connection attachment stored on the hibernatable WebSocket. */
export interface WsAttachment {
	readonly claims: ResolvedClaims;
	readonly clientId: string | null;
}

/** Dependencies injected into WebSocket message handlers. */
export interface WsHandlerDeps {
	readonly gateway: SyncGateway;
	readonly attachment: WsAttachment;
	readonly getSyncRules: () => Promise<SyncRulesConfig | undefined>;
	readonly scheduleFlushAlarm: () => Promise<void>;
	readonly broadcastDeltas: (
		deltas: RowDelta[],
		serverHlc: HLCTimestamp,
		excludeClientId: string,
	) => Promise<void>;
}

/** Result from a WebSocket message handler. */
type WsHandlerResult =
	| { readonly action: "send"; readonly data: Uint8Array }
	| { readonly action: "close"; readonly code: number; readonly reason: string }
	| { readonly action: "none" };

/** A handler for a single WebSocket message tag. */
type WebSocketMessageHandler = (
	payload: Uint8Array,
	deps: WsHandlerDeps,
) => Promise<WsHandlerResult>;

// ---------------------------------------------------------------------------
// Tag handlers
// ---------------------------------------------------------------------------

/** Handle tag 0x01 — SyncPush. */
async function handlePushTag(payload: Uint8Array, deps: WsHandlerDeps): Promise<WsHandlerResult> {
	if (payload.byteLength > MAX_PUSH_PAYLOAD_BYTES) {
		return { action: "close", code: 1009, reason: "Payload too large (max 1 MiB)" };
	}

	const decoded = decodeSyncPush(payload);
	if (!decoded.ok) {
		return protoErrorResult(decoded.error);
	}

	// Verify client ID matches the authenticated identity
	if (deps.attachment.clientId && decoded.value.clientId !== deps.attachment.clientId) {
		return {
			action: "close",
			code: 1008,
			reason: "Client ID mismatch: push clientId does not match authenticated identity",
		};
	}

	const pushResult = deps.gateway.handlePush(decoded.value);
	if (!pushResult.ok) {
		return protoErrorResult(pushResult.error);
	}

	// Schedule flush alarm after successful push
	await deps.scheduleFlushAlarm();

	// Build a SyncResponse echoing the server HLC and no deltas
	const response: SyncResponse = {
		deltas: [],
		serverHlc: pushResult.value.serverHlc,
		hasMore: false,
	};

	const encoded = encodeSyncResponse(response);
	if (!encoded.ok) {
		return { action: "close", code: 1011, reason: "Failed to encode response" };
	}

	// Broadcast ingested deltas to other connected WebSocket clients
	await deps.broadcastDeltas(
		pushResult.value.deltas,
		pushResult.value.serverHlc,
		decoded.value.clientId,
	);

	return { action: "send", data: encoded.value };
}

/** Handle tag 0x02 — SyncPull. */
async function handlePullTag(payload: Uint8Array, deps: WsHandlerDeps): Promise<WsHandlerResult> {
	const decoded = decodeSyncPull(payload);
	if (!decoded.ok) {
		return protoErrorResult(decoded.error);
	}

	// Build sync rules context from stored claims
	const rules = await deps.getSyncRules();
	const context = buildSyncRulesContext(rules, deps.attachment.claims);

	// WebSocket pull: source adapters not supported via proto (HTTP only)
	const pullResult = deps.gateway.pullFromBuffer(decoded.value, context);
	if (!pullResult.ok) {
		return protoErrorResult(pullResult.error);
	}

	const encoded = encodeSyncResponse(pullResult.value);
	if (!encoded.ok) {
		return { action: "close", code: 1011, reason: "Failed to encode response" };
	}

	return { action: "send", data: encoded.value };
}

// ---------------------------------------------------------------------------
// Dispatch map
// ---------------------------------------------------------------------------

/** Data-driven dispatch map: tag byte to handler function. */
const wsTagDispatch: Record<number, WebSocketMessageHandler> = {
	1: handlePushTag,
	2: handlePullTag,
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Handle an incoming WebSocket binary message.
 *
 * Parses the tag byte from the message, dispatches to the correct handler,
 * and applies the result (send, close, or no-op) to the WebSocket.
 */
export async function handleWebSocketMessage(
	ws: WebSocket,
	message: string | ArrayBuffer,
	deps: WsHandlerDeps,
): Promise<void> {
	if (typeof message === "string") {
		ws.close(1003, "Binary frames only");
		return;
	}

	const bytes = new Uint8Array(message);
	if (bytes.length < 2) {
		ws.close(1002, "Message too short");
		return;
	}

	const tag = bytes[0]!;
	const payload = bytes.subarray(1);

	logger.info("ws_message", { tag, clientId: deps.attachment.clientId ?? "unknown" });

	const handler = wsTagDispatch[tag];
	if (!handler) {
		ws.close(1002, `Unknown message tag: 0x${tag.toString(16).padStart(2, "0")}`);
		return;
	}

	const result = await handler(payload, deps);
	applyWsResult(ws, result);
}

/**
 * Broadcast deltas to all connected WebSocket clients except the sender.
 *
 * For each socket, applies sync rules filtering based on per-connection claims,
 * encodes a broadcast frame, and sends it. Errors on individual sockets are
 * silently caught (the socket may have closed).
 */
export async function broadcastDeltasToSockets(
	sockets: WebSocket[],
	deltas: RowDelta[],
	serverHlc: HLCTimestamp,
	excludeClientId: string,
	rules: SyncRulesConfig | undefined,
): Promise<void> {
	if (deltas.length === 0) return;
	if (sockets.length === 0) return;

	for (const ws of sockets) {
		try {
			const attachment = deserializeWsAttachment(ws);

			// Skip the sender
			if (attachment.clientId === excludeClientId) continue;

			// Apply sync rules filtering per-client
			let filtered = deltas;
			if (rules && rules.buckets.length > 0) {
				const context: SyncRulesContext = {
					claims: attachment.claims,
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
			// Socket may have closed — silently skip
		}
	}
}

/**
 * Deserialise the per-connection attachment from a hibernatable WebSocket.
 */
export function deserializeWsAttachment(ws: WebSocket): WsAttachment {
	const attachment = (
		ws as unknown as {
			deserializeAttachment: () => { claims?: ResolvedClaims; clientId?: string } | null;
		}
	).deserializeAttachment();

	return {
		claims: attachment?.claims ?? {},
		clientId: attachment?.clientId ?? null,
	};
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/** Build a close result from a proto/codec error. */
function protoErrorResult(error: CodecError | { message: string }): WsHandlerResult {
	return { action: "close", code: 1008, reason: error.message };
}

/** Apply a handler result to the WebSocket. */
function applyWsResult(ws: WebSocket, result: WsHandlerResult): void {
	switch (result.action) {
		case "send":
			ws.send(result.data);
			break;
		case "close":
			ws.close(result.code, result.reason);
			break;
		case "none":
			break;
	}
}

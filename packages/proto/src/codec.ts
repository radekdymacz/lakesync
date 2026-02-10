import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type {
	ActionErrorResult as CoreActionErrorResult,
	ActionPush as CoreActionPush,
	ActionResponse as CoreActionResponse,
	ActionResult as CoreActionResult,
	ColumnDelta as CoreColumnDelta,
	DeltaOp as CoreDeltaOp,
	RowDelta as CoreRowDelta,
	HLCTimestamp,
	Result,
} from "@lakesync/core";
import { Err, Ok } from "@lakesync/core";
import {
	ActionPushSchema,
	ActionResponseEntrySchema,
	ActionResponseSchema,
	ActionResultMsgSchema,
	ActionSchema,
	ColumnDeltaSchema,
	type ColumnDelta as ProtoColumnDelta,
	DeltaOp as ProtoDeltaOp,
	type RowDelta as ProtoRowDelta,
	RowDeltaSchema,
	SyncPullSchema,
	SyncPushSchema,
	SyncResponseSchema,
} from "./gen/lakesync_pb.js";

// ---------------------------------------------------------------------------
// Codec error
// ---------------------------------------------------------------------------

/** Error returned when encoding or decoding a protobuf message fails. */
export class CodecError extends Error {
	readonly code = "CODEC_ERROR";

	constructor(message: string, cause?: Error) {
		super(message);
		this.name = "CodecError";
		this.cause = cause;
	}
}

// ---------------------------------------------------------------------------
// Result helper
// ---------------------------------------------------------------------------

/**
 * Wrap a codec operation in a try/catch, returning a `Result`.
 * Centralises the error-wrapping logic shared by all encode/decode functions.
 */
function tryCodec<T>(label: string, fn: () => T): Result<T, CodecError> {
	try {
		return Ok(fn());
	} catch (err) {
		const cause = err instanceof Error ? err : new Error(String(err));
		return Err(new CodecError(label, cause));
	}
}

// ---------------------------------------------------------------------------
// Value serialisation helpers
// ---------------------------------------------------------------------------

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/** Encode an arbitrary serialisable column value to UTF-8 JSON bytes. */
function encodeValue(value: unknown): Uint8Array {
	return textEncoder.encode(JSON.stringify(value));
}

/** Decode UTF-8 JSON bytes back to a column value. */
function decodeValue(bytes: Uint8Array): unknown {
	return JSON.parse(textDecoder.decode(bytes));
}

// ---------------------------------------------------------------------------
// DeltaOp mapping
// ---------------------------------------------------------------------------

/** Map from core DeltaOp string to proto DeltaOp enum number. */
const CORE_OP_TO_PROTO: Record<CoreDeltaOp, ProtoDeltaOp> = {
	INSERT: ProtoDeltaOp.INSERT,
	UPDATE: ProtoDeltaOp.UPDATE,
	DELETE: ProtoDeltaOp.DELETE,
};

/** Map from proto DeltaOp enum number to core DeltaOp string. */
const PROTO_OP_TO_CORE: Record<ProtoDeltaOp, CoreDeltaOp | undefined> = {
	[ProtoDeltaOp.UNSPECIFIED]: undefined,
	[ProtoDeltaOp.INSERT]: "INSERT",
	[ProtoDeltaOp.UPDATE]: "UPDATE",
	[ProtoDeltaOp.DELETE]: "DELETE",
};

// ---------------------------------------------------------------------------
// Internal conversion helpers
// ---------------------------------------------------------------------------

/** Convert a core ColumnDelta to a proto ColumnDelta message. */
function coreColumnToProto(col: CoreColumnDelta): ProtoColumnDelta {
	return create(ColumnDeltaSchema, {
		column: col.column,
		value: encodeValue(col.value),
	});
}

/** Convert a proto ColumnDelta message to a core ColumnDelta. */
function protoColumnToCore(col: ProtoColumnDelta): CoreColumnDelta {
	return {
		column: col.column,
		value: decodeValue(col.value),
	};
}

/** Convert a core RowDelta to a proto RowDelta message. */
function coreRowToProto(delta: CoreRowDelta): ProtoRowDelta {
	return create(RowDeltaSchema, {
		op: CORE_OP_TO_PROTO[delta.op],
		table: delta.table,
		rowId: delta.rowId,
		columns: delta.columns.map(coreColumnToProto),
		hlc: delta.hlc as bigint,
		clientId: delta.clientId,
		deltaId: delta.deltaId,
	});
}

/**
 * Convert a proto RowDelta message to a core RowDelta.
 *
 * @throws {CodecError} If the proto DeltaOp is UNSPECIFIED.
 */
function protoRowToCore(delta: ProtoRowDelta): CoreRowDelta {
	const op = PROTO_OP_TO_CORE[delta.op];
	if (op === undefined) {
		throw new CodecError(`Unknown or unspecified DeltaOp: ${delta.op}`);
	}
	return {
		op,
		table: delta.table,
		rowId: delta.rowId,
		columns: delta.columns.map(protoColumnToCore),
		hlc: delta.hlc as HLCTimestamp,
		clientId: delta.clientId,
		deltaId: delta.deltaId,
	};
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** Shape of a SyncPush payload using core domain types. */
export interface SyncPushPayload {
	/** Client identifier. */
	clientId: string;
	/** Deltas to push to the server. */
	deltas: CoreRowDelta[];
	/** Last HLC timestamp seen by the client. */
	lastSeenHlc: HLCTimestamp;
}

/** Shape of a SyncPull request using core domain types. */
export interface SyncPullPayload {
	/** Client identifier. */
	clientId: string;
	/** Request deltas since this HLC timestamp. */
	sinceHlc: HLCTimestamp;
	/** Maximum number of deltas to return. */
	maxDeltas: number;
}

/** Shape of a SyncResponse using core domain types. */
export interface SyncResponsePayload {
	/** Deltas returned by the server. */
	deltas: CoreRowDelta[];
	/** Current server HLC timestamp. */
	serverHlc: HLCTimestamp;
	/** Whether more deltas are available. */
	hasMore: boolean;
}

// ---------------------------------------------------------------------------
// Wire tags for binary framing
// ---------------------------------------------------------------------------

/** Tag byte for SyncPush frames. */
export const TAG_SYNC_PUSH = 0x01;

/** Tag byte for SyncPull frames. */
export const TAG_SYNC_PULL = 0x02;

/** Tag byte for server-initiated broadcast frames. */
export const TAG_BROADCAST = 0x03;

/** Tag byte for ActionPush frames. */
export const TAG_ACTION_PUSH = 0x04;

/** Tag byte for ActionResponse frames. */
export const TAG_ACTION_RESPONSE = 0x05;

// ---------------------------------------------------------------------------
// Broadcast frame encode / decode
// ---------------------------------------------------------------------------

/**
 * Encode a broadcast frame: tag `0x03` + SyncResponse proto bytes.
 *
 * Used by the server to push deltas to connected WebSocket clients.
 *
 * @param response - The SyncResponse payload to broadcast.
 * @returns A `Result` containing the framed binary, or a `CodecError` on failure.
 */
export function encodeBroadcastFrame(
	response: SyncResponsePayload,
): Result<Uint8Array, CodecError> {
	const encoded = encodeSyncResponse(response);
	if (!encoded.ok) return encoded;
	const frame = new Uint8Array(1 + encoded.value.length);
	frame[0] = TAG_BROADCAST;
	frame.set(encoded.value, 1);
	return Ok(frame);
}

/**
 * Decode a broadcast frame: strip tag `0x03` and decode the SyncResponse.
 *
 * @param frame - The full framed binary (tag byte + proto payload).
 * @returns A `Result` containing the SyncResponse payload, or a `CodecError` on failure.
 */
export function decodeBroadcastFrame(frame: Uint8Array): Result<SyncResponsePayload, CodecError> {
	if (frame.length < 2) {
		return Err(new CodecError("Broadcast frame too short"));
	}
	if (frame[0] !== TAG_BROADCAST) {
		return Err(
			new CodecError(
				`Expected broadcast tag 0x03, got 0x${frame[0]!.toString(16).padStart(2, "0")}`,
			),
		);
	}
	return decodeSyncResponse(frame.subarray(1));
}

// ---------------------------------------------------------------------------
// RowDelta encode / decode
// ---------------------------------------------------------------------------

/**
 * Serialise a core RowDelta to protobuf binary.
 *
 * @param delta - The core RowDelta to serialise.
 * @returns A `Result` containing the binary bytes, or a `CodecError` on failure.
 */
export function encodeRowDelta(delta: CoreRowDelta): Result<Uint8Array, CodecError> {
	return tryCodec("Failed to encode RowDelta", () =>
		toBinary(RowDeltaSchema, coreRowToProto(delta)),
	);
}

/**
 * Deserialise protobuf binary to a core RowDelta.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the core RowDelta, or a `CodecError` on failure.
 */
export function decodeRowDelta(bytes: Uint8Array): Result<CoreRowDelta, CodecError> {
	return tryCodec("Failed to decode RowDelta", () =>
		protoRowToCore(fromBinary(RowDeltaSchema, bytes)),
	);
}

// ---------------------------------------------------------------------------
// SyncPush encode / decode
// ---------------------------------------------------------------------------

/**
 * Serialise a SyncPush payload to protobuf binary.
 *
 * @param push - The SyncPush payload containing client ID, deltas, and last seen HLC.
 * @returns A `Result` containing the binary bytes, or a `CodecError` on failure.
 */
export function encodeSyncPush(push: SyncPushPayload): Result<Uint8Array, CodecError> {
	return tryCodec("Failed to encode SyncPush", () => {
		const proto = create(SyncPushSchema, {
			clientId: push.clientId,
			deltas: push.deltas.map(coreRowToProto),
			lastSeenHlc: push.lastSeenHlc as bigint,
		});
		return toBinary(SyncPushSchema, proto);
	});
}

/**
 * Deserialise protobuf binary to a SyncPush payload.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the SyncPush payload, or a `CodecError` on failure.
 */
export function decodeSyncPush(bytes: Uint8Array): Result<SyncPushPayload, CodecError> {
	return tryCodec("Failed to decode SyncPush", () => {
		const proto = fromBinary(SyncPushSchema, bytes);
		return {
			clientId: proto.clientId,
			deltas: proto.deltas.map(protoRowToCore),
			lastSeenHlc: proto.lastSeenHlc as HLCTimestamp,
		};
	});
}

// ---------------------------------------------------------------------------
// SyncPull encode / decode
// ---------------------------------------------------------------------------

/**
 * Serialise a SyncPull request to protobuf binary.
 *
 * @param pull - The SyncPull payload containing client ID, since HLC, and max deltas.
 * @returns A `Result` containing the binary bytes, or a `CodecError` on failure.
 */
export function encodeSyncPull(pull: SyncPullPayload): Result<Uint8Array, CodecError> {
	return tryCodec("Failed to encode SyncPull", () => {
		const proto = create(SyncPullSchema, {
			clientId: pull.clientId,
			sinceHlc: pull.sinceHlc as bigint,
			maxDeltas: pull.maxDeltas,
		});
		return toBinary(SyncPullSchema, proto);
	});
}

/**
 * Deserialise protobuf binary to a SyncPull payload.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the SyncPull payload, or a `CodecError` on failure.
 */
export function decodeSyncPull(bytes: Uint8Array): Result<SyncPullPayload, CodecError> {
	return tryCodec("Failed to decode SyncPull", () => {
		const proto = fromBinary(SyncPullSchema, bytes);
		return {
			clientId: proto.clientId,
			sinceHlc: proto.sinceHlc as HLCTimestamp,
			maxDeltas: proto.maxDeltas,
		};
	});
}

// ---------------------------------------------------------------------------
// SyncResponse encode / decode
// ---------------------------------------------------------------------------

/**
 * Serialise a SyncResponse to protobuf binary.
 *
 * @param response - The SyncResponse payload containing deltas, server HLC, and has_more flag.
 * @returns A `Result` containing the binary bytes, or a `CodecError` on failure.
 */
export function encodeSyncResponse(response: SyncResponsePayload): Result<Uint8Array, CodecError> {
	return tryCodec("Failed to encode SyncResponse", () => {
		const proto = create(SyncResponseSchema, {
			deltas: response.deltas.map(coreRowToProto),
			serverHlc: response.serverHlc as bigint,
			hasMore: response.hasMore,
		});
		return toBinary(SyncResponseSchema, proto);
	});
}

/**
 * Deserialise protobuf binary to a SyncResponse payload.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the SyncResponse payload, or a `CodecError` on failure.
 */
export function decodeSyncResponse(bytes: Uint8Array): Result<SyncResponsePayload, CodecError> {
	return tryCodec("Failed to decode SyncResponse", () => {
		const proto = fromBinary(SyncResponseSchema, bytes);
		return {
			deltas: proto.deltas.map(protoRowToCore),
			serverHlc: proto.serverHlc as HLCTimestamp,
			hasMore: proto.hasMore,
		};
	});
}

// ---------------------------------------------------------------------------
// ActionPush encode / decode
// ---------------------------------------------------------------------------

/**
 * Serialise an ActionPush payload to protobuf binary.
 *
 * @param push - The ActionPush payload containing client ID and actions.
 * @returns A `Result` containing the binary bytes, or a `CodecError` on failure.
 */
export function encodeActionPush(push: CoreActionPush): Result<Uint8Array, CodecError> {
	return tryCodec("Failed to encode ActionPush", () => {
		const proto = create(ActionPushSchema, {
			clientId: push.clientId,
			actions: push.actions.map((a) =>
				create(ActionSchema, {
					actionId: a.actionId,
					clientId: a.clientId,
					hlc: a.hlc as bigint,
					connector: a.connector,
					actionType: a.actionType,
					params: encodeValue(a.params),
					idempotencyKey: a.idempotencyKey ?? "",
				}),
			),
		});
		return toBinary(ActionPushSchema, proto);
	});
}

/**
 * Deserialise protobuf binary to an ActionPush payload.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the ActionPush payload, or a `CodecError` on failure.
 */
export function decodeActionPush(bytes: Uint8Array): Result<CoreActionPush, CodecError> {
	return tryCodec("Failed to decode ActionPush", () => {
		const proto = fromBinary(ActionPushSchema, bytes);
		return {
			clientId: proto.clientId,
			actions: proto.actions.map((a) => ({
				actionId: a.actionId,
				clientId: a.clientId,
				hlc: a.hlc as HLCTimestamp,
				connector: a.connector,
				actionType: a.actionType,
				params: decodeValue(a.params) as Record<string, unknown>,
				...(a.idempotencyKey ? { idempotencyKey: a.idempotencyKey } : {}),
			})),
		};
	});
}

// ---------------------------------------------------------------------------
// ActionResponse encode / decode
// ---------------------------------------------------------------------------

/**
 * Serialise an ActionResponse to protobuf binary.
 *
 * @param response - The ActionResponse payload.
 * @returns A `Result` containing the binary bytes, or a `CodecError` on failure.
 */
export function encodeActionResponse(response: CoreActionResponse): Result<Uint8Array, CodecError> {
	return tryCodec("Failed to encode ActionResponse", () => {
		const entries = response.results.map((r) => {
			if ("data" in r && "serverHlc" in r) {
				// Success result
				const success = r as CoreActionResult;
				return create(ActionResponseEntrySchema, {
					result: {
						case: "success" as const,
						value: create(ActionResultMsgSchema, {
							actionId: success.actionId,
							data: encodeValue(success.data),
							serverHlc: success.serverHlc as bigint,
						}),
					},
				});
			}
			// Error result
			const error = r as CoreActionErrorResult;
			return create(ActionResponseEntrySchema, {
				result: {
					case: "error" as const,
					value: {
						actionId: error.actionId,
						code: error.code,
						message: error.message,
						retryable: error.retryable,
					},
				},
			});
		});

		const proto = create(ActionResponseSchema, {
			results: entries,
			serverHlc: response.serverHlc as bigint,
		});
		return toBinary(ActionResponseSchema, proto);
	});
}

/**
 * Deserialise protobuf binary to an ActionResponse payload.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the ActionResponse payload, or a `CodecError` on failure.
 */
export function decodeActionResponse(bytes: Uint8Array): Result<CoreActionResponse, CodecError> {
	return tryCodec("Failed to decode ActionResponse", () => {
		const proto = fromBinary(ActionResponseSchema, bytes);
		const results: Array<CoreActionResult | CoreActionErrorResult> = proto.results.map((entry) => {
			if (entry.result.case === "success") {
				const s = entry.result.value;
				return {
					actionId: s.actionId,
					data: decodeValue(s.data) as Record<string, unknown>,
					serverHlc: s.serverHlc as HLCTimestamp,
				};
			}
			if (entry.result.case === "error") {
				const e = entry.result.value;
				return {
					actionId: e.actionId,
					code: e.code,
					message: e.message,
					retryable: e.retryable,
				};
			}
			throw new CodecError("ActionResponseEntry has no result");
		});

		return {
			results,
			serverHlc: proto.serverHlc as HLCTimestamp,
		};
	});
}

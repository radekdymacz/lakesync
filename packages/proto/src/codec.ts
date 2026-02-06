import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type {
	ColumnDelta as CoreColumnDelta,
	DeltaOp as CoreDeltaOp,
	HLCTimestamp,
	RowDelta as CoreRowDelta,
} from "@lakesync/core";
import { type Err, type Ok, type Result } from "@lakesync/core";
import {
	type ColumnDelta as ProtoColumnDelta,
	ColumnDeltaSchema,
	DeltaOp as ProtoDeltaOp,
	type RowDelta as ProtoRowDelta,
	RowDeltaSchema,
	type SyncPull as ProtoSyncPull,
	SyncPullSchema,
	type SyncPush as ProtoSyncPush,
	SyncPushSchema,
	type SyncResponse as ProtoSyncResponse,
	SyncResponseSchema,
} from "./gen/lakesync_pb.js";

// ---------------------------------------------------------------------------
// Value serialisation helpers
// ---------------------------------------------------------------------------

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Encode an arbitrary serialisable column value to UTF-8 JSON bytes.
 *
 * @param value - The column value to serialise (must be JSON-safe).
 * @returns UTF-8 encoded JSON bytes.
 */
function encodeValue(value: unknown): Uint8Array {
	return textEncoder.encode(JSON.stringify(value));
}

/**
 * Decode UTF-8 JSON bytes back to a column value.
 *
 * @param bytes - The UTF-8 encoded JSON bytes.
 * @returns The deserialised value.
 */
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
// Codec error
// ---------------------------------------------------------------------------

/** Error returned when decoding a protobuf message fails. */
export class CodecError extends Error {
	readonly code = "CODEC_ERROR";

	constructor(message: string, cause?: Error) {
		super(message);
		this.name = "CodecError";
		this.cause = cause;
	}
}

// ---------------------------------------------------------------------------
// Internal conversion helpers
// ---------------------------------------------------------------------------

/**
 * Convert a core ColumnDelta to a proto ColumnDelta message.
 */
function coreColumnToProto(col: CoreColumnDelta): ProtoColumnDelta {
	return create(ColumnDeltaSchema, {
		column: col.column,
		value: encodeValue(col.value),
	});
}

/**
 * Convert a proto ColumnDelta message to a core ColumnDelta.
 */
function protoColumnToCore(col: ProtoColumnDelta): CoreColumnDelta {
	return {
		column: col.column,
		value: decodeValue(col.value),
	};
}

/**
 * Convert a core RowDelta to a proto RowDelta message.
 */
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
// RowDelta encode / decode
// ---------------------------------------------------------------------------

/**
 * Serialise a core RowDelta to protobuf binary.
 *
 * @param delta - The core RowDelta to serialise.
 * @returns A `Result` containing the binary bytes, or a `CodecError` on failure.
 */
export function encodeRowDelta(delta: CoreRowDelta): Result<Uint8Array, CodecError> {
	try {
		const proto = coreRowToProto(delta);
		return { ok: true, value: toBinary(RowDeltaSchema, proto) };
	} catch (err) {
		return {
			ok: false,
			error: new CodecError(
				"Failed to encode RowDelta",
				err instanceof Error ? err : new Error(String(err)),
			),
		};
	}
}

/**
 * Deserialise protobuf binary to a core RowDelta.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the core RowDelta, or a `CodecError` on failure.
 */
export function decodeRowDelta(bytes: Uint8Array): Result<CoreRowDelta, CodecError> {
	try {
		const proto = fromBinary(RowDeltaSchema, bytes);
		return { ok: true, value: protoRowToCore(proto) };
	} catch (err) {
		return {
			ok: false,
			error: new CodecError(
				"Failed to decode RowDelta",
				err instanceof Error ? err : new Error(String(err)),
			),
		};
	}
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
	try {
		const proto = create(SyncPushSchema, {
			clientId: push.clientId,
			deltas: push.deltas.map(coreRowToProto),
			lastSeenHlc: push.lastSeenHlc as bigint,
		});
		return { ok: true, value: toBinary(SyncPushSchema, proto) };
	} catch (err) {
		return {
			ok: false,
			error: new CodecError(
				"Failed to encode SyncPush",
				err instanceof Error ? err : new Error(String(err)),
			),
		};
	}
}

/**
 * Deserialise protobuf binary to a SyncPush payload.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the SyncPush payload, or a `CodecError` on failure.
 */
export function decodeSyncPush(bytes: Uint8Array): Result<SyncPushPayload, CodecError> {
	try {
		const proto = fromBinary(SyncPushSchema, bytes);
		return {
			ok: true,
			value: {
				clientId: proto.clientId,
				deltas: proto.deltas.map(protoRowToCore),
				lastSeenHlc: proto.lastSeenHlc as HLCTimestamp,
			},
		};
	} catch (err) {
		return {
			ok: false,
			error: new CodecError(
				"Failed to decode SyncPush",
				err instanceof Error ? err : new Error(String(err)),
			),
		};
	}
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
	try {
		const proto = create(SyncPullSchema, {
			clientId: pull.clientId,
			sinceHlc: pull.sinceHlc as bigint,
			maxDeltas: pull.maxDeltas,
		});
		return { ok: true, value: toBinary(SyncPullSchema, proto) };
	} catch (err) {
		return {
			ok: false,
			error: new CodecError(
				"Failed to encode SyncPull",
				err instanceof Error ? err : new Error(String(err)),
			),
		};
	}
}

/**
 * Deserialise protobuf binary to a SyncPull payload.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the SyncPull payload, or a `CodecError` on failure.
 */
export function decodeSyncPull(bytes: Uint8Array): Result<SyncPullPayload, CodecError> {
	try {
		const proto = fromBinary(SyncPullSchema, bytes);
		return {
			ok: true,
			value: {
				clientId: proto.clientId,
				sinceHlc: proto.sinceHlc as HLCTimestamp,
				maxDeltas: proto.maxDeltas,
			},
		};
	} catch (err) {
		return {
			ok: false,
			error: new CodecError(
				"Failed to decode SyncPull",
				err instanceof Error ? err : new Error(String(err)),
			),
		};
	}
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
export function encodeSyncResponse(
	response: SyncResponsePayload,
): Result<Uint8Array, CodecError> {
	try {
		const proto = create(SyncResponseSchema, {
			deltas: response.deltas.map(coreRowToProto),
			serverHlc: response.serverHlc as bigint,
			hasMore: response.hasMore,
		});
		return { ok: true, value: toBinary(SyncResponseSchema, proto) };
	} catch (err) {
		return {
			ok: false,
			error: new CodecError(
				"Failed to encode SyncResponse",
				err instanceof Error ? err : new Error(String(err)),
			),
		};
	}
}

/**
 * Deserialise protobuf binary to a SyncResponse payload.
 *
 * @param bytes - The protobuf binary to deserialise.
 * @returns A `Result` containing the SyncResponse payload, or a `CodecError` on failure.
 */
export function decodeSyncResponse(
	bytes: Uint8Array,
): Result<SyncResponsePayload, CodecError> {
	try {
		const proto = fromBinary(SyncResponseSchema, bytes);
		return {
			ok: true,
			value: {
				deltas: proto.deltas.map(protoRowToCore),
				serverHlc: proto.serverHlc as HLCTimestamp,
				hasMore: proto.hasMore,
			},
		};
	} catch (err) {
		return {
			ok: false,
			error: new CodecError(
				"Failed to decode SyncResponse",
				err instanceof Error ? err : new Error(String(err)),
			),
		};
	}
}

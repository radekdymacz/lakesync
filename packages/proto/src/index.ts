export {
	encodeRowDelta,
	decodeRowDelta,
	encodeSyncPush,
	decodeSyncPush,
	encodeSyncPull,
	decodeSyncPull,
	encodeSyncResponse,
	decodeSyncResponse,
	CodecError,
} from "./codec.js";

export type { SyncPushPayload, SyncPullPayload, SyncResponsePayload } from "./codec.js";

export {
	DeltaOp as ProtoDeltaOp,
	ColumnDeltaSchema,
	RowDeltaSchema,
	SyncPushSchema,
	SyncPullSchema,
	SyncResponseSchema,
	DeltaOpSchema,
} from "./gen/lakesync_pb.js";

export type {
	ColumnDelta as ProtoColumnDelta,
	RowDelta as ProtoRowDelta,
	SyncPush as ProtoSyncPush,
	SyncPull as ProtoSyncPull,
	SyncResponse as ProtoSyncResponse,
} from "./gen/lakesync_pb.js";

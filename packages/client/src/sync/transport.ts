import type {
	HLCTimestamp,
	LakeSyncError,
	Result,
	RowDelta,
	SyncPull,
	SyncPush,
	SyncResponse,
} from "@lakesync/core";

/** Response from a checkpoint download */
export interface CheckpointResponse {
	/** All deltas from the checkpoint (filtered by server) */
	deltas: RowDelta[];
	/** Snapshot HLC marking the point-in-time of this checkpoint */
	snapshotHlc: HLCTimestamp;
}

/** Abstract transport layer for communicating with a remote sync gateway */
export interface SyncTransport {
	/** Push local deltas to the gateway */
	push(
		msg: SyncPush,
	): Promise<Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>>;
	/** Pull remote deltas from the gateway */
	pull(msg: SyncPull): Promise<Result<SyncResponse, LakeSyncError>>;
	/** Download checkpoint for initial sync. Returns null if no checkpoint available. */
	checkpoint?(): Promise<Result<CheckpointResponse | null, LakeSyncError>>;

	/** Whether this transport supports real-time server push. */
	readonly supportsRealtime?: boolean;
	/** Register callback for server-initiated broadcasts. */
	onBroadcast?(callback: (deltas: RowDelta[], serverHlc: HLCTimestamp) => void): void;
	/** Connect persistent transport (e.g. open WebSocket). */
	connect?(): void;
	/** Disconnect persistent transport (e.g. close WebSocket). */
	disconnect?(): void;
}

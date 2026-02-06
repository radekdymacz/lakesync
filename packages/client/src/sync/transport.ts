import type {
	HLCTimestamp,
	LakeSyncError,
	Result,
	SyncPull,
	SyncPush,
	SyncResponse,
} from "@lakesync/core";

/** Abstract transport layer for communicating with a remote sync gateway */
export interface SyncTransport {
	/** Push local deltas to the gateway */
	push(
		msg: SyncPush,
	): Promise<Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>>;
	/** Pull remote deltas from the gateway */
	pull(msg: SyncPull): Promise<Result<SyncResponse, LakeSyncError>>;
}

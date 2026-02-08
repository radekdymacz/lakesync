import type {
	HLCTimestamp,
	LakeSyncError,
	Result,
	SyncPull,
	SyncPush,
	SyncResponse,
} from "@lakesync/core";
import { Ok } from "@lakesync/core";
import type { CheckpointResponse, SyncTransport } from "./transport";

/**
 * Gateway-like interface used by LocalTransport.
 *
 * Matches the shape of SyncGateway's push/pull methods without
 * requiring a direct dependency on `@lakesync/gateway`.
 */
export interface LocalGateway {
	/** Handle an incoming push from a client */
	handlePush(msg: SyncPush): Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>;
	/** Handle a pull request from a client */
	handlePull(msg: SyncPull): Result<SyncResponse, LakeSyncError>;
}

/**
 * In-process transport that wraps a local SyncGateway instance.
 *
 * Useful for testing and single-tab offline demos where the client
 * and gateway run in the same process.
 */
export class LocalTransport implements SyncTransport {
	constructor(private readonly gateway: LocalGateway) {}

	/** Push local deltas to the in-process gateway */
	async push(
		msg: SyncPush,
	): Promise<Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>> {
		return this.gateway.handlePush(msg);
	}

	/** Pull remote deltas from the in-process gateway */
	async pull(msg: SyncPull): Promise<Result<SyncResponse, LakeSyncError>> {
		const result = this.gateway.handlePull(msg);
		return result instanceof Promise ? result : result;
	}

	/** Local transport has no checkpoint — returns null */
	async checkpoint(): Promise<Result<CheckpointResponse | null, LakeSyncError>> {
		return Ok(null);
	}
}

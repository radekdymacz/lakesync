import type {
	ActionDiscovery,
	ActionPush,
	ActionResponse,
	ConnectorDescriptor,
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

/** Core sync transport — push and pull deltas. */
export interface SyncTransport {
	/** Push local deltas to the gateway */
	push(
		msg: SyncPush,
	): Promise<Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>>;
	/** Pull remote deltas from the gateway */
	pull(msg: SyncPull): Promise<Result<SyncResponse, LakeSyncError>>;
}

/** Transport that supports checkpoint downloads for initial sync. */
export interface CheckpointTransport {
	/** Download checkpoint for initial sync. Returns null if no checkpoint available. */
	checkpoint(): Promise<Result<CheckpointResponse | null, LakeSyncError>>;
}

/** Transport that supports real-time server-initiated broadcasts. */
export interface RealtimeTransport {
	/** Whether this transport supports real-time server push. */
	readonly supportsRealtime: boolean;
	/** Register callback for server-initiated broadcasts. */
	onBroadcast(callback: (deltas: RowDelta[], serverHlc: HLCTimestamp) => void): void;
	/** Connect persistent transport (e.g. open WebSocket). */
	connect(): void;
	/** Disconnect persistent transport (e.g. close WebSocket). */
	disconnect(): void;
}

/** Transport that supports imperative action execution. */
export interface ActionTransport {
	/** Execute imperative actions against external systems via the gateway. */
	executeAction(msg: ActionPush): Promise<Result<ActionResponse, LakeSyncError>>;
	/** Discover available connectors and their supported action types. */
	describeActions(): Promise<Result<ActionDiscovery, LakeSyncError>>;
	/** List available connector types and their configuration schemas. */
	listConnectorTypes(): Promise<Result<ConnectorDescriptor[], LakeSyncError>>;
}

/**
 * Union type combining the core sync transport with optional capabilities.
 *
 * Transports must implement push/pull. Checkpoint, real-time, and action
 * capabilities are opt-in via the respective interfaces.
 */
export type TransportWithCapabilities = SyncTransport &
	Partial<CheckpointTransport> &
	Partial<RealtimeTransport> &
	Partial<ActionTransport>;

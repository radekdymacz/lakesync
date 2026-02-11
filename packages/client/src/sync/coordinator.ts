import {
	type ActionDiscovery,
	type ActionErrorResult,
	type ActionResult,
	type ConnectorDescriptor,
	HLC,
	type HLCTimestamp,
	type LakeSyncError,
	LWWResolver,
	type Result,
	type RowDelta,
} from "@lakesync/core";
import type { LocalDB } from "../db/local-db";
import type { ActionQueue } from "../queue/action-types";
import { IDBQueue } from "../queue/idb-queue";
import type { SyncQueue } from "../queue/types";
import { ActionProcessor } from "./action-processor";
import { applyRemoteDeltas } from "./applier";
import { AutoSyncScheduler } from "./auto-sync";
import { SyncTracker } from "./tracker";
import type { SyncTransport } from "./transport";

/** Controls which operations syncOnce() / startAutoSync() performs */
export type SyncMode = "full" | "pushOnly" | "pullOnly";

/** Readable snapshot of the coordinator's current sync state. */
export interface SyncState {
	/** Whether a sync cycle is currently in progress. */
	syncing: boolean;
	/** Last successful sync time, or null if never synced. */
	lastSyncTime: Date | null;
	/** HLC timestamp of the last successfully synced delta. */
	lastSyncedHlc: HLCTimestamp;
}

/** Events emitted by SyncCoordinator */
export interface SyncEvents {
	/** Fired after remote deltas are applied locally. Count is the number of deltas applied. */
	onChange: (count: number) => void;
	/** Fired when a sync cycle (push + pull) begins. */
	onSyncStart: () => void;
	/** Fired after a successful sync cycle (push + pull) completes. */
	onSyncComplete: () => void;
	/** Fired when a sync error occurs. */
	onError: (error: Error) => void;
	/** Fired when an action completes (success or non-retryable failure). */
	onActionComplete: (actionId: string, result: ActionResult | ActionErrorResult) => void;
}

/** Optional configuration for dependency injection (useful for testing) */
export interface SyncCoordinatorConfig {
	/** Sync queue implementation. Defaults to IDBQueue. */
	queue?: SyncQueue;
	/** HLC instance. Defaults to a new HLC(). */
	hlc?: HLC;
	/** Client identifier. Defaults to a random UUID. */
	clientId?: string;
	/** Maximum retries before dead-lettering an entry. Defaults to 10. */
	maxRetries?: number;
	/** Sync mode. Defaults to "full" (push + pull). */
	syncMode?: SyncMode;
	/** Auto-sync interval in milliseconds. Defaults to 10000 (10 seconds). */
	autoSyncIntervalMs?: number;
	/** Polling interval when realtime transport is active (heartbeat). Defaults to 60000 (60 seconds). */
	realtimeHeartbeatMs?: number;
	/** Action queue for imperative command execution. */
	actionQueue?: ActionQueue;
	/** Maximum retries for actions before dead-lettering. Defaults to 5. */
	maxActionRetries?: number;
}

/** Auto-sync interval in milliseconds (every 10 seconds) */
const AUTO_SYNC_INTERVAL_MS = 10_000;

/** Auto-sync heartbeat interval when realtime is active (every 60 seconds) */
const REALTIME_HEARTBEAT_MS = 60_000;

/**
 * Coordinates local mutations (via SyncTracker) with gateway push/pull.
 *
 * Uses a {@link SyncTransport} abstraction to communicate with the gateway,
 * allowing both in-process (LocalTransport) and remote (HttpTransport) usage.
 *
 * Delegates auto-sync scheduling to {@link AutoSyncScheduler} and action
 * processing to {@link ActionProcessor}.
 */
export class SyncCoordinator {
	readonly tracker: SyncTracker;
	private readonly queue: SyncQueue;
	private readonly hlc: HLC;
	private readonly transport: SyncTransport;
	private readonly db: LocalDB;
	private readonly resolver = new LWWResolver();
	private readonly _clientId: string;
	private readonly maxRetries: number;
	private readonly syncMode: SyncMode;
	private lastSyncedHlc = HLC.encode(0, 0);
	private _lastSyncTime: Date | null = null;
	private syncing = false;
	private readonly autoSyncScheduler: AutoSyncScheduler;
	private readonly actionProcessor: ActionProcessor | null;
	private listeners: { [K in keyof SyncEvents]: Array<SyncEvents[K]> } = {
		onChange: [],
		onSyncStart: [],
		onSyncComplete: [],
		onError: [],
		onActionComplete: [],
	};

	constructor(db: LocalDB, transport: SyncTransport, config?: SyncCoordinatorConfig) {
		this.db = db;
		this.transport = transport;
		this.hlc = config?.hlc ?? new HLC();
		this.queue = config?.queue ?? new IDBQueue();
		this._clientId = config?.clientId ?? `client-${crypto.randomUUID()}`;
		this.maxRetries = config?.maxRetries ?? 10;
		this.syncMode = config?.syncMode ?? "full";
		this.tracker = new SyncTracker(db, this.queue, this.hlc, this._clientId);

		// Compute auto-sync interval
		const autoSyncIntervalMs = config?.autoSyncIntervalMs ?? AUTO_SYNC_INTERVAL_MS;
		const realtimeHeartbeatMs = config?.realtimeHeartbeatMs ?? REALTIME_HEARTBEAT_MS;
		const intervalMs = transport.supportsRealtime ? realtimeHeartbeatMs : autoSyncIntervalMs;

		// Initialise composed modules
		this.autoSyncScheduler = new AutoSyncScheduler(() => this.syncOnce(), intervalMs);

		if (config?.actionQueue) {
			this.actionProcessor = new ActionProcessor({
				actionQueue: config.actionQueue,
				transport,
				clientId: this._clientId,
				hlc: this.hlc,
				maxRetries: config?.maxActionRetries ?? 5,
			});
			this.actionProcessor.setOnComplete((actionId, result) => {
				this.emit("onActionComplete", actionId, result);
			});
		} else {
			this.actionProcessor = null;
		}

		// Register broadcast handler for realtime transports
		if (this.transport.onBroadcast) {
			this.transport.onBroadcast((deltas, serverHlc) => {
				void this.handleBroadcast(deltas, serverHlc);
			});
		}
	}

	/** Register an event listener */
	on<K extends keyof SyncEvents>(event: K, listener: SyncEvents[K]): void {
		this.listeners[event].push(listener);
	}

	/** Remove an event listener */
	off<K extends keyof SyncEvents>(event: K, listener: SyncEvents[K]): void {
		const arr = this.listeners[event];
		const idx = arr.indexOf(listener);
		if (idx !== -1) arr.splice(idx, 1);
	}

	private emit<K extends keyof SyncEvents>(event: K, ...args: Parameters<SyncEvents[K]>): void {
		for (const fn of this.listeners[event]) {
			try {
				(fn as (...a: Parameters<SyncEvents[K]>) => void)(...args);
			} catch {
				// Swallow listener errors to avoid breaking sync
			}
		}
	}

	/** Readable snapshot of the current sync state. */
	get state(): SyncState {
		return {
			syncing: this.syncing,
			lastSyncTime: this._lastSyncTime,
			lastSyncedHlc: this.lastSyncedHlc,
		};
	}

	/** Push pending deltas to the gateway via the transport */
	async pushToGateway(): Promise<void> {
		const peekResult = await this.queue.peek(100);
		if (!peekResult.ok || peekResult.value.length === 0) return;

		// Dead-letter entries that exceeded max retries
		const deadLettered = peekResult.value.filter((e) => e.retryCount >= this.maxRetries);
		const entries = peekResult.value.filter((e) => e.retryCount < this.maxRetries);

		if (deadLettered.length > 0) {
			console.warn(
				`[SyncCoordinator] Dead-lettering ${deadLettered.length} entries after ${this.maxRetries} retries`,
			);
			await this.queue.ack(deadLettered.map((e) => e.id));
			this.emit(
				"onError",
				new Error(`Dead-lettered ${deadLettered.length} entries after ${this.maxRetries} retries`),
			);
		}

		if (entries.length === 0) return;

		const ids = entries.map((e) => e.id);
		await this.queue.markSending(ids);

		const pushResult = await this.transport.push({
			clientId: this._clientId,
			deltas: entries.map((e) => e.delta),
			lastSeenHlc: this.hlc.now(),
		});

		if (pushResult.ok) {
			await this.queue.ack(ids);
			this.lastSyncedHlc = pushResult.value.serverHlc;
			this._lastSyncTime = new Date();
		} else {
			await this.queue.nack(ids);
		}
	}

	/**
	 * Pull deltas from a named adapter source.
	 *
	 * Convenience wrapper around {@link pullFromGateway} that passes the
	 * `source` field through to the gateway, triggering an adapter-sourced
	 * pull instead of a buffer pull.
	 */
	async pullFrom(source: string): Promise<number> {
		return this.pullFromGateway(source);
	}

	/** Pull remote deltas from the gateway and apply them */
	async pullFromGateway(source?: string): Promise<number> {
		const pullResult = await this.transport.pull({
			clientId: this._clientId,
			sinceHlc: this.lastSyncedHlc,
			maxDeltas: 1000,
			source,
		});

		if (!pullResult.ok || pullResult.value.deltas.length === 0) return 0;

		const { deltas, serverHlc } = pullResult.value;
		const applyResult = await applyRemoteDeltas(this.db, deltas, this.resolver, this.queue);

		if (applyResult.ok) {
			this.lastSyncedHlc = serverHlc;
			this._lastSyncTime = new Date();
			if (applyResult.value > 0) {
				this.emit("onChange", applyResult.value);
			}
			return applyResult.value;
		}
		return 0;
	}

	/**
	 * Handle a server-initiated broadcast of deltas.
	 *
	 * Applies the deltas using the same conflict resolution and idempotency
	 * logic as a regular pull. Advances `lastSyncedHlc` and emits `onChange`.
	 */
	private async handleBroadcast(deltas: RowDelta[], serverHlc: HLCTimestamp): Promise<void> {
		if (deltas.length === 0) return;

		try {
			const applyResult = await applyRemoteDeltas(this.db, deltas, this.resolver, this.queue);

			if (applyResult.ok) {
				if (HLC.compare(serverHlc, this.lastSyncedHlc) > 0) {
					this.lastSyncedHlc = serverHlc;
				}
				this._lastSyncTime = new Date();
				if (applyResult.value > 0) {
					this.emit("onChange", applyResult.value);
				}
			}
		} catch (err) {
			this.emit("onError", err instanceof Error ? err : new Error(String(err)));
		}
	}

	/** Get the queue depth */
	async queueDepth(): Promise<number> {
		const result = await this.queue.depth();
		return result.ok ? result.value : 0;
	}

	/** Get the client identifier */
	get clientId(): string {
		return this._clientId;
	}

	/** Get the last successful sync time, or null if never synced */
	get lastSyncTime(): Date | null {
		return this._lastSyncTime;
	}

	/**
	 * Start auto-sync: periodic interval + visibility change handler.
	 * Synchronises (push + pull) on tab focus and every N seconds.
	 */
	startAutoSync(): void {
		this.transport.connect?.();
		this.autoSyncScheduler.start();
	}

	/**
	 * Perform initial sync via checkpoint download.
	 *
	 * Called on first sync when `lastSyncedHlc` is zero. Downloads the
	 * server's checkpoint (which is pre-filtered by JWT claims server-side),
	 * applies the deltas locally, and advances the sync cursor to the
	 * snapshot's HLC. If no checkpoint is available or the transport does
	 * not support checkpoints, falls back to incremental pull.
	 */
	private async initialSync(): Promise<void> {
		if (!this.transport.checkpoint) return;
		const result = await this.transport.checkpoint();
		if (!result.ok || result.value === null) return;
		const { deltas, snapshotHlc } = result.value;
		if (deltas.length > 0) {
			await applyRemoteDeltas(this.db, deltas, this.resolver, this.queue);
		}
		this.lastSyncedHlc = snapshotHlc;
		this._lastSyncTime = new Date();
	}

	/** Perform a single sync cycle (push + pull + actions, depending on syncMode). */
	async syncOnce(): Promise<void> {
		if (this.syncing) return;
		this.syncing = true;
		this.emit("onSyncStart");
		try {
			if (this.syncMode !== "pushOnly") {
				if (this.lastSyncedHlc === HLC.encode(0, 0)) {
					await this.initialSync();
				}
				await this.pullFromGateway();
			}
			if (this.syncMode !== "pullOnly") {
				await this.pushToGateway();
			}
			// Process pending actions after push
			await this.processActionQueue();
			this.emit("onSyncComplete");
		} catch (err) {
			this.emit("onError", err instanceof Error ? err : new Error(String(err)));
		} finally {
			this.syncing = false;
		}
	}

	/**
	 * Submit an action for execution.
	 *
	 * Pushes the action to the ActionQueue and triggers immediate processing.
	 * The action will be sent to the gateway on the next sync cycle or
	 * immediately if not currently syncing.
	 *
	 * @param params - Partial action (connector, actionType, params). ActionId and HLC are generated.
	 */
	async executeAction(params: {
		connector: string;
		actionType: string;
		params: Record<string, unknown>;
		idempotencyKey?: string;
	}): Promise<void> {
		if (!this.actionProcessor) {
			this.emit("onError", new Error("No action queue configured"));
			return;
		}
		await this.actionProcessor.enqueue(params);
	}

	/**
	 * Process pending actions from the action queue.
	 *
	 * Delegates to the ActionProcessor if one is configured.
	 */
	async processActionQueue(): Promise<void> {
		if (!this.actionProcessor) return;
		await this.actionProcessor.processQueue();
	}

	/**
	 * Discover available connectors and their supported action types.
	 *
	 * Delegates to the transport's `describeActions()` method. Returns
	 * empty connectors when the transport does not support discovery.
	 */
	async describeActions(): Promise<Result<ActionDiscovery, LakeSyncError>> {
		if (this.actionProcessor) {
			return this.actionProcessor.describeActions();
		}
		if (!this.transport.describeActions) {
			return { ok: true, value: { connectors: {} } };
		}
		return this.transport.describeActions();
	}

	/**
	 * List available connector types and their configuration schemas.
	 *
	 * Delegates to the transport's `listConnectorTypes()` method. Returns
	 * an empty array when the transport does not support it.
	 */
	async listConnectorTypes(): Promise<Result<ConnectorDescriptor[], LakeSyncError>> {
		if (this.actionProcessor) {
			return this.actionProcessor.listConnectorTypes();
		}
		if (!this.transport.listConnectorTypes) {
			return { ok: true, value: [] };
		}
		return this.transport.listConnectorTypes();
	}

	/** Stop auto-sync and clean up listeners */
	stopAutoSync(): void {
		this.autoSyncScheduler.stop();
		// Disconnect persistent transport (e.g. WebSocket)
		this.transport.disconnect?.();
	}
}

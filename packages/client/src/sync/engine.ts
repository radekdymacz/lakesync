import type { HLCTimestamp, RowDelta } from "@lakesync/core";
import { HLC, LWWResolver } from "@lakesync/core";
import type { LocalDB } from "../db/local-db";
import type { SyncQueue } from "../queue/types";
import { applyRemoteDeltas } from "./applier";
import type { SyncContext } from "./strategy";
import { PullFirstStrategy, type SyncStrategy } from "./strategy";
import type { TransportWithCapabilities } from "./transport";

/** Controls which operations syncOnce() performs */
export type SyncMode = "full" | "pushOnly" | "pullOnly";

/**
 * Configuration for creating a {@link SyncEngine}.
 */
export interface SyncEngineConfig {
	/** The local SQLite database. */
	db: LocalDB;
	/** Transport for communicating with the gateway. */
	transport: TransportWithCapabilities;
	/** Sync queue for outbound deltas. */
	queue: SyncQueue;
	/** HLC instance for timestamp generation. */
	hlc: HLC;
	/** Client identifier. */
	clientId: string;
	/** Maximum retries before dead-lettering a queue entry. Defaults to 10. */
	maxRetries?: number;
	/** Sync mode. Defaults to "full". */
	syncMode?: SyncMode;
	/** Sync strategy. Defaults to PullFirstStrategy. */
	strategy?: SyncStrategy;
}

/**
 * Pure sync engine — owns push, pull, and syncOnce logic.
 *
 * Decomplected from scheduling and event management. The engine
 * manages the core sync state (lastSyncedHlc, lastSyncTime, syncing
 * guard) and exposes sync operations as methods.
 *
 * {@link SyncCoordinator} composes this engine with
 * {@link AutoSyncScheduler}, event listeners, and
 * {@link ActionProcessor}.
 */
export class SyncEngine {
	private readonly db: LocalDB;
	private readonly transport: TransportWithCapabilities;
	private readonly queue: SyncQueue;
	private readonly hlc: HLC;
	private readonly resolver = new LWWResolver();
	private readonly _clientId: string;
	private readonly maxRetries: number;
	private readonly syncMode: SyncMode;
	private readonly strategy: SyncStrategy;

	private _lastSyncedHlc = HLC.encode(0, 0);
	private _lastSyncTime: Date | null = null;
	private _syncing = false;

	/** Callback fired when remote deltas are applied. */
	onRemoteDeltasApplied: ((count: number) => void) | null = null;

	/** Callback fired when a dead-letter event occurs. */
	onDeadLetter: ((count: number) => void) | null = null;

	constructor(config: SyncEngineConfig) {
		this.db = config.db;
		this.transport = config.transport;
		this.queue = config.queue;
		this.hlc = config.hlc;
		this._clientId = config.clientId;
		this.maxRetries = config.maxRetries ?? 10;
		this.syncMode = config.syncMode ?? "full";
		this.strategy = config.strategy ?? new PullFirstStrategy();
	}

	/** Client identifier used for sync operations. */
	get clientId(): string {
		return this._clientId;
	}

	/** HLC timestamp of the last successfully synced delta. */
	get lastSyncedHlc(): HLCTimestamp {
		return this._lastSyncedHlc;
	}

	/** Last successful sync time, or null if never synced. */
	get lastSyncTime(): Date | null {
		return this._lastSyncTime;
	}

	/** Whether a sync cycle is currently in progress. */
	get syncing(): boolean {
		return this._syncing;
	}

	/** Push pending deltas to the gateway via the transport. */
	async push(): Promise<void> {
		const peekResult = await this.queue.peek(100);
		if (!peekResult.ok || peekResult.value.length === 0) return;

		// Dead-letter entries that exceeded max retries
		const deadLettered = peekResult.value.filter((e) => e.retryCount >= this.maxRetries);
		const entries = peekResult.value.filter((e) => e.retryCount < this.maxRetries);

		if (deadLettered.length > 0) {
			console.warn(
				`[SyncEngine] Dead-lettering ${deadLettered.length} entries after ${this.maxRetries} retries`,
			);
			await this.queue.ack(deadLettered.map((e) => e.id));
			this.onDeadLetter?.(deadLettered.length);
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
			this._lastSyncedHlc = pushResult.value.serverHlc;
			this._lastSyncTime = new Date();
		} else {
			await this.queue.nack(ids);
		}
	}

	/**
	 * Pull deltas from a named adapter source.
	 *
	 * Convenience wrapper around {@link pull} that passes the
	 * `source` field through to the gateway.
	 */
	async pullFrom(source: string): Promise<number> {
		return this.pull(source);
	}

	/** Pull remote deltas from the gateway and apply them. */
	async pull(source?: string): Promise<number> {
		const pullResult = await this.transport.pull({
			clientId: this._clientId,
			sinceHlc: this._lastSyncedHlc,
			maxDeltas: 1000,
			source,
		});

		if (!pullResult.ok || pullResult.value.deltas.length === 0) return 0;

		const { deltas, serverHlc } = pullResult.value;
		const applyResult = await applyRemoteDeltas(this.db, deltas, this.resolver, this.queue);

		if (applyResult.ok) {
			this._lastSyncedHlc = serverHlc;
			this._lastSyncTime = new Date();
			if (applyResult.value > 0) {
				this.onRemoteDeltasApplied?.(applyResult.value);
			}
			return applyResult.value;
		}
		return 0;
	}

	/**
	 * Handle a server-initiated broadcast of deltas.
	 *
	 * Applies the deltas using the same conflict resolution and idempotency
	 * logic as a regular pull. Advances `lastSyncedHlc` and fires
	 * `onRemoteDeltasApplied`.
	 */
	async handleBroadcast(deltas: RowDelta[], serverHlc: HLCTimestamp): Promise<void> {
		if (deltas.length === 0) return;

		const applyResult = await applyRemoteDeltas(this.db, deltas, this.resolver, this.queue);

		if (applyResult.ok) {
			if (HLC.compare(serverHlc, this._lastSyncedHlc) > 0) {
				this._lastSyncedHlc = serverHlc;
			}
			this._lastSyncTime = new Date();
			if (applyResult.value > 0) {
				this.onRemoteDeltasApplied?.(applyResult.value);
			}
		}
	}

	/**
	 * Perform initial sync via checkpoint download.
	 *
	 * Called on first sync when `lastSyncedHlc` is zero. Downloads the
	 * server's checkpoint, applies the deltas locally, and advances the
	 * sync cursor to the snapshot's HLC.
	 */
	async initialSync(): Promise<void> {
		if (!this.transport.checkpoint) return;
		const result = await this.transport.checkpoint();
		if (!result.ok || result.value === null) return;
		const { deltas, snapshotHlc } = result.value;
		if (deltas.length > 0) {
			await applyRemoteDeltas(this.db, deltas, this.resolver, this.queue);
		}
		this._lastSyncedHlc = snapshotHlc;
		this._lastSyncTime = new Date();
	}

	/** Get the queue depth. */
	async queueDepth(): Promise<number> {
		const result = await this.queue.depth();
		return result.ok ? result.value : 0;
	}

	/** Build a {@link SyncContext} exposing sync operations for the current cycle. */
	createSyncContext(processActions: () => Promise<void>): SyncContext {
		return {
			isFirstSync: this._lastSyncedHlc === HLC.encode(0, 0),
			syncMode: this.syncMode,
			initialSync: () => this.initialSync(),
			pull: () => this.pull(),
			push: () => this.push(),
			processActions,
		};
	}

	/**
	 * Perform a single sync cycle.
	 *
	 * Delegates to the configured {@link SyncStrategy} which determines
	 * the ordering of pull/push/actions. By default uses
	 * {@link PullFirstStrategy}: pull first, then push — making the
	 * ordering structural rather than temporal.
	 *
	 * @param processActions - Callback to process the action queue.
	 * @returns true if the cycle executed, false if skipped (already syncing).
	 */
	async syncOnce(processActions: () => Promise<void>): Promise<boolean> {
		if (this._syncing) return false;
		this._syncing = true;
		try {
			await this.strategy.execute(this.createSyncContext(processActions));
			return true;
		} finally {
			this._syncing = false;
		}
	}
}

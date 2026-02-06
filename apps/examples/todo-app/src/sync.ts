import type { LocalDB } from "@lakesync/client";
import { applyRemoteDeltas, IDBQueue, SyncTracker } from "@lakesync/client";
import { HLC, LWWResolver } from "@lakesync/core";
import type { SyncGateway } from "@lakesync/gateway";

const CLIENT_ID = `client-${crypto.randomUUID()}`;

/** Auto-sync interval in milliseconds (every 10 seconds). */
const AUTO_SYNC_INTERVAL_MS = 10_000;

/**
 * Coordinates local mutations (via SyncTracker) with gateway push/pull.
 *
 * Replaces the old SyncManager by delegating delta extraction to SyncTracker
 * and conflict resolution to applyRemoteDeltas.
 */
export class SyncCoordinator {
	readonly tracker: SyncTracker;
	private readonly queue: IDBQueue;
	private readonly hlc: HLC;
	private readonly gateway: SyncGateway;
	private readonly db: LocalDB;
	private readonly resolver = new LWWResolver();
	private lastSyncedHlc = HLC.encode(0, 0);
	private _lastSyncTime: Date | null = null;
	private syncIntervalId: ReturnType<typeof setInterval> | null = null;
	private visibilityHandler: (() => void) | null = null;

	constructor(db: LocalDB, gateway: SyncGateway) {
		this.db = db;
		this.gateway = gateway;
		this.hlc = new HLC();
		this.queue = new IDBQueue();
		this.tracker = new SyncTracker(db, this.queue, this.hlc, CLIENT_ID);
	}

	/** Push pending deltas to the gateway. */
	async pushToGateway(): Promise<void> {
		const peekResult = await this.queue.peek(100);
		if (!peekResult.ok || peekResult.value.length === 0) return;

		const entries = peekResult.value;
		const ids = entries.map((e) => e.id);
		await this.queue.markSending(ids);

		const pushResult = this.gateway.handlePush({
			clientId: CLIENT_ID,
			deltas: entries.map((e) => e.delta),
			lastSeenHlc: this.hlc.now(),
		});

		if (pushResult.ok) {
			await this.queue.ack(ids);
			this._lastSyncTime = new Date();
		} else {
			await this.queue.nack(ids);
		}
	}

	/** Pull remote deltas from the gateway and apply them. */
	async pullFromGateway(): Promise<number> {
		const pullResult = this.gateway.handlePull({
			clientId: CLIENT_ID,
			sinceHlc: this.lastSyncedHlc,
			maxDeltas: 1000,
		});

		if (!pullResult.ok || pullResult.value.deltas.length === 0) return 0;

		const { deltas, serverHlc } = pullResult.value;
		const applyResult = await applyRemoteDeltas(this.db, deltas, this.resolver, this.queue);

		if (applyResult.ok) {
			this.lastSyncedHlc = serverHlc;
			this._lastSyncTime = new Date();
			return applyResult.value;
		}
		return 0;
	}

	/** Flush gateway buffer to storage. */
	async flush(): Promise<{ ok: boolean; message: string }> {
		const result = await this.gateway.flush();
		return result.ok
			? { ok: true, message: "Flushed successfully" }
			: { ok: false, message: result.error.message };
	}

	/** Get the queue depth. */
	async queueDepth(): Promise<number> {
		const result = await this.queue.depth();
		return result.ok ? result.value : 0;
	}

	/** Get buffer statistics for monitoring. */
	get stats() {
		return this.gateway.bufferStats;
	}

	/** Get the client identifier. */
	get clientId(): string {
		return CLIENT_ID;
	}

	/** Get the last successful sync time, or null if never synced. */
	get lastSyncTime(): Date | null {
		return this._lastSyncTime;
	}

	/**
	 * Start auto-sync: periodic interval + visibility change handler.
	 * Synchronises (push + pull) on tab focus and every 10 seconds.
	 */
	startAutoSync(): void {
		// Periodic sync
		this.syncIntervalId = setInterval(() => {
			void this.pushToGateway();
			void this.pullFromGateway();
		}, AUTO_SYNC_INTERVAL_MS);

		// Sync on tab visibility change (e.g. tab regains focus)
		this.visibilityHandler = () => {
			if (document.visibilityState === "visible") {
				void this.pushToGateway();
				void this.pullFromGateway();
			}
		};
		document.addEventListener("visibilitychange", this.visibilityHandler);
	}

	/** Stop auto-sync and clean up listeners. */
	stopAutoSync(): void {
		if (this.syncIntervalId !== null) {
			clearInterval(this.syncIntervalId);
			this.syncIntervalId = null;
		}
		if (this.visibilityHandler) {
			document.removeEventListener("visibilitychange", this.visibilityHandler);
			this.visibilityHandler = null;
		}
	}
}

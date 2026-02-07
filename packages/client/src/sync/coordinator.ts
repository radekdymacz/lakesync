import { HLC, LWWResolver } from "@lakesync/core";
import type { LocalDB } from "../db/local-db";
import { IDBQueue } from "../queue/idb-queue";
import type { SyncQueue } from "../queue/types";
import { applyRemoteDeltas } from "./applier";
import { SyncTracker } from "./tracker";
import type { SyncTransport } from "./transport";

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
}

/** Auto-sync interval in milliseconds (every 10 seconds) */
const AUTO_SYNC_INTERVAL_MS = 10_000;

/**
 * Coordinates local mutations (via SyncTracker) with gateway push/pull.
 *
 * Uses a {@link SyncTransport} abstraction to communicate with the gateway,
 * allowing both in-process (LocalTransport) and remote (HttpTransport) usage.
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
	private lastSyncedHlc = HLC.encode(0, 0);
	private _lastSyncTime: Date | null = null;
	private syncIntervalId: ReturnType<typeof setInterval> | null = null;
	private visibilityHandler: (() => void) | null = null;
	private syncing = false;

	constructor(db: LocalDB, transport: SyncTransport, config?: SyncCoordinatorConfig) {
		this.db = db;
		this.transport = transport;
		this.hlc = config?.hlc ?? new HLC();
		this.queue = config?.queue ?? new IDBQueue();
		this._clientId = config?.clientId ?? `client-${crypto.randomUUID()}`;
		this.maxRetries = config?.maxRetries ?? 10;
		this.tracker = new SyncTracker(db, this.queue, this.hlc, this._clientId);
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

	/** Pull remote deltas from the gateway and apply them */
	async pullFromGateway(): Promise<number> {
		const pullResult = await this.transport.pull({
			clientId: this._clientId,
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
	 * Synchronises (push + pull) on tab focus and every 10 seconds.
	 */
	startAutoSync(): void {
		this.syncIntervalId = setInterval(() => {
			if (this.syncing) return;
			this.syncing = true;
			void (async () => {
				try {
					await this.pullFromGateway();
					await this.pushToGateway();
				} finally {
					this.syncing = false;
				}
			})();
		}, AUTO_SYNC_INTERVAL_MS);

		this.visibilityHandler = () => {
			if (typeof document !== "undefined" && document.visibilityState === "visible") {
				if (this.syncing) return;
				this.syncing = true;
				void (async () => {
					try {
						await this.pullFromGateway();
						await this.pushToGateway();
					} finally {
						this.syncing = false;
					}
				})();
			}
		};

		if (typeof document !== "undefined") {
			document.addEventListener("visibilitychange", this.visibilityHandler);
		}
	}

	/** Stop auto-sync and clean up listeners */
	stopAutoSync(): void {
		if (this.syncIntervalId !== null) {
			clearInterval(this.syncIntervalId);
			this.syncIntervalId = null;
		}
		if (this.visibilityHandler) {
			if (typeof document !== "undefined") {
				document.removeEventListener("visibilitychange", this.visibilityHandler);
			}
			this.visibilityHandler = null;
		}
	}
}

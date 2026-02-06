import type { LocalDB } from "@lakesync/client";
import { applyRemoteDeltas, MemoryQueue, SyncTracker } from "@lakesync/client";
import { HLC, LWWResolver } from "@lakesync/core";
import type { SyncGateway } from "@lakesync/gateway";

const CLIENT_ID = `client-${crypto.randomUUID()}`;

/**
 * Coordinates local mutations (via SyncTracker) with gateway push/pull.
 *
 * Replaces the old SyncManager by delegating delta extraction to SyncTracker
 * and conflict resolution to applyRemoteDeltas.
 */
export class SyncCoordinator {
	readonly tracker: SyncTracker;
	private readonly queue: MemoryQueue;
	private readonly hlc: HLC;
	private readonly gateway: SyncGateway;
	private readonly db: LocalDB;
	private readonly resolver = new LWWResolver();
	private lastSyncedHlc = HLC.encode(0, 0);

	constructor(db: LocalDB, gateway: SyncGateway) {
		this.db = db;
		this.gateway = gateway;
		this.hlc = new HLC();
		this.queue = new MemoryQueue();
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
}

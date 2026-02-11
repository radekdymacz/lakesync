import type { LakeSyncError, Result, RowDelta } from "@lakesync/core";
import { MemoryOutbox } from "./memory-outbox";
import type { QueueEntry, SyncQueue } from "./types";

/**
 * In-memory sync queue implementation.
 * Suitable for testing and server-side use.
 *
 * Delegates to {@link MemoryOutbox} for the generic outbox logic and
 * adapts the entry shape to the {@link SyncQueue} interface.
 */
export class MemoryQueue implements SyncQueue {
	private readonly outbox = new MemoryOutbox<RowDelta>("mem");

	/** Add a delta to the queue */
	async push(delta: RowDelta): Promise<Result<QueueEntry, LakeSyncError>> {
		const result = await this.outbox.push(delta);
		if (!result.ok) return result;
		return { ok: true, value: this.toQueueEntry(result.value) };
	}

	/** Peek at pending entries (ordered by createdAt), skipping entries with future retryAfter */
	async peek(limit: number): Promise<Result<QueueEntry[], LakeSyncError>> {
		const result = await this.outbox.peek(limit);
		if (!result.ok) return result;
		return { ok: true, value: result.value.map((e) => this.toQueueEntry(e)) };
	}

	/** Mark entries as currently being sent */
	async markSending(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return this.outbox.markSending(ids);
	}

	/** Acknowledge successful delivery (removes entries) */
	async ack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return this.outbox.ack(ids);
	}

	/** Negative acknowledge — reset to pending with incremented retryCount and exponential backoff */
	async nack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return this.outbox.nack(ids);
	}

	/** Get the number of pending + sending entries */
	async depth(): Promise<Result<number, LakeSyncError>> {
		return this.outbox.depth();
	}

	/** Remove all entries */
	async clear(): Promise<Result<void, LakeSyncError>> {
		return this.outbox.clear();
	}

	/** Convert a generic OutboxEntry to the SyncQueue-specific QueueEntry shape. */
	private toQueueEntry(entry: {
		id: string;
		item: RowDelta;
		status: string;
		createdAt: number;
		retryCount: number;
		retryAfter?: number;
	}): QueueEntry {
		return {
			id: entry.id,
			delta: entry.item,
			status: entry.status as QueueEntry["status"],
			createdAt: entry.createdAt,
			retryCount: entry.retryCount,
			retryAfter: entry.retryAfter,
		};
	}
}

import type { LakeSyncError, Result, RowDelta } from "@lakesync/core";
import { Ok } from "@lakesync/core";
import type { QueueEntry, SyncQueue } from "./types";

/**
 * In-memory sync queue implementation.
 * Suitable for testing and server-side use.
 */
export class MemoryQueue implements SyncQueue {
	private entries: Map<string, QueueEntry> = new Map();
	private counter = 0;

	/** Add a delta to the queue */
	async push(delta: RowDelta): Promise<Result<QueueEntry, LakeSyncError>> {
		const entry: QueueEntry = {
			id: `mem-${++this.counter}`,
			delta,
			status: "pending",
			createdAt: Date.now(),
			retryCount: 0,
		};
		this.entries.set(entry.id, entry);
		return Ok(entry);
	}

	/** Peek at pending entries (ordered by createdAt), skipping entries with future retryAfter */
	async peek(limit: number): Promise<Result<QueueEntry[], LakeSyncError>> {
		const now = Date.now();
		const pending = [...this.entries.values()]
			.filter((e) => e.status === "pending" && (e.retryAfter === undefined || e.retryAfter <= now))
			.sort((a, b) => a.createdAt - b.createdAt)
			.slice(0, limit);
		return Ok(pending);
	}

	/** Mark entries as currently being sent */
	async markSending(ids: string[]): Promise<Result<void, LakeSyncError>> {
		for (const id of ids) {
			const entry = this.entries.get(id);
			if (entry?.status === "pending") {
				entry.status = "sending";
			}
		}
		return Ok(undefined);
	}

	/** Acknowledge successful delivery (removes entries) */
	async ack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		for (const id of ids) {
			this.entries.delete(id);
		}
		return Ok(undefined);
	}

	/** Negative acknowledge — reset to pending with incremented retryCount and exponential backoff */
	async nack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		for (const id of ids) {
			const entry = this.entries.get(id);
			if (entry) {
				entry.status = "pending";
				entry.retryCount++;
				const backoffMs = Math.min(1000 * 2 ** entry.retryCount, 30_000);
				entry.retryAfter = Date.now() + backoffMs;
			}
		}
		return Ok(undefined);
	}

	/** Get the number of pending + sending entries */
	async depth(): Promise<Result<number, LakeSyncError>> {
		const count = [...this.entries.values()].filter((e) => e.status !== "acked").length;
		return Ok(count);
	}

	/** Remove all entries */
	async clear(): Promise<Result<void, LakeSyncError>> {
		this.entries.clear();
		return Ok(undefined);
	}
}

import type { Action, LakeSyncError, Result } from "@lakesync/core";
import { Ok } from "@lakesync/core";
import type { ActionQueue, ActionQueueEntry } from "./action-types";

/**
 * In-memory action queue implementation.
 * Suitable for testing and server-side use.
 */
export class MemoryActionQueue implements ActionQueue {
	private entries: Map<string, ActionQueueEntry> = new Map();
	private counter = 0;

	/** Add an action to the queue. */
	async push(action: Action): Promise<Result<ActionQueueEntry, LakeSyncError>> {
		const entry: ActionQueueEntry = {
			id: `mem-action-${++this.counter}`,
			action,
			status: "pending",
			createdAt: Date.now(),
			retryCount: 0,
		};
		this.entries.set(entry.id, entry);
		return Ok(entry);
	}

	/** Peek at pending entries (ordered by createdAt), skipping entries with future retryAfter. */
	async peek(limit: number): Promise<Result<ActionQueueEntry[], LakeSyncError>> {
		const now = Date.now();
		const pending = [...this.entries.values()]
			.filter((e) => e.status === "pending" && (e.retryAfter === undefined || e.retryAfter <= now))
			.sort((a, b) => a.createdAt - b.createdAt)
			.slice(0, limit);
		return Ok(pending);
	}

	/** Mark entries as currently being sent. */
	async markSending(ids: string[]): Promise<Result<void, LakeSyncError>> {
		for (const id of ids) {
			const entry = this.entries.get(id);
			if (entry?.status === "pending") {
				entry.status = "sending";
			}
		}
		return Ok(undefined);
	}

	/** Acknowledge successful delivery (removes entries). */
	async ack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		for (const id of ids) {
			this.entries.delete(id);
		}
		return Ok(undefined);
	}

	/** Negative acknowledge — reset to pending with incremented retryCount and exponential backoff. */
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

	/** Get the number of pending + sending entries. */
	async depth(): Promise<Result<number, LakeSyncError>> {
		const count = [...this.entries.values()].filter(
			(e) => e.status === "pending" || e.status === "sending",
		).length;
		return Ok(count);
	}

	/** Remove all entries. */
	async clear(): Promise<Result<void, LakeSyncError>> {
		this.entries.clear();
		return Ok(undefined);
	}
}

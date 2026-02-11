import type { Action, LakeSyncError, Result } from "@lakesync/core";
import type { ActionQueue, ActionQueueEntry } from "./action-types";
import { MemoryOutbox } from "./memory-outbox";

/**
 * In-memory action queue implementation.
 * Suitable for testing and server-side use.
 *
 * Delegates to {@link MemoryOutbox} for the generic outbox logic and
 * adapts the entry shape to the {@link ActionQueue} interface.
 */
export class MemoryActionQueue implements ActionQueue {
	private readonly outbox = new MemoryOutbox<Action>("mem-action");

	/** Add an action to the queue. */
	async push(action: Action): Promise<Result<ActionQueueEntry, LakeSyncError>> {
		const result = await this.outbox.push(action);
		if (!result.ok) return result;
		return { ok: true, value: this.toActionEntry(result.value) };
	}

	/** Peek at pending entries (ordered by createdAt), skipping entries with future retryAfter. */
	async peek(limit: number): Promise<Result<ActionQueueEntry[], LakeSyncError>> {
		const result = await this.outbox.peek(limit);
		if (!result.ok) return result;
		return { ok: true, value: result.value.map((e) => this.toActionEntry(e)) };
	}

	/** Mark entries as currently being sent. */
	async markSending(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return this.outbox.markSending(ids);
	}

	/** Acknowledge successful delivery (removes entries). */
	async ack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return this.outbox.ack(ids);
	}

	/** Negative acknowledge — reset to pending with incremented retryCount and exponential backoff. */
	async nack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return this.outbox.nack(ids);
	}

	/** Get the number of pending + sending entries. */
	async depth(): Promise<Result<number, LakeSyncError>> {
		return this.outbox.depth();
	}

	/** Remove all entries. */
	async clear(): Promise<Result<void, LakeSyncError>> {
		return this.outbox.clear();
	}

	/** Convert a generic OutboxEntry to the ActionQueue-specific ActionQueueEntry shape. */
	private toActionEntry(entry: {
		id: string;
		item: Action;
		status: string;
		createdAt: number;
		retryCount: number;
		retryAfter?: number;
	}): ActionQueueEntry {
		return {
			id: entry.id,
			action: entry.item,
			status: entry.status as ActionQueueEntry["status"],
			createdAt: entry.createdAt,
			retryCount: entry.retryCount,
			retryAfter: entry.retryAfter,
		};
	}
}

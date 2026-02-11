import type { LakeSyncError, Result } from "@lakesync/core";
import { Ok } from "@lakesync/core";

/** Status of an outbox entry. */
export type OutboxEntryStatus = "pending" | "sending" | "acked";

/** A single entry in a generic outbox queue. */
export interface OutboxEntry<T> {
	/** Unique entry identifier. */
	id: string;
	/** The payload item. */
	item: T;
	/** Current processing status. */
	status: OutboxEntryStatus;
	/** Timestamp when the entry was created. */
	createdAt: number;
	/** Number of times this entry has been retried. */
	retryCount: number;
	/** Earliest time (ms since epoch) this entry should be retried. Undefined = immediately. */
	retryAfter?: number;
}

/** Generic outbox queue interface. */
export interface Outbox<T> {
	/** Add an item to the queue. */
	push(item: T): Promise<Result<OutboxEntry<T>, LakeSyncError>>;
	/** Peek at pending entries (ordered by createdAt). */
	peek(limit: number): Promise<Result<OutboxEntry<T>[], LakeSyncError>>;
	/** Mark entries as currently being sent. */
	markSending(ids: string[]): Promise<Result<void, LakeSyncError>>;
	/** Acknowledge successful delivery (removes entries). */
	ack(ids: string[]): Promise<Result<void, LakeSyncError>>;
	/** Negative acknowledge — reset to pending with incremented retryCount. */
	nack(ids: string[]): Promise<Result<void, LakeSyncError>>;
	/** Get the number of pending + sending entries. */
	depth(): Promise<Result<number, LakeSyncError>>;
	/** Remove all entries. */
	clear(): Promise<Result<void, LakeSyncError>>;
}

/**
 * Generic in-memory outbox queue.
 *
 * Suitable for testing and server-side use. Parameterised over the
 * payload type so the same logic can back both delta and action queues.
 */
export class MemoryOutbox<T> implements Outbox<T> {
	private entries: Map<string, OutboxEntry<T>> = new Map();
	private counter = 0;
	private readonly prefix: string;

	constructor(prefix = "mem") {
		this.prefix = prefix;
	}

	/** Add an item to the queue. */
	async push(item: T): Promise<Result<OutboxEntry<T>, LakeSyncError>> {
		const entry: OutboxEntry<T> = {
			id: `${this.prefix}-${++this.counter}`,
			item,
			status: "pending",
			createdAt: Date.now(),
			retryCount: 0,
		};
		this.entries.set(entry.id, entry);
		return Ok(entry);
	}

	/** Peek at pending entries (ordered by createdAt), skipping entries with future retryAfter. */
	async peek(limit: number): Promise<Result<OutboxEntry<T>[], LakeSyncError>> {
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

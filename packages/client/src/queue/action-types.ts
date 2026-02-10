import type { Action, LakeSyncError, Result } from "@lakesync/core";

/** Status of an action queue entry. */
export type ActionQueueEntryStatus = "pending" | "sending" | "acked" | "failed";

/** A single entry in the action queue. */
export interface ActionQueueEntry {
	/** Unique entry identifier. */
	id: string;
	/** The action to be executed. */
	action: Action;
	/** Current processing status. */
	status: ActionQueueEntryStatus;
	/** Timestamp when the entry was created. */
	createdAt: number;
	/** Number of times this entry has been retried. */
	retryCount: number;
	/** Earliest time (ms since epoch) this entry should be retried. Undefined = immediately. */
	retryAfter?: number;
}

/** Outbox-pattern action queue interface. */
export interface ActionQueue {
	/** Add an action to the queue. */
	push(action: Action): Promise<Result<ActionQueueEntry, LakeSyncError>>;
	/** Peek at pending entries (ordered by createdAt). */
	peek(limit: number): Promise<Result<ActionQueueEntry[], LakeSyncError>>;
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

import type { LakeSyncError, Result, RowDelta } from "@lakesync/core";

/** Status of a queue entry */
export type QueueEntryStatus = "pending" | "sending" | "acked";

/** A single entry in the sync queue */
export interface QueueEntry {
	/** Unique entry identifier */
	id: string;
	/** The delta to be synced */
	delta: RowDelta;
	/** Current processing status */
	status: QueueEntryStatus;
	/** Timestamp when the entry was created */
	createdAt: number;
	/** Number of times this entry has been retried */
	retryCount: number;
}

/** Outbox-pattern sync queue interface */
export interface SyncQueue {
	/** Add a delta to the queue */
	push(delta: RowDelta): Promise<Result<QueueEntry, LakeSyncError>>;
	/** Peek at pending entries (ordered by createdAt) */
	peek(limit: number): Promise<Result<QueueEntry[], LakeSyncError>>;
	/** Mark entries as currently being sent */
	markSending(ids: string[]): Promise<Result<void, LakeSyncError>>;
	/** Acknowledge successful delivery (removes entries) */
	ack(ids: string[]): Promise<Result<void, LakeSyncError>>;
	/** Negative acknowledge — reset to pending with incremented retryCount */
	nack(ids: string[]): Promise<Result<void, LakeSyncError>>;
	/** Get the number of pending + sending entries */
	depth(): Promise<Result<number, LakeSyncError>>;
	/** Remove all entries */
	clear(): Promise<Result<void, LakeSyncError>>;
}

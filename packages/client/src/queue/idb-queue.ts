import type { HLCTimestamp, Result, RowDelta } from "@lakesync/core";
import { Err, LakeSyncError, Ok } from "@lakesync/core";
import { type IDBPDatabase, openDB } from "idb";
import type { QueueEntry, SyncQueue } from "./types";

const DB_NAME = "lakesync-queue";
const DB_VERSION = 1;
const STORE_NAME = "entries";

/**
 * Serialised form of a RowDelta where the HLC bigint is stored as a string.
 * IndexedDB uses structuredClone internally which cannot handle bigint values,
 * so we convert to/from string representation for storage.
 */
type SerialisedRowDelta = Omit<RowDelta, "hlc"> & { hlc: string };

/** Serialised queue entry stored in IndexedDB */
type SerialisedQueueEntry = Omit<QueueEntry, "delta"> & {
	delta: SerialisedRowDelta;
};

/** Convert a RowDelta to its serialised form for IDB storage */
function serialiseDelta(delta: RowDelta): SerialisedRowDelta {
	return { ...delta, hlc: delta.hlc.toString() };
}

/** Convert a serialised delta back to a RowDelta with bigint HLC */
function deserialiseDelta(serialised: SerialisedRowDelta): RowDelta {
	return { ...serialised, hlc: BigInt(serialised.hlc) as HLCTimestamp };
}

/** Convert a QueueEntry to its serialised form */
function serialiseEntry(entry: QueueEntry): SerialisedQueueEntry {
	return { ...entry, delta: serialiseDelta(entry.delta) };
}

/** Convert a serialised entry back to a QueueEntry */
function deserialiseEntry(serialised: SerialisedQueueEntry): QueueEntry {
	return { ...serialised, delta: deserialiseDelta(serialised.delta) };
}

/** Wrap an async IDB operation, catching errors into a QUEUE_ERROR Result */
async function wrapIdbOp<T>(
	operation: string,
	fn: () => Promise<T>,
): Promise<Result<T, LakeSyncError>> {
	try {
		return Ok(await fn());
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		return Err(new LakeSyncError(`Failed to ${operation}: ${message}`, "QUEUE_ERROR"));
	}
}

/**
 * IndexedDB-backed sync queue implementation.
 * Uses a single readwrite transaction for atomic claim operations.
 *
 * HLC timestamps (branded bigints) are serialised to strings for storage,
 * as IndexedDB's structuredClone cannot handle bigint values.
 */
export class IDBQueue implements SyncQueue {
	private dbPromise: Promise<IDBPDatabase>;
	private counter = 0;

	/**
	 * Create a new IDB-backed sync queue.
	 *
	 * @param dbName - Optional database name. Defaults to `'lakesync-queue'`.
	 *                 Useful for tests or running multiple independent queues.
	 */
	constructor(dbName: string = DB_NAME) {
		this.dbPromise = openDB(dbName, DB_VERSION, {
			upgrade(db) {
				const store = db.createObjectStore(STORE_NAME, { keyPath: "id" });
				store.createIndex("status", "status");
				store.createIndex("createdAt", "createdAt");
			},
		});
	}

	/** Add a delta to the queue */
	async push(delta: RowDelta): Promise<Result<QueueEntry, LakeSyncError>> {
		return wrapIdbOp("push to queue", async () => {
			const db = await this.dbPromise;
			const entry: QueueEntry = {
				id: `idb-${Date.now()}-${++this.counter}`,
				delta,
				status: "pending",
				createdAt: Date.now(),
				retryCount: 0,
			};
			await db.put(STORE_NAME, serialiseEntry(entry));
			return entry;
		});
	}

	/** Peek at pending entries (ordered by createdAt) */
	async peek(limit: number): Promise<Result<QueueEntry[], LakeSyncError>> {
		return wrapIdbOp("peek queue", async () => {
			const db = await this.dbPromise;
			const tx = db.transaction(STORE_NAME, "readonly");
			const index = tx.objectStore(STORE_NAME).index("createdAt");
			const results: QueueEntry[] = [];

			let cursor = await index.openCursor();
			while (cursor && results.length < limit) {
				const serialised = cursor.value as SerialisedQueueEntry;
				if (serialised.status === "pending") {
					results.push(deserialiseEntry(serialised));
				}
				cursor = await cursor.continue();
			}

			return results;
		});
	}

	/** Mark entries as currently being sent */
	async markSending(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return wrapIdbOp("mark sending", async () => {
			const db = await this.dbPromise;
			const tx = db.transaction(STORE_NAME, "readwrite");
			const store = tx.objectStore(STORE_NAME);

			for (const id of ids) {
				const serialised = (await store.get(id)) as SerialisedQueueEntry | undefined;
				if (serialised?.status === "pending") {
					serialised.status = "sending";
					await store.put(serialised);
				}
			}

			await tx.done;
		});
	}

	/** Acknowledge successful delivery (removes entries) */
	async ack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return wrapIdbOp("ack", async () => {
			const db = await this.dbPromise;
			const tx = db.transaction(STORE_NAME, "readwrite");
			for (const id of ids) {
				await tx.objectStore(STORE_NAME).delete(id);
			}
			await tx.done;
		});
	}

	/** Negative acknowledge — reset to pending with incremented retryCount */
	async nack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return wrapIdbOp("nack", async () => {
			const db = await this.dbPromise;
			const tx = db.transaction(STORE_NAME, "readwrite");
			const store = tx.objectStore(STORE_NAME);

			for (const id of ids) {
				const serialised = (await store.get(id)) as SerialisedQueueEntry | undefined;
				if (serialised) {
					serialised.status = "pending";
					serialised.retryCount++;
					await store.put(serialised);
				}
			}

			await tx.done;
		});
	}

	/** Get the number of pending + sending entries */
	async depth(): Promise<Result<number, LakeSyncError>> {
		return wrapIdbOp("get depth", async () => {
			const db = await this.dbPromise;
			const all = (await db.getAll(STORE_NAME)) as SerialisedQueueEntry[];
			return all.filter((e) => e.status !== "acked").length;
		});
	}

	/** Remove all entries */
	async clear(): Promise<Result<void, LakeSyncError>> {
		return wrapIdbOp("clear queue", async () => {
			const db = await this.dbPromise;
			await db.clear(STORE_NAME);
		});
	}
}

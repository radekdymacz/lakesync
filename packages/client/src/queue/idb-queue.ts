import type { RowDelta } from "@lakesync/core";
import type { HLCTimestamp } from "@lakesync/core";
import { Err, LakeSyncError, Ok } from "@lakesync/core";
import type { Result } from "@lakesync/core";
import { type IDBPDatabase, openDB } from "idb";
import type { QueueEntry, QueueEntryStatus, SyncQueue } from "./types";

const DB_NAME = "lakesync-queue";
const DB_VERSION = 1;
const STORE_NAME = "entries";

/**
 * Serialised form of a RowDelta where the HLC bigint is stored as a string.
 * IndexedDB uses structuredClone internally which cannot handle bigint values,
 * so we convert to/from string representation for storage.
 */
interface SerialisedRowDelta {
	op: RowDelta["op"];
	table: string;
	rowId: string;
	clientId: string;
	columns: RowDelta["columns"];
	hlc: string;
	deltaId: string;
}

/** Serialised queue entry stored in IndexedDB */
interface SerialisedQueueEntry {
	id: string;
	delta: SerialisedRowDelta;
	status: QueueEntryStatus;
	createdAt: number;
	retryCount: number;
}

/** Convert a RowDelta to its serialised form for IDB storage */
function serialiseDelta(delta: RowDelta): SerialisedRowDelta {
	return {
		op: delta.op,
		table: delta.table,
		rowId: delta.rowId,
		clientId: delta.clientId,
		columns: delta.columns,
		hlc: delta.hlc.toString(),
		deltaId: delta.deltaId,
	};
}

/** Convert a serialised delta back to a RowDelta with bigint HLC */
function deserialiseDelta(serialised: SerialisedRowDelta): RowDelta {
	return {
		op: serialised.op,
		table: serialised.table,
		rowId: serialised.rowId,
		clientId: serialised.clientId,
		columns: serialised.columns,
		hlc: BigInt(serialised.hlc) as HLCTimestamp,
		deltaId: serialised.deltaId,
	};
}

/** Convert a QueueEntry to its serialised form */
function serialiseEntry(entry: QueueEntry): SerialisedQueueEntry {
	return {
		id: entry.id,
		delta: serialiseDelta(entry.delta),
		status: entry.status,
		createdAt: entry.createdAt,
		retryCount: entry.retryCount,
	};
}

/** Convert a serialised entry back to a QueueEntry */
function deserialiseEntry(serialised: SerialisedQueueEntry): QueueEntry {
	return {
		id: serialised.id,
		delta: deserialiseDelta(serialised.delta),
		status: serialised.status,
		createdAt: serialised.createdAt,
		retryCount: serialised.retryCount,
	};
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
		try {
			const db = await this.dbPromise;
			const entry: QueueEntry = {
				id: `idb-${Date.now()}-${++this.counter}`,
				delta,
				status: "pending",
				createdAt: Date.now(),
				retryCount: 0,
			};
			await db.put(STORE_NAME, serialiseEntry(entry));
			return Ok(entry);
		} catch (error) {
			return Err(
				new LakeSyncError(
					`Failed to push to queue: ${error instanceof Error ? error.message : String(error)}`,
					"QUEUE_ERROR",
				),
			);
		}
	}

	/** Peek at pending entries (ordered by createdAt) */
	async peek(limit: number): Promise<Result<QueueEntry[], LakeSyncError>> {
		try {
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

			return Ok(results);
		} catch (error) {
			return Err(
				new LakeSyncError(
					`Failed to peek queue: ${error instanceof Error ? error.message : String(error)}`,
					"QUEUE_ERROR",
				),
			);
		}
	}

	/** Mark entries as currently being sent */
	async markSending(ids: string[]): Promise<Result<void, LakeSyncError>> {
		try {
			const db = await this.dbPromise;
			const tx = db.transaction(STORE_NAME, "readwrite");
			const store = tx.objectStore(STORE_NAME);

			for (const id of ids) {
				const serialised = (await store.get(id)) as SerialisedQueueEntry | undefined;
				if (serialised && serialised.status === "pending") {
					serialised.status = "sending";
					await store.put(serialised);
				}
			}

			await tx.done;
			return Ok(undefined);
		} catch (error) {
			return Err(
				new LakeSyncError(
					`Failed to mark sending: ${error instanceof Error ? error.message : String(error)}`,
					"QUEUE_ERROR",
				),
			);
		}
	}

	/** Acknowledge successful delivery (removes entries) */
	async ack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		try {
			const db = await this.dbPromise;
			const tx = db.transaction(STORE_NAME, "readwrite");
			for (const id of ids) {
				await tx.objectStore(STORE_NAME).delete(id);
			}
			await tx.done;
			return Ok(undefined);
		} catch (error) {
			return Err(
				new LakeSyncError(
					`Failed to ack: ${error instanceof Error ? error.message : String(error)}`,
					"QUEUE_ERROR",
				),
			);
		}
	}

	/** Negative acknowledge — reset to pending with incremented retryCount */
	async nack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		try {
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
			return Ok(undefined);
		} catch (error) {
			return Err(
				new LakeSyncError(
					`Failed to nack: ${error instanceof Error ? error.message : String(error)}`,
					"QUEUE_ERROR",
				),
			);
		}
	}

	/** Get the number of pending + sending entries */
	async depth(): Promise<Result<number, LakeSyncError>> {
		try {
			const db = await this.dbPromise;
			const all = await db.getAll(STORE_NAME);
			const count = (all as SerialisedQueueEntry[]).filter((e) => e.status !== "acked").length;
			return Ok(count);
		} catch (error) {
			return Err(
				new LakeSyncError(
					`Failed to get depth: ${error instanceof Error ? error.message : String(error)}`,
					"QUEUE_ERROR",
				),
			);
		}
	}

	/** Remove all entries */
	async clear(): Promise<Result<void, LakeSyncError>> {
		try {
			const db = await this.dbPromise;
			await db.clear(STORE_NAME);
			return Ok(undefined);
		} catch (error) {
			return Err(
				new LakeSyncError(
					`Failed to clear queue: ${error instanceof Error ? error.message : String(error)}`,
					"QUEUE_ERROR",
				),
			);
		}
	}
}

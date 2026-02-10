import type { Action, HLCTimestamp, Result } from "@lakesync/core";
import { Err, LakeSyncError, Ok } from "@lakesync/core";
import { type IDBPDatabase, openDB } from "idb";
import type { ActionQueue, ActionQueueEntry } from "./action-types";

const DB_NAME = "lakesync-action-queue";
const DB_VERSION = 1;
const STORE_NAME = "entries";

/**
 * Serialised form of an Action where the HLC bigint is stored as a string.
 * IndexedDB uses structuredClone internally which cannot handle bigint values.
 */
type SerialisedAction = Omit<Action, "hlc"> & { hlc: string };

/** Serialised queue entry stored in IndexedDB. */
type SerialisedActionQueueEntry = Omit<ActionQueueEntry, "action"> & {
	action: SerialisedAction;
};

/** Convert an Action to its serialised form for IDB storage. */
function serialiseAction(action: Action): SerialisedAction {
	return { ...action, hlc: action.hlc.toString() };
}

/** Convert a serialised action back to an Action with bigint HLC. */
function deserialiseAction(serialised: SerialisedAction): Action {
	return { ...serialised, hlc: BigInt(serialised.hlc) as HLCTimestamp };
}

/** Convert an ActionQueueEntry to its serialised form. */
function serialiseEntry(entry: ActionQueueEntry): SerialisedActionQueueEntry {
	return { ...entry, action: serialiseAction(entry.action) };
}

/** Convert a serialised entry back to an ActionQueueEntry. */
function deserialiseEntry(serialised: SerialisedActionQueueEntry): ActionQueueEntry {
	return { ...serialised, action: deserialiseAction(serialised.action) };
}

/** Wrap an async IDB operation, catching errors into a QUEUE_ERROR Result. */
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
 * IndexedDB-backed action queue implementation.
 *
 * HLC timestamps (branded bigints) are serialised to strings for storage,
 * as IndexedDB's structuredClone cannot handle bigint values.
 */
export class IDBActionQueue implements ActionQueue {
	private dbPromise: Promise<IDBPDatabase>;
	private counter = 0;

	/**
	 * Create a new IDB-backed action queue.
	 *
	 * @param dbName - Optional database name. Defaults to `'lakesync-action-queue'`.
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

	/** Add an action to the queue. */
	async push(action: Action): Promise<Result<ActionQueueEntry, LakeSyncError>> {
		return wrapIdbOp("push to action queue", async () => {
			const db = await this.dbPromise;
			const entry: ActionQueueEntry = {
				id: `idb-action-${Date.now()}-${++this.counter}`,
				action,
				status: "pending",
				createdAt: Date.now(),
				retryCount: 0,
			};
			await db.put(STORE_NAME, serialiseEntry(entry));
			return entry;
		});
	}

	/** Peek at pending entries (ordered by createdAt). */
	async peek(limit: number): Promise<Result<ActionQueueEntry[], LakeSyncError>> {
		return wrapIdbOp("peek action queue", async () => {
			const db = await this.dbPromise;
			const tx = db.transaction(STORE_NAME, "readonly");
			const index = tx.objectStore(STORE_NAME).index("createdAt");
			const results: ActionQueueEntry[] = [];

			let cursor = await index.openCursor();
			while (cursor && results.length < limit) {
				const serialised = cursor.value as SerialisedActionQueueEntry;
				if (serialised.status === "pending") {
					const entry = deserialiseEntry(serialised);
					if (entry.retryAfter === undefined || entry.retryAfter <= Date.now()) {
						results.push(entry);
					}
				}
				cursor = await cursor.continue();
			}

			return results;
		});
	}

	/** Mark entries as currently being sent. */
	async markSending(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return wrapIdbOp("mark sending", async () => {
			const db = await this.dbPromise;
			const tx = db.transaction(STORE_NAME, "readwrite");
			const store = tx.objectStore(STORE_NAME);

			for (const id of ids) {
				const serialised = (await store.get(id)) as SerialisedActionQueueEntry | undefined;
				if (serialised?.status === "pending") {
					serialised.status = "sending";
					await store.put(serialised);
				}
			}

			await tx.done;
		});
	}

	/** Acknowledge successful delivery (removes entries). */
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

	/** Negative acknowledge — reset to pending with incremented retryCount and exponential backoff. */
	async nack(ids: string[]): Promise<Result<void, LakeSyncError>> {
		return wrapIdbOp("nack", async () => {
			const db = await this.dbPromise;
			const tx = db.transaction(STORE_NAME, "readwrite");
			const store = tx.objectStore(STORE_NAME);

			for (const id of ids) {
				const serialised = (await store.get(id)) as SerialisedActionQueueEntry | undefined;
				if (serialised) {
					serialised.status = "pending";
					serialised.retryCount++;
					const backoffMs = Math.min(1000 * 2 ** serialised.retryCount, 30_000);
					(serialised as Record<string, unknown>).retryAfter = Date.now() + backoffMs;
					await store.put(serialised);
				}
			}

			await tx.done;
		});
	}

	/** Get the number of pending + sending entries. */
	async depth(): Promise<Result<number, LakeSyncError>> {
		return wrapIdbOp("get depth", async () => {
			const db = await this.dbPromise;
			const all = (await db.getAll(STORE_NAME)) as SerialisedActionQueueEntry[];
			return all.filter((e) => e.status === "pending" || e.status === "sending").length;
		});
	}

	/** Remove all entries. */
	async clear(): Promise<Result<void, LakeSyncError>> {
		return wrapIdbOp("clear action queue", async () => {
			const db = await this.dbPromise;
			await db.clear(STORE_NAME);
		});
	}
}

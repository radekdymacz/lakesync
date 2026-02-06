import { type IDBPDatabase, openDB } from "idb";

const IDB_DB_NAME = "lakesync-snapshots";
const IDB_DB_VERSION = 1;
const STORE_NAME = "snapshots";

/** Cached IDB connection */
let cachedDb: IDBPDatabase | null = null;

/** Open (or reuse) the snapshot IndexedDB database */
async function getDb(): Promise<IDBPDatabase> {
	if (cachedDb) return cachedDb;
	cachedDb = await openDB(IDB_DB_NAME, IDB_DB_VERSION, {
		upgrade(db) {
			if (!db.objectStoreNames.contains(STORE_NAME)) {
				db.createObjectStore(STORE_NAME);
			}
		},
	});
	return cachedDb;
}

/**
 * Load a database snapshot from IndexedDB.
 *
 * @param dbName - The logical database name (used as key in IndexedDB)
 * @returns The raw SQLite database bytes, or null if no snapshot exists
 */
export async function loadSnapshot(dbName: string): Promise<Uint8Array | null> {
	const idb = await getDb();
	const data = await idb.get(STORE_NAME, dbName);
	if (data instanceof Uint8Array) return data;
	return null;
}

/**
 * Save a database snapshot to IndexedDB.
 *
 * @param dbName - The logical database name (used as key in IndexedDB)
 * @param data - The raw SQLite database bytes from sql.js `db.export()`
 */
export async function saveSnapshot(dbName: string, data: Uint8Array): Promise<void> {
	const idb = await getDb();
	await idb.put(STORE_NAME, data, dbName);
}

/**
 * Delete a database snapshot from IndexedDB.
 *
 * @param dbName - The logical database name to remove
 */
export async function deleteSnapshot(dbName: string): Promise<void> {
	const idb = await getDb();
	await idb.delete(STORE_NAME, dbName);
}

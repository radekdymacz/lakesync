import type { RowDelta } from "@lakesync/core";
import { bigintReplacer, bigintReviver } from "@lakesync/core";

/**
 * Persistence interface for buffering unflushed deltas across restarts.
 *
 * Implementations must be synchronous (no async) to avoid race conditions
 * during the push-then-flush cycle.
 */
export interface DeltaPersistence {
	/** Append a batch of deltas to the persistence store. */
	appendBatch(deltas: RowDelta[]): void;
	/** Load all persisted deltas. */
	loadAll(): RowDelta[];
	/** Clear all persisted deltas (after successful flush). */
	clear(): void;
	/** Release resources. */
	close(): void;
}

/**
 * In-memory persistence (no durability across restarts).
 * Used as the default when `persistence` is "memory".
 */
export class MemoryPersistence implements DeltaPersistence {
	private buffer: RowDelta[] = [];

	appendBatch(deltas: RowDelta[]): void {
		this.buffer.push(...deltas);
	}

	loadAll(): RowDelta[] {
		return [...this.buffer];
	}

	clear(): void {
		this.buffer = [];
	}

	close(): void {
		this.buffer = [];
	}
}

/**
 * SQLite-backed persistence using `better-sqlite3`.
 *
 * Stores deltas as JSON rows in a single table. On startup, loads all
 * rows back as RowDeltas. On flush success, truncates the table.
 */
export class SqlitePersistence implements DeltaPersistence {
	private db: import("better-sqlite3").Database;

	constructor(path: string) {
		// eslint-disable-next-line @typescript-eslint/no-require-imports
		const Database = require("better-sqlite3") as typeof import("better-sqlite3");
		this.db = new Database(path);
		this.db.pragma("journal_mode = WAL");
		this.db.exec(
			"CREATE TABLE IF NOT EXISTS unflushed_deltas (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL)",
		);
	}

	appendBatch(deltas: RowDelta[]): void {
		const stmt = this.db.prepare("INSERT INTO unflushed_deltas (data) VALUES (?)");
		const tx = this.db.transaction(() => {
			for (const delta of deltas) {
				stmt.run(JSON.stringify(delta, bigintReplacer));
			}
		});
		tx();
	}

	loadAll(): RowDelta[] {
		const rows = this.db.prepare("SELECT data FROM unflushed_deltas ORDER BY id").all() as Array<{
			data: string;
		}>;
		return rows.map((row) => JSON.parse(row.data, bigintReviver) as RowDelta);
	}

	clear(): void {
		this.db.exec("DELETE FROM unflushed_deltas");
	}

	close(): void {
		this.db.close();
	}
}

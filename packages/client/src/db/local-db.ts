import type { Result } from "@lakesync/core";
import { Err, Ok } from "@lakesync/core";
import type { Database, QueryExecResult } from "sql.js";
import initSqlJs from "sql.js";
import { loadSnapshot, saveSnapshot } from "./idb-persistence";
import type { DbConfig, Transaction } from "./types";
import { DbError } from "./types";

/** Resolved storage backend after auto-detection */
type ResolvedBackend = "idb" | "memory";

/** Map sql.js query results into typed row objects */
function mapResultRows<T>(results: QueryExecResult[]): T[] {
	if (results.length === 0 || !results[0]) {
		return [];
	}
	const { columns, values } = results[0];
	return values.map((row) => {
		const obj: Record<string, unknown> = {};
		for (let i = 0; i < columns.length; i++) {
			const col = columns[i];
			if (col !== undefined) {
				obj[col] = row[i];
			}
		}
		return obj as T;
	});
}

/**
 * Local SQLite database backed by sql.js (SQLite compiled to WASM).
 *
 * Supports two persistence backends:
 * - `"memory"` — purely in-memory, data lost on close
 * - `"idb"` — snapshots persisted to IndexedDB between sessions
 *
 * When no backend is specified, auto-detects: uses `"idb"` if
 * `indexedDB` is available, otherwise falls back to `"memory"`.
 */
export class LocalDB {
	readonly #db: Database;
	readonly #config: DbConfig;
	readonly #backend: ResolvedBackend;

	private constructor(db: Database, config: DbConfig, backend: ResolvedBackend) {
		this.#db = db;
		this.#config = config;
		this.#backend = backend;
	}

	/** The database name from configuration */
	get name(): string {
		return this.#config.name;
	}

	/** The resolved storage backend for this instance */
	get backend(): ResolvedBackend {
		return this.#backend;
	}

	/**
	 * Open a new LocalDB instance.
	 *
	 * Initialises the sql.js WASM engine and creates a database. When the
	 * backend is `"idb"`, any existing snapshot is loaded from IndexedDB.
	 * If no backend is specified, auto-detects based on `indexedDB` availability.
	 */
	static async open(config: DbConfig): Promise<Result<LocalDB, DbError>> {
		try {
			const backend: ResolvedBackend = resolveBackend(config.backend);

			const SQL = await initSqlJs();

			let data: Uint8Array | null = null;
			if (backend === "idb") {
				data = await loadSnapshot(config.name);
			}

			const db = data ? new SQL.Database(data) : new SQL.Database();
			return Ok(new LocalDB(db, config, backend));
		} catch (err) {
			return Err(
				new DbError(
					`Failed to open database "${config.name}"`,
					err instanceof Error ? err : new Error(String(err)),
				),
			);
		}
	}

	/**
	 * Execute a SQL statement (INSERT, UPDATE, DELETE, CREATE, etc.).
	 *
	 * Returns `Ok(void)` on success, or `Err(DbError)` on failure.
	 */
	async exec(sql: string, params?: unknown[]): Promise<Result<void, DbError>> {
		try {
			this.#db.run(sql, params as Parameters<Database["run"]>[1]);
			return Ok(undefined);
		} catch (err) {
			return Err(
				new DbError(
					`Failed to execute SQL: ${sql}`,
					err instanceof Error ? err : new Error(String(err)),
				),
			);
		}
	}

	/**
	 * Query the database and return typed rows as an array of objects.
	 *
	 * Each row is mapped from sql.js column-array format into a keyed object.
	 */
	async query<T>(sql: string, params?: unknown[]): Promise<Result<T[], DbError>> {
		try {
			const results = this.#db.exec(sql, params as Parameters<Database["exec"]>[1]);
			return Ok(mapResultRows<T>(results));
		} catch (err) {
			return Err(
				new DbError(
					`Failed to query SQL: ${sql}`,
					err instanceof Error ? err : new Error(String(err)),
				),
			);
		}
	}

	/**
	 * Execute a function within a database transaction.
	 *
	 * Begins a transaction, executes the callback with a `Transaction` object,
	 * commits on success, or rolls back if the callback throws.
	 */
	async transaction<T>(fn: (tx: Transaction) => T): Promise<Result<T, DbError>> {
		const tx = this.#createTransaction();

		try {
			this.#db.run("BEGIN");
		} catch (err) {
			return Err(
				new DbError(
					"Failed to begin transaction",
					err instanceof Error ? err : new Error(String(err)),
				),
			);
		}

		try {
			const result = fn(tx);
			this.#db.run("COMMIT");
			return Ok(result);
		} catch (err) {
			try {
				this.#db.run("ROLLBACK");
			} catch (_rollbackErr) {
				// Rollback failure is secondary; report the original error
			}
			return Err(
				new DbError("Transaction failed", err instanceof Error ? err : new Error(String(err))),
			);
		}
	}

	/**
	 * Export the current database state and persist it to IndexedDB.
	 *
	 * No-op when the backend is `"memory"`.
	 */
	async save(): Promise<Result<void, DbError>> {
		if (this.#backend !== "idb") {
			return Ok(undefined);
		}
		try {
			const data = this.#db.export();
			await saveSnapshot(this.#config.name, data);
			return Ok(undefined);
		} catch (err) {
			return Err(
				new DbError(
					`Failed to save database "${this.#config.name}" to IndexedDB`,
					err instanceof Error ? err : new Error(String(err)),
				),
			);
		}
	}

	/**
	 * Close the database and release resources.
	 *
	 * When the backend is `"idb"`, the database snapshot is persisted
	 * to IndexedDB before closing.
	 */
	async close(): Promise<void> {
		if (this.#backend === "idb") {
			await this.save();
		}
		this.#db.close();
	}

	#createTransaction(): Transaction {
		const db = this.#db;
		return {
			exec(sql: string, params?: unknown[]): Result<void, DbError> {
				try {
					db.run(sql, params as Parameters<Database["run"]>[1]);
					return Ok(undefined);
				} catch (err) {
					return Err(
						new DbError(
							`Transaction exec failed: ${sql}`,
							err instanceof Error ? err : new Error(String(err)),
						),
					);
				}
			},
			query<T>(sql: string, params?: unknown[]): Result<T[], DbError> {
				try {
					const results = db.exec(sql, params as Parameters<Database["exec"]>[1]);
					return Ok(mapResultRows<T>(results));
				} catch (err) {
					return Err(
						new DbError(
							`Transaction query failed: ${sql}`,
							err instanceof Error ? err : new Error(String(err)),
						),
					);
				}
			},
		};
	}
}

/**
 * Resolve the storage backend from configuration.
 *
 * When no backend is specified, auto-detects: uses `"idb"` if
 * the `indexedDB` global is available, otherwise `"memory"`.
 */
function resolveBackend(configured?: DbConfig["backend"]): ResolvedBackend {
	if (configured === "memory") return "memory";
	if (configured === "idb") return "idb";
	// Auto-detect: prefer IndexedDB when available
	if (typeof indexedDB !== "undefined") return "idb";
	return "memory";
}

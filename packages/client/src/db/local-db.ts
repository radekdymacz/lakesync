import { Err, Ok } from "@lakesync/core";
import type { Result } from "@lakesync/core";
import initSqlJs from "sql.js";
import type { Database } from "sql.js";
import type { DbConfig, Transaction } from "./types";
import { DbError } from "./types";

/**
 * Local SQLite database backed by sql.js (SQLite compiled to WASM).
 *
 * Currently uses in-memory mode for all operations. Future versions
 * will support OPFS and IndexedDB persistence backends.
 */
export class LocalDB {
	readonly #db: Database;
	readonly #config: DbConfig;

	private constructor(db: Database, config: DbConfig) {
		this.#db = db;
		this.#config = config;
	}

	/** The database name from configuration */
	get name(): string {
		return this.#config.name;
	}

	/**
	 * Open a new LocalDB instance.
	 *
	 * Initialises the sql.js WASM engine and creates an in-memory database.
	 * Future backends (OPFS, IndexedDB) will be selected via `config.backend`.
	 */
	static async open(config: DbConfig): Promise<Result<LocalDB, DbError>> {
		try {
			const SQL = await initSqlJs();
			const db = new SQL.Database();
			return Ok(new LocalDB(db, config));
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
			if (results.length === 0) {
				return Ok([]);
			}
			const first = results[0];
			if (!first) {
				return Ok([]);
			}
			const { columns, values } = first;
			const rows = values.map((row) => {
				const obj: Record<string, unknown> = {};
				for (let i = 0; i < columns.length; i++) {
					const col = columns[i];
					if (col !== undefined) {
						obj[col] = row[i];
					}
				}
				return obj as T;
			});
			return Ok(rows);
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

	/** Close the database and release resources */
	async close(): Promise<void> {
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
					if (results.length === 0) {
						return Ok([]);
					}
					const first = results[0];
					if (!first) {
						return Ok([]);
					}
					const { columns, values } = first;
					const rows = values.map((row) => {
						const obj: Record<string, unknown> = {};
						for (let i = 0; i < columns.length; i++) {
							const col = columns[i];
							if (col !== undefined) {
								obj[col] = row[i];
							}
						}
						return obj as T;
					});
					return Ok(rows);
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

import type { Result } from "@lakesync/core";
import { LakeSyncError } from "@lakesync/core";

/** Configuration for opening a local database */
export interface DbConfig {
	/** Database name (used for identification and future persistence) */
	name: string;
	/** Storage backend — auto-detected if not set */
	backend?: "idb" | "memory";
	/** Optional URL to the sql-wasm.wasm binary (for custom hosting) */
	wasmUrl?: string;
}

/** Error type for database operations */
export class DbError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "DB_ERROR", cause);
	}
}

/** Synchronous transaction interface wrapping sql.js operations */
export interface Transaction {
	/** Execute a SQL statement with optional parameters */
	exec(sql: string, params?: unknown[]): Result<void, DbError>;
	/** Query rows from the database with optional parameters */
	query<T>(sql: string, params?: unknown[]): Result<T[], DbError>;
}

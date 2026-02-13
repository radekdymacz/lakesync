import type { AdapterError, ColumnDelta, Result, TableSchema } from "@lakesync/core";

/**
 * Cursor state for CDC position tracking. Shape is dialect-specific
 * (e.g. `{ lsn: "0/16B3748" }` for Postgres, `{ gtid: "..." }` for MySQL).
 */
export type CdcCursor = Record<string, unknown>;

/** A single captured change from the database. */
export interface CdcRawChange {
	kind: "insert" | "update" | "delete";
	schema: string;
	table: string;
	rowId: string;
	columns: ColumnDelta[];
}

/** A batch of changes returned by fetchChanges. */
export interface CdcChangeBatch {
	changes: CdcRawChange[];
	cursor: CdcCursor;
}

/**
 * CDC dialect — encapsulates database-specific change capture mechanism.
 * Same pattern as {@link import("../materialise").SqlDialect} for materialisation.
 *
 * Implement this interface to add CDC support for a new database engine.
 * The generic {@link import("./cdc-source").CdcSource} handles polling,
 * delta conversion, and cursor tracking.
 */
export interface CdcDialect {
	/** Dialect identifier (e.g. `"postgres"`, `"mysql"`). */
	readonly name: string;

	/** Establish a connection to the database. */
	connect(): Promise<Result<void, AdapterError>>;

	/**
	 * Ensure CDC capture is configured for the given tables.
	 * For Postgres this creates a replication slot; for MySQL it verifies binlog settings, etc.
	 *
	 * @param tables - Tables to capture, or `null` for all tables.
	 */
	ensureCapture(tables: string[] | null): Promise<Result<void, AdapterError>>;

	/**
	 * Fetch changes since the given cursor position.
	 *
	 * Returns raw changes (not RowDeltas) — the generic CdcSource converts them.
	 * Also returns the updated cursor to track the new position.
	 *
	 * @param cursor - Current cursor position (dialect-specific shape).
	 */
	fetchChanges(cursor: CdcCursor): Promise<Result<CdcChangeBatch, AdapterError>>;

	/**
	 * Discover available table schemas from the database.
	 *
	 * @param tables - Tables to discover, or `null` for all tables.
	 */
	discoverSchemas(tables: string[] | null): Promise<Result<TableSchema[], AdapterError>>;

	/** Close the connection and release resources. */
	close(): Promise<void>;

	/** Return the default cursor for a fresh start (no prior position). */
	defaultCursor(): CdcCursor;
}

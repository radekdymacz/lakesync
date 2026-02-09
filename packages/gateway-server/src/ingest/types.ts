// ---------------------------------------------------------------------------
// Source Polling Ingest — Type Definitions
// ---------------------------------------------------------------------------

/** Generic query function — abstracts any SQL database. */
export type QueryFn = (sql: string, params?: unknown[]) => Promise<Record<string, unknown>[]>;

/** Configuration for a single polling ingest source. */
export interface IngestSourceConfig {
	/** Unique name for this source (used as clientId prefix). */
	name: string;
	/** Generic query function for the source database. */
	queryFn: QueryFn;
	/** Tables to poll. */
	tables: IngestTableConfig[];
	/** Poll interval in ms. Default 10_000 (10s). */
	intervalMs?: number;
}

/** Configuration for a single table within an ingest source. */
export interface IngestTableConfig {
	/** Target table name in LakeSync (where deltas appear). */
	table: string;
	/** SQL query to fetch rows. Must return a row ID column + data columns. */
	query: string;
	/** Column used as the unique row identifier. Default: "id". */
	rowIdColumn?: string;
	/** Change detection strategy. */
	strategy: CursorStrategy | DiffStrategy;
}

/** Cursor-based change detection — fast, requires a monotonically increasing column. */
export interface CursorStrategy {
	type: "cursor";
	/** Column name for cursor (e.g. "updated_at"). Must be monotonically increasing. */
	cursorColumn: string;
	/** Look-back overlap in ms to catch late-committing transactions. Default 5000. */
	lookbackMs?: number;
}

/** Full-diff change detection — slower, detects deletes, no schema requirement. */
export interface DiffStrategy {
	type: "diff";
}

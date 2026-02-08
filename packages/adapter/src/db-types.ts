import type { AdapterError, HLCTimestamp, Result, RowDelta, TableSchema } from "@lakesync/core";

/** Configuration for a database adapter connection. */
export interface DatabaseAdapterConfig {
	/** Connection string (e.g. postgres://user:pass@host/db) */
	connectionString: string;
}

/**
 * Abstract interface for SQL database storage operations.
 * Alternative to LakeAdapter for small-data backends (Postgres, MySQL, etc).
 */
export interface DatabaseAdapter {
	/** Insert deltas into the database in a single batch. Idempotent via deltaId uniqueness. */
	insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>>;

	/** Query deltas with HLC greater than the given timestamp, optionally filtered by table. */
	queryDeltasSince(hlc: HLCTimestamp, tables?: string[]): Promise<Result<RowDelta[], AdapterError>>;

	/** Get the latest merged state for a specific row. Returns null if the row doesn't exist. */
	getLatestState(
		table: string,
		rowId: string,
	): Promise<Result<Record<string, unknown> | null, AdapterError>>;

	/** Ensure the database schema matches the given TableSchema. Creates/alters tables as needed. */
	ensureSchema(schema: TableSchema): Promise<Result<void, AdapterError>>;

	/** Close the database connection and release resources. */
	close(): Promise<void>;
}

/**
 * Map a LakeSync column type to a BigQuery column definition.
 */
export function lakeSyncTypeToBigQuery(type: TableSchema["columns"][number]["type"]): string {
	switch (type) {
		case "string":
			return "STRING";
		case "number":
			return "FLOAT64";
		case "boolean":
			return "BOOL";
		case "json":
			return "JSON";
		case "null":
			return "STRING";
	}
}

/** Type guard to distinguish DatabaseAdapter from LakeAdapter at runtime. */
export function isDatabaseAdapter(adapter: unknown): adapter is DatabaseAdapter {
	return (
		adapter !== null &&
		typeof adapter === "object" &&
		"insertDeltas" in adapter &&
		"queryDeltasSince" in adapter &&
		typeof (adapter as DatabaseAdapter).insertDeltas === "function"
	);
}

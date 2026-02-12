import type { RowDelta, TableSchema } from "./delta";
import type { HLCTimestamp } from "./hlc";
import type { AdapterError, Result } from "./result";

// ---------------------------------------------------------------------------
// LakeAdapter — abstract object-store interface (S3, R2, MinIO, etc.)
// ---------------------------------------------------------------------------

/** Information about an object in the lake store. */
export interface ObjectInfo {
	/** S3 object key */
	key: string;
	/** Object size in bytes */
	size: number;
	/** Last modification date */
	lastModified: Date;
}

/** Abstract interface for lake storage operations. */
export interface LakeAdapter {
	/** Store an object in the lake. */
	putObject(
		path: string,
		data: Uint8Array,
		contentType?: string,
	): Promise<Result<void, AdapterError>>;

	/** Retrieve an object from the lake. */
	getObject(path: string): Promise<Result<Uint8Array, AdapterError>>;

	/** Get object metadata without retrieving the body. */
	headObject(path: string): Promise<Result<{ size: number; lastModified: Date }, AdapterError>>;

	/** List objects matching a given prefix. */
	listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>>;

	/** Delete a single object from the lake. */
	deleteObject(path: string): Promise<Result<void, AdapterError>>;

	/** Delete multiple objects from the lake in a single batch operation. */
	deleteObjects(paths: string[]): Promise<Result<void, AdapterError>>;
}

// ---------------------------------------------------------------------------
// DatabaseAdapter — abstract SQL database interface (Postgres, MySQL, etc.)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Materialisable — opt-in capability for delta materialisation
// ---------------------------------------------------------------------------

/**
 * Opt-in capability for adapters that can materialise deltas into destination tables.
 *
 * Materialisation is a separate concern from delta storage — adapters that store
 * deltas (via `DatabaseAdapter.insertDeltas`) may also materialise them into
 * queryable destination tables by implementing this interface.
 *
 * Destination tables follow the hybrid column model:
 * - Synced columns (written by materialiser, derived from `TableSchema.columns`)
 * - `props JSONB DEFAULT '{}'` — consumer-extensible, never touched by materialiser
 * - `synced_at` — updated on every materialise cycle
 */
export interface Materialisable {
	/**
	 * Materialise deltas into destination tables.
	 *
	 * For each table with a matching schema, merges delta history into the
	 * latest row state and upserts into the destination table. Tombstoned
	 * rows are deleted. The `props` column is never touched.
	 *
	 * @param deltas - The deltas that were just flushed.
	 * @param schemas - Table schemas defining destination tables and column mappings.
	 */
	materialise(
		deltas: RowDelta[],
		schemas: ReadonlyArray<TableSchema>,
	): Promise<Result<void, AdapterError>>;
}

/**
 * Type guard to check if an adapter supports materialisation.
 *
 * Uses duck-typing (same pattern as `isDatabaseAdapter`).
 */
export function isMaterialisable(adapter: unknown): adapter is Materialisable {
	return (
		adapter !== null &&
		typeof adapter === "object" &&
		"materialise" in adapter &&
		typeof (adapter as Materialisable).materialise === "function"
	);
}

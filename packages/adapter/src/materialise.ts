import type { ColumnDelta } from "@lakesync/core";
import {
	type AdapterError,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import { groupAndMerge, wrapAsync } from "./shared";

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

/**
 * Resolve the primary key columns for a table schema.
 * Defaults to `["row_id"]` when not explicitly set.
 */
export function resolvePrimaryKey(schema: TableSchema): string[] {
	return schema.primaryKey ?? ["row_id"];
}

/**
 * Resolve the conflict columns used for upsert ON CONFLICT targeting.
 * When `externalIdColumn` is set, upserts resolve on that column instead of the PK.
 */
export function resolveConflictColumns(schema: TableSchema): string[] {
	return schema.externalIdColumn ? [schema.externalIdColumn] : resolvePrimaryKey(schema);
}

/**
 * Whether tombstoned rows should be soft-deleted (default) or hard-deleted.
 */
export function isSoftDelete(schema: TableSchema): boolean {
	return schema.softDelete !== false;
}

/**
 * Group deltas by their table name, collecting the set of affected row IDs per table.
 *
 * @param deltas - The deltas to group.
 * @returns A map from table name to the set of affected row IDs.
 */
export function groupDeltasByTable(deltas: ReadonlyArray<RowDelta>): Map<string, Set<string>> {
	const result = new Map<string, Set<string>>();
	for (const delta of deltas) {
		let rowIds = result.get(delta.table);
		if (!rowIds) {
			rowIds = new Set<string>();
			result.set(delta.table, rowIds);
		}
		rowIds.add(delta.rowId);
	}
	return result;
}

/**
 * Build an index from source table name to schema.
 *
 * Keys are `schema.sourceTable ?? schema.table`, so deltas can be matched
 * by their `table` field to find the correct destination schema.
 *
 * @param schemas - The table schemas to index.
 * @returns A map from source table name to schema.
 */
export function buildSchemaIndex(schemas: ReadonlyArray<TableSchema>): Map<string, TableSchema> {
	const index = new Map<string, TableSchema>();
	for (const schema of schemas) {
		const key = schema.sourceTable ?? schema.table;
		index.set(key, schema);
	}
	return index;
}

// ---------------------------------------------------------------------------
// Shared materialise algorithm: SqlDialect + QueryExecutor + executeMaterialise
// ---------------------------------------------------------------------------

/** Minimal query interface for executing SQL against a database. */
export interface QueryExecutor {
	query(sql: string, params: unknown[]): Promise<void>;
	queryRows(
		sql: string,
		params: unknown[],
	): Promise<Array<{ row_id: string; columns: string | ColumnDelta[]; op: string }>>;
}

/**
 * SQL dialect interface — encapsulates the syntactic differences between
 * Postgres, MySQL, and BigQuery for the materialise algorithm.
 */
export interface SqlDialect {
	/** Generate CREATE TABLE IF NOT EXISTS for the destination table. */
	createDestinationTable(
		dest: string,
		schema: TableSchema,
		pk: string[],
		softDelete: boolean,
	): { sql: string; params: unknown[] };

	/** Generate a query to fetch delta history for a set of affected row IDs. */
	queryDeltaHistory(sourceTable: string, rowIds: string[]): { sql: string; params: unknown[] };

	/** Generate an upsert statement for the merged row states. */
	buildUpsert(
		dest: string,
		schema: TableSchema,
		conflictCols: string[],
		softDelete: boolean,
		upserts: Array<{ rowId: string; state: Record<string, unknown> }>,
	): { sql: string; params: unknown[] };

	/** Generate a delete (hard or soft) statement for tombstoned row IDs. */
	buildDelete(
		dest: string,
		deleteIds: string[],
		softDelete: boolean,
	): { sql: string; params: unknown[] };
}

/**
 * Execute the shared materialise algorithm using the provided dialect and executor.
 *
 * Algorithm: group by table -> build schema index -> for each table:
 * create dest table -> query history -> merge -> upsert -> delete.
 *
 * @param executor - Executes SQL statements against the database.
 * @param dialect - Generates dialect-specific SQL.
 * @param deltas - The deltas that were just flushed.
 * @param schemas - Table schemas defining destination tables and column mappings.
 */
export async function executeMaterialise(
	executor: QueryExecutor,
	dialect: SqlDialect,
	deltas: RowDelta[],
	schemas: ReadonlyArray<TableSchema>,
): Promise<Result<void, AdapterError>> {
	if (deltas.length === 0) {
		return Ok(undefined);
	}

	return wrapAsync(async () => {
		const grouped = groupDeltasByTable(deltas);
		const schemaIndex = buildSchemaIndex(schemas);

		for (const [tableName, rowIds] of grouped) {
			const schema = schemaIndex.get(tableName);
			if (!schema) continue;

			const dest = schema.table;
			const pk = resolvePrimaryKey(schema);
			const conflictCols = resolveConflictColumns(schema);
			const soft = isSoftDelete(schema);

			// 1. Create destination table
			const createStmt = dialect.createDestinationTable(dest, schema, pk, soft);
			await executor.query(createStmt.sql, createStmt.params);

			// 2. Query delta history for affected rows
			const sourceTable = schema.sourceTable ?? schema.table;
			const rowIdArray = [...rowIds];
			const historyStmt = dialect.queryDeltaHistory(sourceTable, rowIdArray);
			const rows = await executor.queryRows(historyStmt.sql, historyStmt.params);

			// 3. Merge to latest state
			const { upserts, deleteIds } = groupAndMerge(rows);

			// 4. Upsert live rows
			if (upserts.length > 0) {
				const upsertStmt = dialect.buildUpsert(dest, schema, conflictCols, soft, upserts);
				await executor.query(upsertStmt.sql, upsertStmt.params);
			}

			// 5. Delete / soft-delete tombstoned rows
			if (deleteIds.length > 0) {
				const deleteStmt = dialect.buildDelete(dest, deleteIds, soft);
				await executor.query(deleteStmt.sql, deleteStmt.params);
			}
		}
	}, "Failed to materialise deltas");
}

import type { AdapterError, Result, RowDelta, TableSchema } from "@lakesync/core";

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

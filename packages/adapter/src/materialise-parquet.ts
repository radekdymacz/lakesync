import {
	AdapterError,
	type ColumnDelta,
	Err,
	type LakeAdapter,
	type Materialisable,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import type { StateRow } from "@lakesync/parquet";
import { writeStateToParquet } from "@lakesync/parquet";
import { buildSchemaIndex, groupDeltasByTable } from "./materialise";

/** Configuration for the Parquet materialiser. */
export interface ParquetMaterialiserConfig {
	/**
	 * Path prefix for materialised files in the lake store.
	 * Defaults to `"materialised"`.
	 */
	pathPrefix?: string;
}

/**
 * Materialises deltas into current-state Parquet files on object storage.
 *
 * For each table with a matching schema, groups deltas by row, merges to
 * the latest column-level state, and writes a single Parquet file
 * containing the current state of all affected rows. Tombstoned rows
 * (last op = DELETE) are excluded from the output.
 *
 * Output path: `{pathPrefix}/{table}/current.parquet`
 *
 * This is NOT the same as flushing raw deltas — this produces the
 * **current state** of each row, suitable for analytical queries.
 */
export class ParquetMaterialiser implements Materialisable {
	private readonly adapter: LakeAdapter;
	private readonly pathPrefix: string;

	constructor(adapter: LakeAdapter, config?: ParquetMaterialiserConfig) {
		this.adapter = adapter;
		this.pathPrefix = config?.pathPrefix ?? "materialised";
	}

	/**
	 * Materialise deltas into current-state Parquet files.
	 *
	 * @param deltas - The deltas that were just flushed.
	 * @param schemas - Table schemas defining destination tables and column mappings.
	 */
	async materialise(
		deltas: RowDelta[],
		schemas: ReadonlyArray<TableSchema>,
	): Promise<Result<void, AdapterError>> {
		if (deltas.length === 0) return Ok(undefined);

		try {
			const grouped = groupDeltasByTable(deltas);
			const schemaIndex = buildSchemaIndex(schemas);

			for (const [tableName] of grouped) {
				const schema = schemaIndex.get(tableName);
				if (!schema) continue;

				// Filter deltas for this table
				const tableDeltas = deltas.filter((d) => d.table === tableName);

				// Merge to current state per row
				const rows = mergeToCurrentState(tableDeltas);
				if (rows.length === 0) continue;

				// Write Parquet
				const parquetResult = await writeStateToParquet(rows, schema);
				if (!parquetResult.ok) {
					return Err(
						new AdapterError(
							`Failed to write Parquet for table "${tableName}": ${parquetResult.error.message}`,
						),
					);
				}

				// Upload to lake
				const dest = schema.table;
				const objectKey = `${this.pathPrefix}/${dest}/current.parquet`;
				const putResult = await this.adapter.putObject(
					objectKey,
					parquetResult.value,
					"application/vnd.apache.parquet",
				);
				if (!putResult.ok) {
					return Err(
						new AdapterError(
							`Failed to upload materialised Parquet for table "${dest}": ${putResult.error.message}`,
						),
					);
				}
			}

			return Ok(undefined);
		} catch (error: unknown) {
			const msg = error instanceof Error ? error.message : String(error);
			return Err(new AdapterError(`Parquet materialisation failed: ${msg}`));
		}
	}
}

/**
 * Merge deltas for a single table into current-state rows.
 *
 * Groups deltas by rowId, applies column-level LWW in delta order,
 * and excludes tombstoned rows (last op = DELETE).
 */
function mergeToCurrentState(deltas: RowDelta[]): StateRow[] {
	const byRowId = new Map<string, Array<{ columns: ColumnDelta[]; op: string }>>();

	for (const delta of deltas) {
		let arr = byRowId.get(delta.rowId);
		if (!arr) {
			arr = [];
			byRowId.set(delta.rowId, arr);
		}
		arr.push({ columns: delta.columns, op: delta.op });
	}

	const rows: StateRow[] = [];

	for (const [rowId, group] of byRowId) {
		const lastOp = group[group.length - 1]!.op;
		if (lastOp === "DELETE") continue;

		const state: Record<string, unknown> = {};
		for (const entry of group) {
			if (entry.op === "DELETE") {
				for (const key of Object.keys(state)) {
					delete state[key];
				}
				continue;
			}
			for (const col of entry.columns) {
				state[col.column] = col.value;
			}
		}

		rows.push({ rowId, state });
	}

	return rows;
}

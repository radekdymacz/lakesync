import { BigQuery } from "@google-cloud/bigquery";
import {
	type AdapterError,
	type ColumnDelta,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import type { DatabaseAdapter } from "./db-types";
import { lakeSyncTypeToBigQuery } from "./db-types";
import type { Materialisable } from "./materialise";
import {
	buildSchemaIndex,
	groupDeltasByTable,
	isSoftDelete,
	resolveConflictColumns,
	resolvePrimaryKey,
} from "./materialise";
import { mergeLatestState, wrapAsync } from "./shared";

/**
 * Configuration for the BigQuery adapter.
 * Unlike SQL adapters, BigQuery is HTTP-based — no connection string needed.
 */
export interface BigQueryAdapterConfig {
	/** GCP project ID. */
	projectId: string;
	/** BigQuery dataset name. */
	dataset: string;
	/** Path to a service account JSON key file. Falls back to ADC if omitted. */
	keyFilename?: string;
	/** Dataset location (default: "US"). */
	location?: string;
}

/** Shape of a row returned from the lakesync_deltas table. */
interface BigQueryDeltaRow {
	delta_id: string;
	table: string;
	row_id: string;
	columns: string;
	hlc: { value: string } | string | number;
	client_id: string;
	op: string;
}

/**
 * Convert a raw BigQuery row into a RowDelta.
 * BigQuery returns INT64 as `{ value: string }` objects to avoid precision loss.
 */
function rowToRowDelta(row: BigQueryDeltaRow): RowDelta {
	const columns: ColumnDelta[] =
		typeof row.columns === "string" ? JSON.parse(row.columns) : row.columns;

	// BigQuery INT64 comes back as { value: "123" } to preserve precision
	const hlcRaw = row.hlc;
	const hlcString =
		typeof hlcRaw === "object" && hlcRaw !== null && "value" in hlcRaw
			? hlcRaw.value
			: String(hlcRaw);

	return {
		deltaId: row.delta_id,
		table: row.table,
		rowId: row.row_id,
		columns,
		hlc: BigInt(hlcString) as HLCTimestamp,
		clientId: row.client_id,
		op: row.op as RowDelta["op"],
	};
}

/**
 * BigQuery database adapter for LakeSync.
 *
 * Stores deltas in a `lakesync_deltas` table using standard SQL DML.
 * Idempotent inserts via MERGE statement. All public methods return
 * `Result` and never throw.
 *
 * **Note:** BigQuery DML is limited to 1,500 statements per table per day
 * on standard (non-partitioned) tables. Query latency is seconds, not
 * milliseconds — this adapter is designed for the analytics tier.
 */
export class BigQueryAdapter implements DatabaseAdapter, Materialisable {
	/** @internal */
	readonly client: BigQuery;
	/** @internal */
	readonly dataset: string;
	/** @internal */
	readonly location: string;

	constructor(config: BigQueryAdapterConfig) {
		this.client = new BigQuery({
			projectId: config.projectId,
			keyFilename: config.keyFilename,
		});
		this.dataset = config.dataset;
		this.location = config.location ?? "US";
	}

	/**
	 * Insert deltas into the database in a single batch.
	 * Idempotent via MERGE — existing deltaIds are silently skipped.
	 */
	async insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>> {
		if (deltas.length === 0) {
			return Ok(undefined);
		}

		return wrapAsync(async () => {
			// Build MERGE source from UNION ALL of parameterised SELECTs
			const params: Record<string, string> = {};
			const selects: string[] = [];

			for (let i = 0; i < deltas.length; i++) {
				const d = deltas[i]!;
				params[`did_${i}`] = d.deltaId;
				params[`tbl_${i}`] = d.table;
				params[`rid_${i}`] = d.rowId;
				params[`col_${i}`] = JSON.stringify(d.columns);
				params[`hlc_${i}`] = d.hlc.toString();
				params[`cid_${i}`] = d.clientId;
				params[`op_${i}`] = d.op;

				selects.push(
					`SELECT @did_${i} AS delta_id, @tbl_${i} AS \`table\`, @rid_${i} AS row_id, @col_${i} AS columns, CAST(@hlc_${i} AS INT64) AS hlc, @cid_${i} AS client_id, @op_${i} AS op`,
				);
			}

			const sql = `MERGE \`${this.dataset}.lakesync_deltas\` AS target
USING (${selects.join(" UNION ALL ")}) AS source
ON target.delta_id = source.delta_id
WHEN NOT MATCHED THEN INSERT (delta_id, \`table\`, row_id, columns, hlc, client_id, op)
VALUES (source.delta_id, source.\`table\`, source.row_id, source.columns, source.hlc, source.client_id, source.op)`;

			await this.client.query({ query: sql, params, location: this.location });
		}, "Failed to insert deltas");
	}

	/**
	 * Query deltas with HLC greater than the given timestamp, optionally filtered by table.
	 */
	async queryDeltasSince(
		hlc: HLCTimestamp,
		tables?: string[],
	): Promise<Result<RowDelta[], AdapterError>> {
		return wrapAsync(async () => {
			let sql: string;
			const params: Record<string, string | string[]> = {
				sinceHlc: hlc.toString(),
			};

			if (tables && tables.length > 0) {
				sql = `SELECT delta_id, \`table\`, row_id, columns, hlc, client_id, op
FROM \`${this.dataset}.lakesync_deltas\`
WHERE hlc > CAST(@sinceHlc AS INT64) AND \`table\` IN UNNEST(@tables)
ORDER BY hlc ASC`;
				params.tables = tables;
			} else {
				sql = `SELECT delta_id, \`table\`, row_id, columns, hlc, client_id, op
FROM \`${this.dataset}.lakesync_deltas\`
WHERE hlc > CAST(@sinceHlc AS INT64)
ORDER BY hlc ASC`;
			}

			const [rows] = await this.client.query({
				query: sql,
				params,
				location: this.location,
			});
			return (rows as BigQueryDeltaRow[]).map(rowToRowDelta);
		}, "Failed to query deltas");
	}

	/**
	 * Get the latest merged state for a specific row using column-level LWW.
	 * Returns null if no deltas exist for this row.
	 */
	async getLatestState(
		table: string,
		rowId: string,
	): Promise<Result<Record<string, unknown> | null, AdapterError>> {
		return wrapAsync(async () => {
			const sql = `SELECT columns, hlc, client_id, op
FROM \`${this.dataset}.lakesync_deltas\`
WHERE \`table\` = @tbl AND row_id = @rid
ORDER BY hlc ASC`;

			const [rows] = await this.client.query({
				query: sql,
				params: { tbl: table, rid: rowId },
				location: this.location,
			});
			return mergeLatestState(rows as BigQueryDeltaRow[]);
		}, `Failed to get latest state for ${table}:${rowId}`);
	}

	/**
	 * Ensure the BigQuery dataset and lakesync_deltas table exist.
	 * The `schema` parameter is accepted for interface compliance but the
	 * internal table structure is fixed (deltas store column data as JSON).
	 */
	async ensureSchema(_schema: TableSchema): Promise<Result<void, AdapterError>> {
		return wrapAsync(async () => {
			// Create dataset if it doesn't exist
			const datasetRef = this.client.dataset(this.dataset);
			const [datasetExists] = await datasetRef.exists();
			if (!datasetExists) {
				await this.client.createDataset(this.dataset, {
					location: this.location,
				});
			}

			// Create the deltas table
			await this.client.query({
				query: `CREATE TABLE IF NOT EXISTS \`${this.dataset}.lakesync_deltas\` (
	delta_id STRING NOT NULL,
	\`table\` STRING NOT NULL,
	row_id STRING NOT NULL,
	columns JSON NOT NULL,
	hlc INT64 NOT NULL,
	client_id STRING NOT NULL,
	op STRING NOT NULL
)
CLUSTER BY \`table\`, hlc`,
				location: this.location,
			});
		}, "Failed to ensure schema");
	}

	/**
	 * Materialise deltas into destination tables.
	 *
	 * For each affected table, queries the full delta history for touched rows,
	 * merges to latest state via column-level LWW, then upserts live rows and
	 * deletes tombstoned rows. The consumer-owned `props` column is never
	 * touched on UPDATE.
	 */
	async materialise(
		deltas: RowDelta[],
		schemas: ReadonlyArray<TableSchema>,
	): Promise<Result<void, AdapterError>> {
		if (deltas.length === 0) {
			return Ok(undefined);
		}

		return wrapAsync(async () => {
			const tableRowIds = groupDeltasByTable(deltas);
			const schemaIndex = buildSchemaIndex(schemas);

			for (const [sourceTable, rowIds] of tableRowIds) {
				const schema = schemaIndex.get(sourceTable);
				if (!schema) continue;

				const pk = resolvePrimaryKey(schema);
				const conflictCols = resolveConflictColumns(schema);
				const soft = isSoftDelete(schema);

				// Ensure destination table exists
				const colDefs = schema.columns
					.map((c) => `${c.name} ${lakeSyncTypeToBigQuery(c.type)}`)
					.join(", ");
				const deletedAtCol = soft ? `,\n\tdeleted_at TIMESTAMP` : "";
				await this.client.query({
					query: `CREATE TABLE IF NOT EXISTS \`${this.dataset}.${schema.table}\` (
	row_id STRING NOT NULL,
	${colDefs},
	props JSON DEFAULT '{}'${deletedAtCol},
	synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY ${pk.map((c) => (c === "row_id" ? "row_id" : c)).join(", ")}`,
					location: this.location,
				});

				// Query delta history for affected rows
				const rowIdArray = [...rowIds];
				const [deltaRows] = await this.client.query({
					query: `SELECT row_id, columns, op FROM \`${this.dataset}.lakesync_deltas\`
WHERE \`table\` = @sourceTable AND row_id IN UNNEST(@rowIds)
ORDER BY hlc ASC`,
					params: { sourceTable, rowIds: rowIdArray },
					location: this.location,
				});

				// Group by row_id and merge to latest state
				const rowGroups = new Map<string, Array<{ columns: string | ColumnDelta[]; op: string }>>();
				for (const row of deltaRows as Array<{
					row_id: string;
					columns: string | ColumnDelta[];
					op: string;
				}>) {
					let group = rowGroups.get(row.row_id);
					if (!group) {
						group = [];
						rowGroups.set(row.row_id, group);
					}
					group.push({ columns: row.columns, op: row.op });
				}

				const upserts: Array<{ rowId: string; state: Record<string, unknown> }> = [];
				const deleteRowIds: string[] = [];

				for (const [rowId, group] of rowGroups) {
					const state = mergeLatestState(group);
					if (state === null) {
						deleteRowIds.push(rowId);
					} else {
						upserts.push({ rowId, state });
					}
				}

				// MERGE upserts
				if (upserts.length > 0) {
					const params: Record<string, unknown> = {};
					const selects: string[] = [];

					for (let i = 0; i < upserts.length; i++) {
						const u = upserts[i]!;
						params[`rid_${i}`] = u.rowId;
						for (const col of schema.columns) {
							params[`c${schema.columns.indexOf(col)}_${i}`] = u.state[col.name] ?? null;
						}

						const colSelects = schema.columns
							.map((col, ci) => `@c${ci}_${i} AS ${col.name}`)
							.join(", ");
						const deletedAtSelect = soft ? ", CAST(NULL AS TIMESTAMP) AS deleted_at" : "";
						selects.push(
							`SELECT @rid_${i} AS row_id, ${colSelects}${deletedAtSelect}, CURRENT_TIMESTAMP() AS synced_at`,
						);
					}

					const mergeOn = conflictCols
						.map((c) => `t.${c === "row_id" ? "row_id" : c} = s.${c === "row_id" ? "row_id" : c}`)
						.join(" AND ");

					const updateSet = schema.columns.map((col) => `${col.name} = s.${col.name}`).join(", ");
					const softUpdateExtra = soft ? ", deleted_at = s.deleted_at" : "";

					const insertColsList = [
						"row_id",
						...schema.columns.map((c) => c.name),
						"props",
						...(soft ? ["deleted_at"] : []),
						"synced_at",
					].join(", ");
					const insertValsList = [
						"s.row_id",
						...schema.columns.map((c) => `s.${c.name}`),
						"'{}'",
						...(soft ? ["s.deleted_at"] : []),
						"s.synced_at",
					].join(", ");

					const mergeSql = `MERGE \`${this.dataset}.${schema.table}\` AS t
USING (${selects.join(" UNION ALL ")}) AS s
ON ${mergeOn}
WHEN MATCHED THEN UPDATE SET ${updateSet}${softUpdateExtra}, synced_at = s.synced_at
WHEN NOT MATCHED THEN INSERT (${insertColsList})
VALUES (${insertValsList})`;

					await this.client.query({
						query: mergeSql,
						params,
						location: this.location,
					});
				}

				// DELETE / soft-delete tombstoned rows
				if (deleteRowIds.length > 0) {
					if (soft) {
						await this.client.query({
							query: `UPDATE \`${this.dataset}.${schema.table}\` SET deleted_at = CURRENT_TIMESTAMP(), synced_at = CURRENT_TIMESTAMP() WHERE row_id IN UNNEST(@rowIds)`,
							params: { rowIds: deleteRowIds },
							location: this.location,
						});
					} else {
						await this.client.query({
							query: `DELETE FROM \`${this.dataset}.${schema.table}\` WHERE row_id IN UNNEST(@rowIds)`,
							params: { rowIds: deleteRowIds },
							location: this.location,
						});
					}
				}
			}
		}, "Failed to materialise deltas");
	}

	/**
	 * No-op — BigQuery client is HTTP-based with no persistent connections.
	 */
	async close(): Promise<void> {
		// No-op: BigQuery uses HTTP requests, no connection pool to close
	}
}

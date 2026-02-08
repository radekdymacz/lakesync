import { BigQuery } from "@google-cloud/bigquery";
import {
	AdapterError,
	type ColumnDelta,
	Err,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import type { DatabaseAdapter } from "./db-types";

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

/**
 * Normalise a caught value into an Error or undefined.
 * Used as the `cause` argument for AdapterError.
 */
function toCause(error: unknown): Error | undefined {
	return error instanceof Error ? error : undefined;
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
export class BigQueryAdapter implements DatabaseAdapter {
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
	 * Execute a database operation and wrap any thrown error into an AdapterError Result.
	 */
	private async wrap<T>(
		operation: () => Promise<T>,
		errorMessage: string,
	): Promise<Result<T, AdapterError>> {
		try {
			const value = await operation();
			return Ok(value);
		} catch (error) {
			if (error instanceof AdapterError) {
				return Err(error);
			}
			return Err(new AdapterError(errorMessage, toCause(error)));
		}
	}

	/**
	 * Insert deltas into the database in a single batch.
	 * Idempotent via MERGE — existing deltaIds are silently skipped.
	 */
	async insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>> {
		if (deltas.length === 0) {
			return Ok(undefined);
		}

		return this.wrap(async () => {
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
		return this.wrap(async () => {
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
		return this.wrap(async () => {
			const sql = `SELECT columns, hlc, client_id, op
FROM \`${this.dataset}.lakesync_deltas\`
WHERE \`table\` = @tbl AND row_id = @rid
ORDER BY hlc ASC`;

			const [rows] = await this.client.query({
				query: sql,
				params: { tbl: table, rid: rowId },
				location: this.location,
			});
			const resultRows = rows as BigQueryDeltaRow[];

			if (resultRows.length === 0) {
				return null;
			}

			// Check if the last delta is a DELETE — if so, the row is tombstoned
			const lastRow = resultRows[resultRows.length - 1]!;
			if (lastRow.op === "DELETE") {
				return null;
			}

			// Merge columns using LWW: iterate in HLC order, later values overwrite
			const state: Record<string, unknown> = {};

			for (const row of resultRows) {
				if (row.op === "DELETE") {
					for (const key of Object.keys(state)) {
						delete state[key];
					}
					continue;
				}

				const columns: ColumnDelta[] =
					typeof row.columns === "string" ? JSON.parse(row.columns) : row.columns;

				for (const col of columns) {
					state[col.column] = col.value;
				}
			}

			return state;
		}, `Failed to get latest state for ${table}:${rowId}`);
	}

	/**
	 * Ensure the BigQuery dataset and lakesync_deltas table exist.
	 * The `schema` parameter is accepted for interface compliance but the
	 * internal table structure is fixed (deltas store column data as JSON).
	 */
	async ensureSchema(_schema: TableSchema): Promise<Result<void, AdapterError>> {
		return this.wrap(async () => {
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
	 * No-op — BigQuery client is HTTP-based with no persistent connections.
	 */
	async close(): Promise<void> {
		// No-op: BigQuery uses HTTP requests, no connection pool to close
	}
}

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
import mysql from "mysql2/promise";
import type { DatabaseAdapter, DatabaseAdapterConfig } from "./db-types";

/**
 * Normalise a caught value into an Error or undefined.
 * Used as the `cause` argument for AdapterError.
 */
function toCause(error: unknown): Error | undefined {
	return error instanceof Error ? error : undefined;
}

/**
 * Map a LakeSync column type to a MySQL column definition.
 */
function lakeSyncTypeToMySQL(type: TableSchema["columns"][number]["type"]): string {
	switch (type) {
		case "string":
			return "TEXT";
		case "number":
			return "DOUBLE";
		case "boolean":
			return "TINYINT(1)";
		case "json":
			return "JSON";
		case "null":
			return "TEXT";
	}
}

/**
 * MySQL database adapter for LakeSync.
 *
 * Stores deltas in a `lakesync_deltas` table using INSERT IGNORE for
 * idempotent writes. All public methods return `Result` and never throw.
 * Uses mysql2/promise connection pool for async operations.
 */
export class MySQLAdapter implements DatabaseAdapter {
	/** @internal */
	readonly pool: mysql.Pool;

	constructor(config: DatabaseAdapterConfig) {
		this.pool = mysql.createPool(config.connectionString);
	}

	/**
	 * Execute a database operation and wrap any thrown error into an AdapterError Result.
	 * Every public method delegates here so error handling is consistent.
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
	 * Uses INSERT IGNORE for idempotent writes — duplicate deltaIds are silently skipped.
	 */
	async insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>> {
		if (deltas.length === 0) {
			return Ok(undefined);
		}

		return this.wrap(async () => {
			const sql = `INSERT IGNORE INTO lakesync_deltas (delta_id, \`table\`, row_id, columns, hlc, client_id, op) VALUES ${deltas.map(() => "(?, ?, ?, ?, ?, ?, ?)").join(", ")}`;

			const values: unknown[] = [];
			for (const delta of deltas) {
				values.push(
					delta.deltaId,
					delta.table,
					delta.rowId,
					JSON.stringify(delta.columns),
					delta.hlc.toString(),
					delta.clientId,
					delta.op,
				);
			}

			await this.pool.execute(sql, values);
		}, "Failed to insert deltas");
	}

	/**
	 * Query deltas with HLC greater than the given timestamp.
	 * Optionally filtered by table name(s).
	 */
	async queryDeltasSince(
		hlc: HLCTimestamp,
		tables?: string[],
	): Promise<Result<RowDelta[], AdapterError>> {
		return this.wrap(async () => {
			let sql =
				"SELECT delta_id, `table`, row_id, columns, hlc, client_id, op FROM lakesync_deltas WHERE hlc > ?";
			const params: unknown[] = [hlc.toString()];

			if (tables && tables.length > 0) {
				sql += ` AND \`table\` IN (${tables.map(() => "?").join(", ")})`;
				params.push(...tables);
			}

			sql += " ORDER BY hlc ASC";

			const [rows] = await this.pool.execute(sql, params);
			return (rows as MySQLDeltaRow[]).map(rowToDelta);
		}, "Failed to query deltas");
	}

	/**
	 * Get the latest merged state for a specific row using column-level LWW.
	 * Returns null if no deltas exist or if the row is tombstoned by DELETE.
	 */
	async getLatestState(
		table: string,
		rowId: string,
	): Promise<Result<Record<string, unknown> | null, AdapterError>> {
		return this.wrap(async () => {
			const sql =
				"SELECT columns, hlc, client_id, op FROM lakesync_deltas WHERE `table` = ? AND row_id = ? ORDER BY hlc ASC";
			const [rows] = await this.pool.execute(sql, [table, rowId]);
			const resultRows = rows as MySQLDeltaRow[];

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
	 * Ensure the database schema exists. Creates the lakesync_deltas table
	 * and a user table matching the given TableSchema definition.
	 */
	async ensureSchema(schema: TableSchema): Promise<Result<void, AdapterError>> {
		return this.wrap(async () => {
			// Create the deltas table
			await this.pool.execute(`
				CREATE TABLE IF NOT EXISTS lakesync_deltas (
					delta_id VARCHAR(255) PRIMARY KEY,
					\`table\` VARCHAR(255) NOT NULL,
					row_id VARCHAR(255) NOT NULL,
					columns JSON NOT NULL,
					hlc BIGINT NOT NULL,
					client_id VARCHAR(255) NOT NULL,
					op VARCHAR(50) NOT NULL DEFAULT 'upsert',
					INDEX idx_hlc (hlc),
					INDEX idx_table_row (\`table\`, row_id)
				)
			`);

			// Create the user-defined table
			const columnDefs = schema.columns
				.map((col) => `\`${col.name}\` ${lakeSyncTypeToMySQL(col.type)}`)
				.join(", ");

			await this.pool.execute(
				`CREATE TABLE IF NOT EXISTS \`${schema.table}\` (row_id VARCHAR(255) PRIMARY KEY, ${columnDefs})`,
			);
		}, `Failed to ensure schema for table ${schema.table}`);
	}

	/** Close the database connection pool and release resources. */
	async close(): Promise<void> {
		await this.pool.end();
	}
}

/** Shape of a row returned from the lakesync_deltas table. */
interface MySQLDeltaRow {
	delta_id: string;
	table: string;
	row_id: string;
	columns: string;
	hlc: string | bigint;
	client_id: string;
	op: string;
}

/**
 * Convert a raw MySQL row into a RowDelta.
 * Handles both string and bigint HLC representations.
 */
function rowToDelta(row: MySQLDeltaRow): RowDelta {
	return {
		deltaId: row.delta_id,
		table: row.table,
		rowId: row.row_id,
		columns: typeof row.columns === "string" ? JSON.parse(row.columns) : row.columns,
		hlc: BigInt(row.hlc) as HLCTimestamp,
		clientId: row.client_id,
		op: row.op as RowDelta["op"],
	};
}

import {
	type AdapterError,
	type ColumnDelta,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import mysql from "mysql2/promise";
import type { DatabaseAdapter, DatabaseAdapterConfig } from "./db-types";
import type { Materialisable } from "./materialise";
import { executeMaterialise, type QueryExecutor, type SqlDialect } from "./materialise";
import { mergeLatestState, wrapAsync } from "./shared";

/**
 * Map a LakeSync column type to a MySQL column definition.
 */
const MYSQL_TYPE_MAP: Record<TableSchema["columns"][number]["type"], string> = {
	string: "TEXT",
	number: "DOUBLE",
	boolean: "TINYINT(1)",
	json: "JSON",
	null: "TEXT",
};

function lakeSyncTypeToMySQL(type: TableSchema["columns"][number]["type"]): string {
	return MYSQL_TYPE_MAP[type];
}

/**
 * MySQL SQL dialect for the shared materialise algorithm.
 *
 * Uses `?` positional parameters, `ON DUPLICATE KEY UPDATE`,
 * `JSON` type, and `TIMESTAMP` types.
 */
export class MySqlDialect implements SqlDialect {
	createDestinationTable(
		dest: string,
		schema: TableSchema,
		pk: string[],
		softDelete: boolean,
	): { sql: string; params: unknown[] } {
		const typedCols = schema.columns
			.map((col) => `\`${col.name}\` ${lakeSyncTypeToMySQL(col.type)}`)
			.join(", ");

		const pkConstraint = `PRIMARY KEY (${pk.map((c) => `\`${c}\``).join(", ")})`;
		const deletedAtCol = softDelete ? `, deleted_at TIMESTAMP NULL` : "";
		const uniqueConstraint = schema.externalIdColumn
			? `, UNIQUE KEY (\`${schema.externalIdColumn}\`)`
			: "";

		return {
			sql: `CREATE TABLE IF NOT EXISTS \`${dest}\` (row_id VARCHAR(255) NOT NULL, ${typedCols}, props JSON NOT NULL DEFAULT ('{}')${deletedAtCol}, synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, ${pkConstraint}${uniqueConstraint})`,
			params: [],
		};
	}

	queryDeltaHistory(sourceTable: string, rowIds: string[]): { sql: string; params: unknown[] } {
		const placeholders = rowIds.map(() => "?").join(", ");
		return {
			sql: `SELECT row_id, columns, op FROM lakesync_deltas WHERE \`table\` = ? AND row_id IN (${placeholders}) ORDER BY hlc ASC`,
			params: [sourceTable, ...rowIds],
		};
	}

	buildUpsert(
		dest: string,
		schema: TableSchema,
		_conflictCols: string[],
		softDelete: boolean,
		upserts: Array<{ rowId: string; state: Record<string, unknown> }>,
	): { sql: string; params: unknown[] } {
		const cols = schema.columns.map((c) => c.name);

		const valuePlaceholders = softDelete
			? upserts.map(() => `(?, ${cols.map(() => "?").join(", ")}, NULL, NOW())`).join(", ")
			: upserts.map(() => `(?, ${cols.map(() => "?").join(", ")}, NOW())`).join(", ");

		const values: unknown[] = [];
		for (const { rowId, state } of upserts) {
			values.push(rowId);
			for (const col of cols) {
				values.push(state[col] ?? null);
			}
		}

		const updateCols = cols.map((c) => `\`${c}\` = VALUES(\`${c}\`)`).join(", ");
		const softUpdateExtra = softDelete ? ", deleted_at = NULL" : "";
		const colList = softDelete
			? `row_id, ${cols.map((c) => `\`${c}\``).join(", ")}, deleted_at, synced_at`
			: `row_id, ${cols.map((c) => `\`${c}\``).join(", ")}, synced_at`;

		return {
			sql: `INSERT INTO \`${dest}\` (${colList}) VALUES ${valuePlaceholders} ON DUPLICATE KEY UPDATE ${updateCols}${softUpdateExtra}, synced_at = VALUES(synced_at)`,
			params: values,
		};
	}

	buildDelete(
		dest: string,
		deleteIds: string[],
		softDelete: boolean,
	): { sql: string; params: unknown[] } {
		const placeholders = deleteIds.map(() => "?").join(", ");
		if (softDelete) {
			return {
				sql: `UPDATE \`${dest}\` SET deleted_at = NOW(), synced_at = NOW() WHERE row_id IN (${placeholders})`,
				params: deleteIds,
			};
		}
		return {
			sql: `DELETE FROM \`${dest}\` WHERE row_id IN (${placeholders})`,
			params: deleteIds,
		};
	}
}

/**
 * MySQL database adapter for LakeSync.
 *
 * Stores deltas in a `lakesync_deltas` table using INSERT IGNORE for
 * idempotent writes. All public methods return `Result` and never throw.
 * Uses mysql2/promise connection pool for async operations.
 */
export class MySQLAdapter implements DatabaseAdapter, Materialisable {
	/** @internal */
	readonly pool: mysql.Pool;
	private readonly dialect = new MySqlDialect();

	constructor(config: DatabaseAdapterConfig) {
		this.pool = mysql.createPool({
			uri: config.connectionString,
			connectionLimit: config.poolMax ?? 10,
			connectTimeout: config.connectionTimeoutMs ?? 30_000,
			idleTimeout: config.idleTimeoutMs ?? 10_000,
		});
	}

	private get executor(): QueryExecutor {
		const pool = this.pool;
		return {
			async query(sql: string, params: unknown[]): Promise<void> {
				await pool.execute(sql, params);
			},
			async queryRows(sql: string, params: unknown[]) {
				const [rows] = await pool.execute(sql, params);
				return rows as Array<{
					row_id: string;
					columns: string | ColumnDelta[];
					op: string;
				}>;
			},
		};
	}

	/**
	 * Insert deltas into the database in a single batch.
	 * Uses INSERT IGNORE for idempotent writes — duplicate deltaIds are silently skipped.
	 */
	async insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>> {
		if (deltas.length === 0) {
			return Ok(undefined);
		}

		return wrapAsync(async () => {
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
		return wrapAsync(async () => {
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
		return wrapAsync(async () => {
			const sql =
				"SELECT columns, hlc, client_id, op FROM lakesync_deltas WHERE `table` = ? AND row_id = ? ORDER BY hlc ASC";
			const [rows] = await this.pool.execute(sql, [table, rowId]);
			return mergeLatestState(rows as MySQLDeltaRow[]);
		}, `Failed to get latest state for ${table}:${rowId}`);
	}

	/**
	 * Ensure the database schema exists. Creates the lakesync_deltas table
	 * and a user table matching the given TableSchema definition.
	 */
	async ensureSchema(schema: TableSchema): Promise<Result<void, AdapterError>> {
		return wrapAsync(async () => {
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
				`CREATE TABLE IF NOT EXISTS \`${schema.table}\` (row_id VARCHAR(255) PRIMARY KEY, ${columnDefs}, props JSON NOT NULL DEFAULT ('{}'), synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
			);
		}, `Failed to ensure schema for table ${schema.table}`);
	}

	/**
	 * Materialise deltas into destination tables.
	 *
	 * Delegates to the shared `executeMaterialise` algorithm with the
	 * MySQL SQL dialect.
	 */
	async materialise(
		deltas: RowDelta[],
		schemas: ReadonlyArray<TableSchema>,
	): Promise<Result<void, AdapterError>> {
		return executeMaterialise(this.executor, this.dialect, deltas, schemas);
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

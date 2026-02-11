import {
	type AdapterError,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import mysql from "mysql2/promise";
import type { DatabaseAdapter, DatabaseAdapterConfig } from "./db-types";
import type { Materialisable } from "./materialise";
import {
	buildSchemaIndex,
	groupDeltasByTable,
	isSoftDelete,
	resolvePrimaryKey,
} from "./materialise";
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
 * MySQL database adapter for LakeSync.
 *
 * Stores deltas in a `lakesync_deltas` table using INSERT IGNORE for
 * idempotent writes. All public methods return `Result` and never throw.
 * Uses mysql2/promise connection pool for async operations.
 */
export class MySQLAdapter implements DatabaseAdapter, Materialisable {
	/** @internal */
	readonly pool: mysql.Pool;

	constructor(config: DatabaseAdapterConfig) {
		this.pool = mysql.createPool(config.connectionString);
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
	 * For each table with a matching schema, merges delta history into the
	 * latest row state and upserts into the destination table. Tombstoned
	 * rows are soft-deleted (default) or hard-deleted. The `props` column
	 * is never touched.
	 */
	async materialise(
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

				const pk = resolvePrimaryKey(schema);
				const soft = isSoftDelete(schema);

				// Ensure destination table exists
				const typedCols = schema.columns
					.map((col) => `\`${col.name}\` ${lakeSyncTypeToMySQL(col.type)}`)
					.join(", ");

				const pkConstraint = `PRIMARY KEY (${pk.map((c) => `\`${c}\``).join(", ")})`;
				const deletedAtCol = soft ? `, deleted_at TIMESTAMP NULL` : "";
				const uniqueConstraint = schema.externalIdColumn
					? `, UNIQUE KEY (\`${schema.externalIdColumn}\`)`
					: "";

				await this.pool.execute(
					`CREATE TABLE IF NOT EXISTS \`${schema.table}\` (row_id VARCHAR(255) NOT NULL, ${typedCols}, props JSON NOT NULL DEFAULT ('{}')${deletedAtCol}, synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, ${pkConstraint}${uniqueConstraint})`,
				);

				// Query delta history for affected rows
				const rowIdArray = [...rowIds];
				const placeholders = rowIdArray.map(() => "?").join(", ");
				const [rows] = await this.pool.execute(
					`SELECT row_id, columns, op FROM lakesync_deltas WHERE \`table\` = ? AND row_id IN (${placeholders}) ORDER BY hlc ASC`,
					[tableName, ...rowIdArray],
				);

				// Group by row_id and merge
				const byRow = new Map<string, Array<{ columns: string; op: string }>>();
				for (const row of rows as Array<{ row_id: string; columns: string; op: string }>) {
					let list = byRow.get(row.row_id);
					if (!list) {
						list = [];
						byRow.set(row.row_id, list);
					}
					list.push(row);
				}

				const upserts: Array<{ rowId: string; state: Record<string, unknown> }> = [];
				const deleteIds: string[] = [];

				for (const [rowId, rowDeltas] of byRow) {
					const state = mergeLatestState(rowDeltas);
					if (state === null) {
						deleteIds.push(rowId);
					} else {
						upserts.push({ rowId, state });
					}
				}

				// UPSERT rows
				if (upserts.length > 0) {
					const cols = schema.columns.map((c) => c.name);
					const valuePlaceholders = soft
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
					const softUpdateExtra = soft ? ", deleted_at = NULL" : "";
					const colList = soft
						? `row_id, ${cols.map((c) => `\`${c}\``).join(", ")}, deleted_at, synced_at`
						: `row_id, ${cols.map((c) => `\`${c}\``).join(", ")}, synced_at`;

					await this.pool.execute(
						`INSERT INTO \`${schema.table}\` (${colList}) VALUES ${valuePlaceholders} ON DUPLICATE KEY UPDATE ${updateCols}${softUpdateExtra}, synced_at = VALUES(synced_at)`,
						values,
					);
				}

				// DELETE / soft-delete tombstoned rows
				if (deleteIds.length > 0) {
					const delPlaceholders = deleteIds.map(() => "?").join(", ");
					if (soft) {
						await this.pool.execute(
							`UPDATE \`${schema.table}\` SET deleted_at = NOW(), synced_at = NOW() WHERE row_id IN (${delPlaceholders})`,
							deleteIds,
						);
					} else {
						await this.pool.execute(
							`DELETE FROM \`${schema.table}\` WHERE row_id IN (${delPlaceholders})`,
							deleteIds,
						);
					}
				}
			}
		}, "Failed to materialise deltas");
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

import {
	type AdapterError,
	type ColumnDelta,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import { Pool, type PoolConfig } from "pg";
import type { DatabaseAdapter, DatabaseAdapterConfig } from "./db-types";
import type { Materialisable } from "./materialise";
import { buildSchemaIndex, groupDeltasByTable } from "./materialise";
import { mergeLatestState, wrapAsync } from "./shared";

const POSTGRES_TYPE_MAP: Record<TableSchema["columns"][number]["type"], string> = {
	string: "TEXT",
	number: "DOUBLE PRECISION",
	boolean: "BOOLEAN",
	json: "JSONB",
	null: "TEXT",
};

/**
 * PostgreSQL database adapter for LakeSync.
 *
 * Stores deltas in a `lakesync_deltas` table using pg Pool.
 * All public methods return `Result` and never throw.
 */
export class PostgresAdapter implements DatabaseAdapter, Materialisable {
	/** @internal */
	readonly pool: Pool;

	constructor(config: DatabaseAdapterConfig) {
		const poolConfig: PoolConfig = {
			connectionString: config.connectionString,
		};
		this.pool = new Pool(poolConfig);
	}

	/**
	 * Insert deltas into the database in a single batch.
	 * Idempotent via `ON CONFLICT (delta_id) DO NOTHING`.
	 */
	async insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>> {
		if (deltas.length === 0) {
			return Ok(undefined);
		}

		return wrapAsync(async () => {
			// Build a multi-row INSERT with parameterised values
			const values: unknown[] = [];
			const rows: string[] = [];

			for (let i = 0; i < deltas.length; i++) {
				const d = deltas[i]!;
				const offset = i * 7;
				rows.push(
					`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7})`,
				);
				values.push(
					d.deltaId,
					d.table,
					d.rowId,
					JSON.stringify(d.columns),
					d.hlc.toString(),
					d.clientId,
					d.op,
				);
			}

			const sql = `INSERT INTO lakesync_deltas (delta_id, "table", row_id, columns, hlc, client_id, op)
VALUES ${rows.join(", ")}
ON CONFLICT (delta_id) DO NOTHING`;

			await this.pool.query(sql, values);
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
			let params: unknown[];

			if (tables && tables.length > 0) {
				sql = `SELECT delta_id, "table", row_id, columns, hlc, client_id, op
FROM lakesync_deltas
WHERE hlc > $1 AND "table" = ANY($2)
ORDER BY hlc ASC`;
				params = [hlc.toString(), tables];
			} else {
				sql = `SELECT delta_id, "table", row_id, columns, hlc, client_id, op
FROM lakesync_deltas
WHERE hlc > $1
ORDER BY hlc ASC`;
				params = [hlc.toString()];
			}

			const result = await this.pool.query(sql, params);
			return result.rows.map(rowToRowDelta);
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
FROM lakesync_deltas
WHERE "table" = $1 AND row_id = $2
ORDER BY hlc ASC`;

			const result = await this.pool.query(sql, [table, rowId]);
			return mergeLatestState(result.rows);
		}, `Failed to get latest state for ${table}:${rowId}`);
	}

	/**
	 * Ensure the lakesync_deltas table and indices exist.
	 * The `schema` parameter is accepted for interface compliance but the
	 * internal table structure is fixed (deltas store column data as JSONB).
	 */
	async ensureSchema(_schema: TableSchema): Promise<Result<void, AdapterError>> {
		return wrapAsync(async () => {
			await this.pool.query(`
CREATE TABLE IF NOT EXISTS lakesync_deltas (
	delta_id TEXT PRIMARY KEY,
	"table" TEXT NOT NULL,
	row_id TEXT NOT NULL,
	columns JSONB NOT NULL,
	hlc BIGINT NOT NULL,
	client_id TEXT NOT NULL,
	op TEXT NOT NULL DEFAULT 'INSERT'
);
CREATE INDEX IF NOT EXISTS idx_lakesync_deltas_hlc ON lakesync_deltas (hlc);
CREATE INDEX IF NOT EXISTS idx_lakesync_deltas_table_row ON lakesync_deltas ("table", row_id);
`);
		}, "Failed to ensure schema");
	}

	/**
	 * Materialise deltas into destination tables.
	 *
	 * For each table with a matching schema, merges delta history into the
	 * latest row state and upserts into the destination table. Tombstoned
	 * rows are deleted. The `props` column is never touched.
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

				const dest = schema.table;
				const columnDefs = schema.columns
					.map((c) => `"${c.name}" ${POSTGRES_TYPE_MAP[c.type]}`)
					.join(", ");

				await this.pool.query(
					`CREATE TABLE IF NOT EXISTS "${dest}" (
	row_id TEXT PRIMARY KEY,
	${columnDefs},
	props JSONB NOT NULL DEFAULT '{}',
	synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`,
				);

				const sourceTable = schema.sourceTable ?? schema.table;
				const rowIdArray = [...rowIds];

				const deltaResult = await this.pool.query(
					`SELECT row_id, columns, op FROM lakesync_deltas WHERE "table" = $1 AND row_id = ANY($2) ORDER BY hlc ASC`,
					[sourceTable, rowIdArray],
				);

				// Group results by row_id
				const byRowId = new Map<string, Array<{ columns: string | ColumnDelta[]; op: string }>>();
				for (const row of deltaResult.rows) {
					const rid = row.row_id as string;
					let arr = byRowId.get(rid);
					if (!arr) {
						arr = [];
						byRowId.set(rid, arr);
					}
					arr.push(row as { columns: string | ColumnDelta[]; op: string });
				}

				const upserts: Array<{ rowId: string; state: Record<string, unknown> }> = [];
				const deleteIds: string[] = [];

				for (const [rowId, rows] of byRowId) {
					const state = mergeLatestState(rows);
					if (state !== null) {
						upserts.push({ rowId, state });
					} else {
						deleteIds.push(rowId);
					}
				}

				if (upserts.length > 0) {
					const colNames = schema.columns.map((c) => c.name);
					const allCols = ["row_id", ...colNames, "synced_at"];
					const colList = allCols.map((c) => `"${c}"`).join(", ");

					const values: unknown[] = [];
					const valueRows: string[] = [];
					const paramsPerRow = allCols.length;

					for (let i = 0; i < upserts.length; i++) {
						const u = upserts[i]!;
						const offset = i * paramsPerRow;
						const placeholders = allCols.map((_, j) => `$${offset + j + 1}`);
						valueRows.push(`(${placeholders.join(", ")})`);

						values.push(u.rowId);
						for (const col of colNames) {
							values.push(u.state[col] ?? null);
						}
						values.push(new Date());
					}

					const updateSet = [...colNames, "synced_at"]
						.map((c) => `"${c}" = EXCLUDED."${c}"`)
						.join(", ");

					await this.pool.query(
						`INSERT INTO "${dest}" (${colList}) VALUES ${valueRows.join(", ")} ON CONFLICT (row_id) DO UPDATE SET ${updateSet}`,
						values,
					);
				}

				if (deleteIds.length > 0) {
					await this.pool.query(`DELETE FROM "${dest}" WHERE row_id = ANY($1)`, [deleteIds]);
				}
			}
		}, "Failed to materialise deltas");
	}

	/** Close the database connection pool and release resources. */
	async close(): Promise<void> {
		await this.pool.end();
	}
}

/**
 * Convert a raw Postgres row into a RowDelta.
 */
function rowToRowDelta(row: Record<string, unknown>): RowDelta {
	const columns: ColumnDelta[] =
		typeof row.columns === "string"
			? JSON.parse(row.columns as string)
			: (row.columns as ColumnDelta[]);

	return {
		deltaId: row.delta_id as string,
		table: row.table as string,
		rowId: row.row_id as string,
		columns,
		hlc: BigInt(row.hlc as string) as HLCTimestamp,
		clientId: row.client_id as string,
		op: row.op as RowDelta["op"],
	};
}

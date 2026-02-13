import {
	AdapterError,
	type ColumnDelta,
	Err,
	Ok,
	type Result,
	type TableSchema,
} from "@lakesync/core";
import mysql from "mysql2/promise";
import type { CdcChangeBatch, CdcCursor, CdcDialect, CdcRawChange } from "./dialect";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Configuration for the MySQL CDC dialect. */
export interface MySqlCdcDialectConfig {
	/** MySQL connection string. */
	connectionString: string;
	/** Changelog table name. Default: `"_lakesync_cdc_log"`. */
	changelogTable?: string;
	/** Maximum number of changelog rows fetched per poll. Default: `1000`. */
	fetchLimit?: number;
}

// ---------------------------------------------------------------------------
// Changelog row shape (returned by SELECT on _lakesync_cdc_log)
// ---------------------------------------------------------------------------

/** A single row from the `_lakesync_cdc_log` changelog table. */
export interface ChangelogRow {
	id: number;
	table_name: string;
	row_id: string;
	op: "insert" | "update" | "delete";
	columns: string | null;
	captured_at: string | number;
}

// ---------------------------------------------------------------------------
// MySqlCdcDialect
// ---------------------------------------------------------------------------

/**
 * MySQL CDC dialect using a trigger-based changelog table.
 *
 * Because MySQL does not expose a convenient polling API (like Postgres
 * logical replication slots), this dialect creates:
 * 1. A `_lakesync_cdc_log` table to record changes.
 * 2. AFTER INSERT / UPDATE / DELETE triggers on each captured table.
 *
 * The triggers write changed data as JSON into the changelog. The
 * `fetchChanges` method polls the changelog by auto-increment ID cursor.
 *
 * Implements the {@link CdcDialect} interface so it can be used with the
 * generic {@link import("./cdc-source").CdcSource}.
 */
export class MySqlCdcDialect implements CdcDialect {
	readonly name: string;

	private readonly connectionString: string;
	private readonly changelogTable: string;
	private readonly fetchLimit: number;
	private pool: mysql.Pool | null = null;

	constructor(config: MySqlCdcDialectConfig) {
		this.connectionString = config.connectionString;
		this.changelogTable = config.changelogTable ?? "_lakesync_cdc_log";
		this.fetchLimit = config.fetchLimit ?? 1_000;
		this.name = `mysql:${this.changelogTable}`;
	}

	// -----------------------------------------------------------------------
	// CdcDialect interface
	// -----------------------------------------------------------------------

	async connect(): Promise<Result<void, AdapterError>> {
		try {
			this.pool = mysql.createPool({
				uri: this.connectionString,
				connectionLimit: 5,
			});
			// Verify the connection works
			const conn = await this.pool.getConnection();
			conn.release();
			return Ok(undefined);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to connect MySQL CDC dialect '${this.name}'`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async ensureCapture(tables: string[] | null): Promise<Result<void, AdapterError>> {
		if (!this.pool) {
			return Err(new AdapterError("MySQL CDC dialect is not connected — call connect() first"));
		}

		try {
			// 1. Create the changelog table
			await this.pool.execute(`
				CREATE TABLE IF NOT EXISTS \`${this.changelogTable}\` (
					id BIGINT AUTO_INCREMENT PRIMARY KEY,
					table_name VARCHAR(255) NOT NULL,
					row_id VARCHAR(255) NOT NULL,
					op ENUM('insert', 'update', 'delete') NOT NULL,
					columns JSON NULL,
					captured_at BIGINT NOT NULL,
					INDEX idx_id (id)
				)
			`);

			// 2. Discover which tables to capture
			const tablesToCapture = tables ?? (await this.listUserTables());

			// 3. Create triggers for each table
			for (const table of tablesToCapture) {
				await this.ensureTriggersForTable(table);
			}

			return Ok(undefined);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to ensure CDC capture for MySQL dialect '${this.name}'`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async fetchChanges(cursor: CdcCursor): Promise<Result<CdcChangeBatch, AdapterError>> {
		if (!this.pool) {
			return Err(new AdapterError("MySQL CDC dialect is not connected — call connect() first"));
		}

		try {
			const lastId = typeof cursor.lastId === "number" ? cursor.lastId : 0;

			const [rows] = await this.pool.execute(
				`SELECT id, table_name, row_id, op, columns, captured_at FROM \`${this.changelogTable}\` WHERE id > ? ORDER BY id ASC LIMIT ?`,
				[lastId, this.fetchLimit],
			);

			const changelogRows = rows as ChangelogRow[];

			if (changelogRows.length === 0) {
				return Ok({ changes: [], cursor });
			}

			const changes = parseChangelogRows(changelogRows);
			const newLastId = changelogRows[changelogRows.length - 1]!.id;

			return Ok({ changes, cursor: { lastId: newLastId } });
		} catch (error) {
			return Err(
				new AdapterError(
					"Failed to fetch CDC changes from MySQL",
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async discoverSchemas(tables: string[] | null): Promise<Result<TableSchema[], AdapterError>> {
		if (!this.pool) {
			return Err(new AdapterError("MySQL CDC dialect is not connected — call connect() first"));
		}

		try {
			const tablesSet = tables ? new Set(tables) : null;

			// Get the current database name
			const [dbRows] = await this.pool.execute("SELECT DATABASE() AS db_name");
			const dbName = (dbRows as Array<{ db_name: string }>)[0]?.db_name;
			if (!dbName) {
				return Err(new AdapterError("No database selected in MySQL connection"));
			}

			const [rows] = await this.pool.execute(
				`SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
				 FROM information_schema.columns
				 WHERE TABLE_SCHEMA = ?
				 ORDER BY TABLE_NAME, ORDINAL_POSITION`,
				[dbName],
			);

			const tableMap = new Map<string, TableSchema>();
			for (const row of rows as Array<{
				TABLE_NAME: string;
				COLUMN_NAME: string;
				DATA_TYPE: string;
			}>) {
				// Skip the changelog table itself
				if (row.TABLE_NAME === this.changelogTable) continue;
				if (tablesSet && !tablesSet.has(row.TABLE_NAME)) continue;

				let schema = tableMap.get(row.TABLE_NAME);
				if (!schema) {
					schema = { table: row.TABLE_NAME, columns: [] };
					tableMap.set(row.TABLE_NAME, schema);
				}
				schema.columns.push({
					name: row.COLUMN_NAME,
					type: mysqlTypeToColumnType(row.DATA_TYPE),
				});
			}

			return Ok(Array.from(tableMap.values()));
		} catch (error) {
			return Err(
				new AdapterError(
					"Failed to discover schemas from MySQL",
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async close(): Promise<void> {
		if (this.pool !== null) {
			try {
				await this.pool.end();
			} catch {
				// Best-effort close — ignore errors
			}
			this.pool = null;
		}
	}

	defaultCursor(): CdcCursor {
		return { lastId: 0 };
	}

	// -----------------------------------------------------------------------
	// Internals
	// -----------------------------------------------------------------------

	/** List all user tables in the current database (excluding the changelog). */
	private async listUserTables(): Promise<string[]> {
		if (!this.pool) return [];

		const [dbRows] = await this.pool.execute("SELECT DATABASE() AS db_name");
		const dbName = (dbRows as Array<{ db_name: string }>)[0]?.db_name;
		if (!dbName) return [];

		const [rows] = await this.pool.execute(
			`SELECT TABLE_NAME FROM information_schema.tables
			 WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
			 AND TABLE_NAME != ?`,
			[dbName, this.changelogTable],
		);

		return (rows as Array<{ TABLE_NAME: string }>).map((r) => r.TABLE_NAME);
	}

	/**
	 * Create AFTER INSERT / UPDATE / DELETE triggers for a single table.
	 *
	 * Each trigger captures the changed data as JSON and writes it to the
	 * changelog table. The trigger uses `JSON_OBJECT()` to serialise all
	 * columns of the affected row.
	 */
	private async ensureTriggersForTable(table: string): Promise<void> {
		if (!this.pool) return;

		// Discover columns for this table
		const [dbRows] = await this.pool.execute("SELECT DATABASE() AS db_name");
		const dbName = (dbRows as Array<{ db_name: string }>)[0]?.db_name;
		if (!dbName) return;

		const [colRows] = await this.pool.execute(
			`SELECT COLUMN_NAME FROM information_schema.columns
			 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
			 ORDER BY ORDINAL_POSITION`,
			[dbName, table],
		);

		const columns = (colRows as Array<{ COLUMN_NAME: string }>).map((r) => r.COLUMN_NAME);
		if (columns.length === 0) return;

		// Determine the primary key column(s) for row_id
		const [pkRows] = await this.pool.execute(
			`SELECT COLUMN_NAME FROM information_schema.key_column_usage
			 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
			 ORDER BY ORDINAL_POSITION`,
			[dbName, table],
		);

		const pkColumns = (pkRows as Array<{ COLUMN_NAME: string }>).map((r) => r.COLUMN_NAME);

		const buildRowIdExpr = (prefix: string): string => {
			if (pkColumns.length === 0) {
				// Fallback: use first column
				return `CAST(${prefix}.\`${columns[0]}\` AS CHAR)`;
			}
			if (pkColumns.length === 1) {
				return `CAST(${prefix}.\`${pkColumns[0]}\` AS CHAR)`;
			}
			// Composite PK: join with ':'
			return `CONCAT(${pkColumns.map((c) => `CAST(${prefix}.\`${c}\` AS CHAR)`).join(", ':', ")})`;
		};

		const buildJsonColumns = (prefix: string): string => {
			const args = columns.map((c) => `'${c}', ${prefix}.\`${c}\``).join(", ");
			return `JSON_ARRAY(${columns.map((c) => `JSON_OBJECT('column', '${c}', 'value', ${prefix}.\`${c}\`)`).join(", ")})`;
		};

		const triggerPrefix = `_lakesync_cdc_${table}`;

		// INSERT trigger
		await this.pool.execute(`DROP TRIGGER IF EXISTS \`${triggerPrefix}_ai\``);
		await this.pool.execute(`
			CREATE TRIGGER \`${triggerPrefix}_ai\` AFTER INSERT ON \`${table}\`
			FOR EACH ROW
			INSERT INTO \`${this.changelogTable}\` (table_name, row_id, op, columns, captured_at)
			VALUES ('${table}', ${buildRowIdExpr("NEW")}, 'insert', ${buildJsonColumns("NEW")}, UNIX_TIMESTAMP(NOW(3)) * 1000)
		`);

		// UPDATE trigger
		await this.pool.execute(`DROP TRIGGER IF EXISTS \`${triggerPrefix}_au\``);
		await this.pool.execute(`
			CREATE TRIGGER \`${triggerPrefix}_au\` AFTER UPDATE ON \`${table}\`
			FOR EACH ROW
			INSERT INTO \`${this.changelogTable}\` (table_name, row_id, op, columns, captured_at)
			VALUES ('${table}', ${buildRowIdExpr("OLD")}, 'update', ${buildJsonColumns("NEW")}, UNIX_TIMESTAMP(NOW(3)) * 1000)
		`);

		// DELETE trigger
		await this.pool.execute(`DROP TRIGGER IF EXISTS \`${triggerPrefix}_ad\``);
		await this.pool.execute(`
			CREATE TRIGGER \`${triggerPrefix}_ad\` AFTER DELETE ON \`${table}\`
			FOR EACH ROW
			INSERT INTO \`${this.changelogTable}\` (table_name, row_id, op, columns, captured_at)
			VALUES ('${table}', ${buildRowIdExpr("OLD")}, 'delete', NULL, UNIX_TIMESTAMP(NOW(3)) * 1000)
		`);
	}
}

// ---------------------------------------------------------------------------
// Changelog parsing — exported for unit testing
// ---------------------------------------------------------------------------

/**
 * Parse an array of changelog rows into CdcRawChanges.
 *
 * Exported for direct unit testing without a MySQL connection.
 *
 * @param rows - The changelog table rows.
 * @returns Array of CdcRawChanges.
 */
export function parseChangelogRows(rows: ChangelogRow[]): CdcRawChange[] {
	const result: CdcRawChange[] = [];

	for (const row of rows) {
		const columns = extractColumnsFromJson(row.columns, row.op);

		result.push({
			kind: row.op,
			schema: "",
			table: row.table_name,
			rowId: row.row_id,
			columns,
		});
	}

	return result;
}

/**
 * Extract ColumnDelta[] from JSON stored in the changelog.
 *
 * The trigger stores columns as a JSON array of `{ column, value }` objects.
 * For DELETE operations, columns is NULL — returns an empty array.
 *
 * Exported for unit testing.
 */
export function extractColumnsFromJson(
	json: string | null,
	op: string,
): ColumnDelta[] {
	if (op === "delete" || json === null) return [];

	const parsed: unknown = typeof json === "string" ? JSON.parse(json) : json;
	if (!Array.isArray(parsed)) return [];

	const columns: ColumnDelta[] = [];
	for (const entry of parsed) {
		if (
			typeof entry === "object" &&
			entry !== null &&
			"column" in entry &&
			"value" in entry
		) {
			columns.push({
				column: String((entry as { column: unknown }).column),
				value: (entry as { value: unknown }).value ?? null,
			});
		}
	}
	return columns;
}

/**
 * Generate the trigger SQL for a table (for a given operation).
 *
 * Exported for unit testing of trigger generation.
 *
 * @param table - Table name.
 * @param op - Operation: `"insert"`, `"update"`, or `"delete"`.
 * @param columns - Column names of the table.
 * @param pkColumns - Primary key column names.
 * @param changelogTable - Name of the changelog table.
 * @returns Object with `triggerName` and `sql` properties.
 */
export function buildTriggerSql(
	table: string,
	op: "insert" | "update" | "delete",
	columns: string[],
	pkColumns: string[],
	changelogTable: string,
): { triggerName: string; sql: string } {
	const suffixMap = { insert: "ai", update: "au", delete: "ad" } as const;
	const triggerName = `_lakesync_cdc_${table}_${suffixMap[op]}`;

	const buildRowIdExpr = (prefix: string): string => {
		if (pkColumns.length === 0) {
			return `CAST(${prefix}.\`${columns[0]}\` AS CHAR)`;
		}
		if (pkColumns.length === 1) {
			return `CAST(${prefix}.\`${pkColumns[0]}\` AS CHAR)`;
		}
		return `CONCAT(${pkColumns.map((c) => `CAST(${prefix}.\`${c}\` AS CHAR)`).join(", ':', ")})`;
	};

	const buildJsonColumns = (prefix: string): string => {
		return `JSON_ARRAY(${columns.map((c) => `JSON_OBJECT('column', '${c}', 'value', ${prefix}.\`${c}\`)`).join(", ")})`;
	};

	const triggerEvent = op.toUpperCase();
	const rowPrefix = op === "delete" ? "OLD" : "NEW";
	const rowIdPrefix = op === "update" ? "OLD" : rowPrefix;

	const columnsExpr = op === "delete" ? "NULL" : buildJsonColumns(rowPrefix);

	const sql = `CREATE TRIGGER \`${triggerName}\` AFTER ${triggerEvent} ON \`${table}\`
FOR EACH ROW
INSERT INTO \`${changelogTable}\` (table_name, row_id, op, columns, captured_at)
VALUES ('${table}', ${buildRowIdExpr(rowIdPrefix)}, '${op}', ${columnsExpr}, UNIX_TIMESTAMP(NOW(3)) * 1000)`;

	return { triggerName, sql };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Map a MySQL data type to a LakeSync column type. */
export function mysqlTypeToColumnType(mysqlType: string): TableSchema["columns"][number]["type"] {
	const lower = mysqlType.toLowerCase();
	// tinyint is MySQL's boolean — check before generic int
	if (lower === "tinyint") return "boolean";
	if (lower.includes("bool")) return "boolean";
	if (
		lower.includes("int") ||
		lower.includes("float") ||
		lower.includes("double") ||
		lower.includes("decimal") ||
		lower.includes("numeric") ||
		lower.includes("real")
	) {
		return "number";
	}
	if (lower.includes("json")) return "json";
	return "string";
}

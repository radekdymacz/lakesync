import {
	AdapterError,
	type ColumnDelta,
	Err,
	Ok,
	type Result,
	type TableSchema,
} from "@lakesync/core";
import type { CdcChangeBatch, CdcCursor, CdcDialect, CdcRawChange } from "./dialect";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Configuration for the MS SQL Server CDC dialect. */
export interface MsSqlCdcDialectConfig {
	/** SQL Server connection string. */
	connectionString: string;
	/** Tables to capture. Empty = all CDC-enabled tables. */
	tables?: string[];
	/** Schema name. Default: `"dbo"`. */
	schema?: string;
}

// ---------------------------------------------------------------------------
// CDC row type from fn_cdc_get_all_changes_*
// ---------------------------------------------------------------------------

/** A single row from SQL Server's `fn_cdc_get_all_changes_*` function. */
export interface MsSqlCdcRow {
	/** Operation: 1=DELETE, 2=INSERT, 3=UPDATE_BEFORE, 4=UPDATE_AFTER. */
	__$operation: number;
	/** Transaction LSN as Buffer (binary(10)). */
	__$start_lsn: Buffer;
	[column: string]: unknown;
}

// ---------------------------------------------------------------------------
// Capture instance metadata
// ---------------------------------------------------------------------------

/** Metadata about a SQL Server CDC capture instance. */
interface CaptureInstance {
	source_schema: string;
	source_table: string;
	capture_instance: string;
}

// ---------------------------------------------------------------------------
// mssql module type (dynamic import)
// ---------------------------------------------------------------------------

/** Minimal type surface of the `mssql` package that we use. */
interface MsSqlModule {
	connect(connectionString: string): Promise<MsSqlPool>;
	Binary: unknown;
}

interface MsSqlPool {
	request(): MsSqlRequest;
	close(): Promise<void>;
}

interface MsSqlRequest {
	input(name: string, type: unknown, value: unknown): MsSqlRequest;
	query<T = Record<string, unknown>>(sql: string): Promise<{ recordset: T[] }>;
}

// ---------------------------------------------------------------------------
// MsSqlCdcDialect
// ---------------------------------------------------------------------------

/**
 * MS SQL Server CDC dialect using the built-in Change Data Capture feature.
 *
 * Implements the {@link CdcDialect} interface so it can be used with the
 * generic {@link import("./cdc-source").CdcSource}.
 *
 * Requires the `mssql` package as an optional dependency. If it is not
 * installed, `connect()` returns an error explaining the requirement.
 */
export class MsSqlCdcDialect implements CdcDialect {
	readonly name: string;

	private readonly connectionString: string;
	private readonly schema: string;
	private pool: MsSqlPool | null = null;
	private mssqlModule: MsSqlModule | null = null;

	constructor(config: MsSqlCdcDialectConfig) {
		this.connectionString = config.connectionString;
		this.schema = config.schema ?? "dbo";
		this.name = `mssql:${this.schema}`;
	}

	// -----------------------------------------------------------------------
	// CdcDialect interface
	// -----------------------------------------------------------------------

	async connect(): Promise<Result<void, AdapterError>> {
		try {
			const mssql = await loadMsSqlModule();
			this.mssqlModule = mssql;
			this.pool = await mssql.connect(this.connectionString);
			return Ok(undefined);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to connect MS SQL CDC dialect '${this.name}'`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async ensureCapture(tables: string[] | null): Promise<Result<void, AdapterError>> {
		if (!this.pool) {
			return Err(new AdapterError("MS SQL CDC dialect is not connected — call connect() first"));
		}

		try {
			// Enable CDC on the database
			await this.pool.request().query("EXEC sys.sp_cdc_enable_db");

			// Discover which tables already have CDC enabled
			const existing = await this.pool
				.request()
				.query<CaptureInstance>("EXEC sys.sp_cdc_help_change_data_capture");
			const enabledTables = new Set(
				existing.recordset.map((r) => `${r.source_schema}.${r.source_table}`),
			);

			// Determine which tables to enable
			const tablesToEnable = tables ?? [];

			for (const table of tablesToEnable) {
				const qualifiedName = `${this.schema}.${table}`;
				if (enabledTables.has(qualifiedName)) continue;

				await this.pool
					.request()
					.query(
						`EXEC sys.sp_cdc_enable_table @source_schema = N'${escapeIdentifier(this.schema)}', @source_name = N'${escapeIdentifier(table)}', @role_name = NULL`,
					);
			}

			return Ok(undefined);
		} catch (error) {
			return Err(
				new AdapterError(
					"Failed to ensure CDC capture on MS SQL Server",
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async fetchChanges(cursor: CdcCursor): Promise<Result<CdcChangeBatch, AdapterError>> {
		if (!this.pool || !this.mssqlModule) {
			return Err(new AdapterError("MS SQL CDC dialect is not connected — call connect() first"));
		}

		try {
			// Get the current maximum LSN
			const maxResult = await this.pool
				.request()
				.query<{ max_lsn: Buffer }>("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn");

			const maxLsnBuf = maxResult.recordset[0]?.max_lsn;
			if (!maxLsnBuf) {
				return Ok({ changes: [], cursor });
			}

			const maxLsnHex = bufferToHex(maxLsnBuf);
			const cursorLsn = typeof cursor.lsn === "string" ? (cursor.lsn as string) : DEFAULT_LSN;

			// No new changes
			if (compareLsn(maxLsnHex, cursorLsn) <= 0) {
				return Ok({ changes: [], cursor });
			}

			// Increment from-LSN to avoid re-reading the cursor position
			const fromLsnBuf = hexToBuffer(cursorLsn);
			const incrementResult = await this.pool
				.request()
				.input("from_lsn", this.mssqlModule.Binary, fromLsnBuf)
				.query<{ incremented: Buffer }>(
					"SELECT sys.fn_cdc_increment_lsn(@from_lsn) AS incremented",
				);

			const fromLsnIncremented = incrementResult.recordset[0]?.incremented;
			if (!fromLsnIncremented) {
				return Ok({ changes: [], cursor });
			}

			// Discover capture instances
			const captures = await this.pool
				.request()
				.query<CaptureInstance>("EXEC sys.sp_cdc_help_change_data_capture");

			const allChanges: CdcRawChange[] = [];

			for (const capture of captures.recordset) {
				if (capture.source_schema !== this.schema) continue;

				const fnName = `cdc.fn_cdc_get_all_changes_${capture.capture_instance}`;
				const result = await this.pool
					.request()
					.input("from_lsn", this.mssqlModule.Binary, fromLsnIncremented)
					.input("to_lsn", this.mssqlModule.Binary, maxLsnBuf)
					.query<MsSqlCdcRow>(
						`SELECT * FROM ${fnName}(@from_lsn, @to_lsn, N'all')`,
					);

				const changes = parseMsSqlCdcRows(result.recordset, capture.source_table, this.schema);
				allChanges.push(...changes);
			}

			return Ok({ changes: allChanges, cursor: { lsn: maxLsnHex } });
		} catch (error) {
			return Err(
				new AdapterError(
					"Failed to fetch CDC changes from MS SQL Server",
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async discoverSchemas(tables: string[] | null): Promise<Result<TableSchema[], AdapterError>> {
		if (!this.pool) {
			return Err(new AdapterError("MS SQL CDC dialect is not connected — call connect() first"));
		}

		try {
			const tablesSet = tables ? new Set(tables) : null;
			const result = await this.pool
				.request()
				.query<{
					TABLE_NAME: string;
					COLUMN_NAME: string;
					DATA_TYPE: string;
				}>(
					`SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE
					 FROM INFORMATION_SCHEMA.COLUMNS c
					 JOIN INFORMATION_SCHEMA.TABLES t
					   ON c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
					 WHERE t.TABLE_SCHEMA = '${escapeIdentifier(this.schema)}'
					   AND t.TABLE_TYPE = 'BASE TABLE'
					 ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION`,
				);

			const tableMap = new Map<string, TableSchema>();
			for (const row of result.recordset) {
				if (tablesSet && !tablesSet.has(row.TABLE_NAME)) continue;

				let schema = tableMap.get(row.TABLE_NAME);
				if (!schema) {
					schema = { table: row.TABLE_NAME, columns: [] };
					tableMap.set(row.TABLE_NAME, schema);
				}
				schema.columns.push({
					name: row.COLUMN_NAME,
					type: mssqlTypeToColumnType(row.DATA_TYPE),
				});
			}

			return Ok(Array.from(tableMap.values()));
		} catch (error) {
			return Err(
				new AdapterError(
					"Failed to discover schemas from MS SQL Server",
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async close(): Promise<void> {
		if (this.pool !== null) {
			try {
				await this.pool.close();
			} catch {
				// Best-effort close — ignore errors
			}
			this.pool = null;
		}
	}

	defaultCursor(): CdcCursor {
		return { lsn: DEFAULT_LSN };
	}
}

// ---------------------------------------------------------------------------
// Default LSN constant
// ---------------------------------------------------------------------------

/** Default zero LSN for SQL Server (binary(10) as hex). */
const DEFAULT_LSN = "0x00000000000000000000";

// ---------------------------------------------------------------------------
// CDC row parsing — exported for unit testing
// ---------------------------------------------------------------------------

/** CDC system columns to exclude from user data. */
const CDC_SYSTEM_COLUMNS = new Set([
	"__$start_lsn",
	"__$end_lsn",
	"__$seqval",
	"__$operation",
	"__$update_mask",
	"__$command_id",
]);

/**
 * Parse an array of MS SQL CDC rows into CdcRawChanges.
 *
 * Filters out `__$operation = 3` (update before-image) and maps
 * operation codes to change kinds:
 * - 1 = delete
 * - 2 = insert
 * - 4 = update (after-image)
 *
 * Exported for direct unit testing without a SQL Server connection.
 *
 * @param rows - The CDC rows from `fn_cdc_get_all_changes_*`.
 * @param table - The source table name.
 * @param schema - The source schema name.
 * @returns Array of CdcRawChanges.
 */
export function parseMsSqlCdcRows(
	rows: MsSqlCdcRow[],
	table: string,
	schema: string,
): CdcRawChange[] {
	const result: CdcRawChange[] = [];

	for (const row of rows) {
		// Skip update before-image
		if (row.__$operation === 3) continue;

		const kind = mapOperationToKind(row.__$operation);
		if (kind === null) continue;

		const { columns, rowId } = extractColumnsAndRowId(row);
		if (rowId === null) continue;

		result.push({
			kind,
			schema,
			table,
			rowId,
			columns: kind === "delete" ? [] : columns,
		});
	}

	return result;
}

/**
 * Map a SQL Server CDC `__$operation` code to a change kind.
 *
 * Exported for unit testing.
 *
 * @param operation - The `__$operation` value (1=delete, 2=insert, 3=update before, 4=update after).
 * @returns The change kind, or `null` for unsupported operations.
 */
export function mapOperationToKind(operation: number): CdcRawChange["kind"] | null {
	switch (operation) {
		case 1:
			return "delete";
		case 2:
			return "insert";
		case 4:
			return "update";
		default:
			return null;
	}
}

/**
 * Convert an LSN hex string to a Buffer.
 *
 * Exported for unit testing.
 *
 * @param hex - LSN as hex string (with or without `0x` prefix).
 * @returns Buffer of 10 bytes.
 */
export function hexToBuffer(hex: string): Buffer {
	const clean = hex.startsWith("0x") ? hex.slice(2) : hex;
	return Buffer.from(clean.padStart(20, "0"), "hex");
}

/**
 * Convert a Buffer to an LSN hex string with `0x` prefix.
 *
 * Exported for unit testing.
 *
 * @param buf - Buffer (binary(10)).
 * @returns Hex string with `0x` prefix.
 */
export function bufferToHex(buf: Buffer): string {
	return `0x${buf.toString("hex").padStart(20, "0")}`;
}

/**
 * Compare two LSN hex strings lexicographically.
 *
 * Returns negative if a < b, zero if equal, positive if a > b.
 *
 * Exported for unit testing.
 *
 * @param a - First LSN hex string.
 * @param b - Second LSN hex string.
 * @returns Comparison result.
 */
export function compareLsn(a: string, b: string): number {
	const aNorm = normaliseLsn(a);
	const bNorm = normaliseLsn(b);
	if (aNorm < bNorm) return -1;
	if (aNorm > bNorm) return 1;
	return 0;
}

/**
 * Derive the capture instance name from schema and table.
 *
 * SQL Server's default capture instance name is `schema_table`.
 *
 * Exported for unit testing.
 *
 * @param schema - The source schema name.
 * @param table - The source table name.
 * @returns The capture instance name.
 */
export function deriveCaptureInstanceName(schema: string, table: string): string {
	return `${schema}_${table}`;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Extract user columns (excluding CDC system columns) and derive rowId from the first column. */
function extractColumnsAndRowId(row: MsSqlCdcRow): {
	columns: ColumnDelta[];
	rowId: string | null;
} {
	const columns: ColumnDelta[] = [];
	let rowId: string | null = null;

	for (const [key, value] of Object.entries(row)) {
		if (CDC_SYSTEM_COLUMNS.has(key)) continue;

		if (rowId === null) {
			rowId = String(value);
		}

		columns.push({
			column: key,
			value: value ?? null,
		});
	}

	return { columns, rowId };
}

/** Normalise an LSN hex string to a fixed 20-character lowercase hex (without prefix). */
function normaliseLsn(lsn: string): string {
	const clean = lsn.startsWith("0x") ? lsn.slice(2) : lsn;
	return clean.padStart(20, "0").toLowerCase();
}

/** Map a SQL Server data type to a LakeSync column type. */
function mssqlTypeToColumnType(dataType: string): TableSchema["columns"][number]["type"] {
	const lower = dataType.toLowerCase();
	if (
		lower.includes("int") ||
		lower.includes("float") ||
		lower.includes("double") ||
		lower.includes("numeric") ||
		lower.includes("decimal") ||
		lower.includes("real") ||
		lower.includes("money") ||
		lower.includes("smallmoney")
	) {
		return "number";
	}
	if (lower === "bit") return "boolean";
	if (lower.includes("json")) return "json";
	return "string";
}

/** Simple identifier escape — prevents SQL injection in identifier positions. */
function escapeIdentifier(identifier: string): string {
	return identifier.replace(/'/g, "''");
}

/**
 * Dynamically load the `mssql` package.
 *
 * The `mssql` package is an optional dependency — users only install it
 * when they need SQL Server support. If it is not available, this throws
 * with a descriptive message.
 */
async function loadMsSqlModule(): Promise<MsSqlModule> {
	try {
		// Use a variable to prevent TypeScript from resolving the module at build time.
		// The `mssql` package is an optional dependency — only installed when needed.
		const moduleName = "mssql";
		const mod = await import(/* @vite-ignore */ moduleName);
		return mod.default ?? mod;
	} catch {
		throw new Error(
			"The 'mssql' package is required for MS SQL Server CDC support. " +
				"Install it with: npm install mssql (or bun add mssql)",
		);
	}
}

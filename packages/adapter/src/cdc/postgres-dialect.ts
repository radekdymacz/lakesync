import {
	AdapterError,
	type ColumnDelta,
	Err,
	Ok,
	type Result,
	type TableSchema,
} from "@lakesync/core";
import { Client } from "pg";
import type { CdcChangeBatch, CdcCursor, CdcDialect, CdcRawChange } from "./dialect";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Configuration for the Postgres CDC dialect. */
export interface PostgresCdcDialectConfig {
	/** Postgres connection string. */
	connectionString: string;
	/** Replication slot name (created if it does not exist). Default: `"lakesync_cdc"`. */
	slotName?: string;
}

// ---------------------------------------------------------------------------
// wal2json types (subset we parse)
// ---------------------------------------------------------------------------

/** A single change entry in the wal2json output. */
export interface Wal2JsonChange {
	kind: "insert" | "update" | "delete";
	schema: string;
	table: string;
	columnnames?: string[];
	columntypes?: string[];
	columnvalues?: unknown[];
	oldkeys?: {
		keynames: string[];
		keytypes?: string[];
		keyvalues: unknown[];
	};
}

/** Top-level wal2json payload. */
export interface Wal2JsonPayload {
	change: Wal2JsonChange[];
}

// ---------------------------------------------------------------------------
// Internal row type returned by pg_logical_slot_get_changes
// ---------------------------------------------------------------------------

interface SlotChangeRow {
	lsn: string;
	xid: string;
	data: string;
}

// ---------------------------------------------------------------------------
// PostgresCdcDialect
// ---------------------------------------------------------------------------

/**
 * Postgres CDC dialect using logical replication slots with the `wal2json`
 * output plugin.
 *
 * Implements the {@link CdcDialect} interface so it can be used with the
 * generic {@link import("./cdc-source").CdcSource}.
 */
export class PostgresCdcDialect implements CdcDialect {
	readonly name: string;

	private readonly connectionString: string;
	private readonly slotName: string;
	private client: Client | null = null;

	constructor(config: PostgresCdcDialectConfig) {
		this.connectionString = config.connectionString;
		this.slotName = config.slotName ?? "lakesync_cdc";
		this.name = `postgres:${this.slotName}`;
	}

	// -----------------------------------------------------------------------
	// CdcDialect interface
	// -----------------------------------------------------------------------

	async connect(): Promise<Result<void, AdapterError>> {
		try {
			this.client = new Client({ connectionString: this.connectionString });
			await this.client.connect();
			return Ok(undefined);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to connect Postgres CDC dialect '${this.name}'`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async ensureCapture(_tables: string[] | null): Promise<Result<void, AdapterError>> {
		if (!this.client) {
			return Err(new AdapterError("Postgres CDC dialect is not connected — call connect() first"));
		}
		try {
			const result = await this.client.query<{ slot_name: string }>(
				"SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
				[this.slotName],
			);

			if (result.rows.length === 0) {
				await this.client.query("SELECT pg_create_logical_replication_slot($1, 'wal2json')", [
					this.slotName,
				]);
			}
			return Ok(undefined);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to ensure replication slot '${this.slotName}'`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	async fetchChanges(cursor: CdcCursor): Promise<Result<CdcChangeBatch, AdapterError>> {
		if (!this.client) {
			return Err(new AdapterError("Postgres CDC dialect is not connected — call connect() first"));
		}

		try {
			const result = await this.client.query<SlotChangeRow>(
				"SELECT lsn, xid, data FROM pg_logical_slot_get_changes($1, NULL, NULL)",
				[this.slotName],
			);

			if (result.rows.length === 0) {
				return Ok({ changes: [], cursor });
			}

			let latestLsn = typeof cursor.lsn === "string" ? cursor.lsn : "0/0";
			const allChanges: CdcRawChange[] = [];

			for (const row of result.rows) {
				latestLsn = row.lsn;
				const payload: Wal2JsonPayload = JSON.parse(row.data);
				const changes = parseWal2JsonChanges(payload.change);
				allChanges.push(...changes);
			}

			return Ok({ changes: allChanges, cursor: { lsn: latestLsn } });
		} catch (error) {
			return Err(
				new AdapterError("Failed to fetch CDC changes", error instanceof Error ? error : undefined),
			);
		}
	}

	async discoverSchemas(tables: string[] | null): Promise<Result<TableSchema[], AdapterError>> {
		if (!this.client) {
			return Err(new AdapterError("Postgres CDC dialect is not connected — call connect() first"));
		}

		try {
			const tablesSet = tables ? new Set(tables) : null;
			const result = await this.client.query<{
				table_name: string;
				column_name: string;
				data_type: string;
			}>(
				`SELECT c.table_name, c.column_name, c.data_type
				 FROM information_schema.columns c
				 JOIN information_schema.tables t
				   ON c.table_name = t.table_name AND c.table_schema = t.table_schema
				 WHERE t.table_schema = 'public'
				   AND t.table_type = 'BASE TABLE'
				 ORDER BY c.table_name, c.ordinal_position`,
			);

			const tableMap = new Map<string, TableSchema>();
			for (const row of result.rows) {
				if (tablesSet && !tablesSet.has(row.table_name)) continue;

				let schema = tableMap.get(row.table_name);
				if (!schema) {
					schema = { table: row.table_name, columns: [] };
					tableMap.set(row.table_name, schema);
				}
				schema.columns.push({
					name: row.column_name,
					type: pgTypeToColumnType(row.data_type),
				});
			}

			return Ok(Array.from(tableMap.values()));
		} catch (error) {
			return Err(
				new AdapterError("Failed to discover schemas", error instanceof Error ? error : undefined),
			);
		}
	}

	async close(): Promise<void> {
		if (this.client !== null) {
			try {
				await this.client.end();
			} catch {
				// Best-effort close — ignore errors
			}
			this.client = null;
		}
	}

	defaultCursor(): CdcCursor {
		return { lsn: "0/0" };
	}
}

// ---------------------------------------------------------------------------
// wal2json parsing — exported for unit testing
// ---------------------------------------------------------------------------

/**
 * Parse an array of wal2json change entries into CdcRawChanges.
 *
 * Exported for direct unit testing without a Postgres connection.
 *
 * @param changes - The wal2json change entries.
 * @returns Array of CdcRawChanges.
 */
export function parseWal2JsonChanges(changes: Wal2JsonChange[]): CdcRawChange[] {
	const result: CdcRawChange[] = [];

	for (const change of changes) {
		const rowId = extractRowId(change);
		if (rowId === null) continue;

		const columns = extractColumns(change);

		result.push({
			kind: change.kind,
			schema: change.schema,
			table: change.table,
			rowId,
			columns,
		});
	}

	return result;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Extract a composite row ID from a wal2json change.
 *
 * For INSERT/UPDATE, uses the primary key columns from `oldkeys` if available,
 * otherwise falls back to the first column value.
 * For DELETE, uses `oldkeys`.
 * Multiple PK columns are joined with `:`.
 */
function extractRowId(change: Wal2JsonChange): string | null {
	// DELETE always has oldkeys
	if (change.kind === "delete") {
		if (!change.oldkeys || change.oldkeys.keyvalues.length === 0) return null;
		return change.oldkeys.keyvalues.map(String).join(":");
	}

	// INSERT/UPDATE — prefer oldkeys (PK), fall back to first column
	if (change.oldkeys && change.oldkeys.keyvalues.length > 0) {
		return change.oldkeys.keyvalues.map(String).join(":");
	}

	// Fall back: use the first column value as the row ID
	if (change.columnvalues && change.columnvalues.length > 0) {
		return String(change.columnvalues[0]);
	}

	return null;
}

/**
 * Extract column deltas from a wal2json change.
 *
 * For INSERT/UPDATE, maps `columnnames`/`columnvalues` to ColumnDelta[].
 * For DELETE, returns an empty array (per RowDelta convention).
 */
function extractColumns(change: Wal2JsonChange): ColumnDelta[] {
	if (change.kind === "delete") return [];

	if (!change.columnnames || !change.columnvalues) return [];

	const columns: ColumnDelta[] = [];
	for (let i = 0; i < change.columnnames.length; i++) {
		columns.push({
			column: change.columnnames[i]!,
			value: change.columnvalues[i] ?? null,
		});
	}
	return columns;
}

/** Map a Postgres data type to a LakeSync column type. */
function pgTypeToColumnType(pgType: string): TableSchema["columns"][number]["type"] {
	const lower = pgType.toLowerCase();
	if (
		lower.includes("int") ||
		lower.includes("float") ||
		lower.includes("double") ||
		lower.includes("numeric") ||
		lower.includes("decimal") ||
		lower.includes("real") ||
		lower.includes("serial")
	) {
		return "number";
	}
	if (lower.includes("bool")) return "boolean";
	if (lower.includes("json")) return "json";
	return "string";
}

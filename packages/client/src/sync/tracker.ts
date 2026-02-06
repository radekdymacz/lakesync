import { Err, Ok, LakeSyncError, extractDelta } from "@lakesync/core";
import type { Result, HLCTimestamp, TableSchema } from "@lakesync/core";
import type { HLC } from "@lakesync/core";
import type { LocalDB } from "../db/local-db";
import type { DbError } from "../db/types";
import { getSchema } from "../db/schema-registry";
import type { SyncQueue } from "../queue/types";

/**
 * Tracks local mutations (insert, update, delete) and produces
 * column-level deltas that are pushed to a SyncQueue.
 *
 * Each write operation:
 * 1. Applies the change to the local SQLite database
 * 2. Extracts a RowDelta describing the change
 * 3. Pushes the delta to the sync queue for eventual upstream delivery
 */
export class SyncTracker {
	constructor(
		private readonly db: LocalDB,
		private readonly queue: SyncQueue,
		private readonly hlc: HLC,
		private readonly clientId: string,
	) {}

	/**
	 * Insert a new row into the specified table.
	 *
	 * Writes the row to SQLite and pushes an INSERT delta to the queue.
	 *
	 * @param table - The target table name
	 * @param rowId - The unique row identifier
	 * @param data - Column name/value pairs for the new row
	 * @returns Ok on success, or Err with a LakeSyncError on failure
	 */
	async insert(
		table: string,
		rowId: string,
		data: Record<string, unknown>,
	): Promise<Result<void, LakeSyncError>> {
		// Fetch schema for delta extraction filtering
		const schemaResult = await getSchema(this.db, table);
		if (!schemaResult.ok) return schemaResult;
		const schema = schemaResult.value ?? undefined;

		// Build the INSERT SQL from data keys
		const columns = Object.keys(data);
		const allColumns = ["_rowId", ...columns];
		const placeholders = allColumns.map(() => "?").join(", ");
		const columnList = allColumns.join(", ");
		const values = [rowId, ...columns.map((col) => data[col])];

		const sql = `INSERT INTO ${table} (${columnList}) VALUES (${placeholders})`;
		const execResult = await this.db.exec(sql, values);
		if (!execResult.ok) return execResult;

		// Extract delta: null -> data means INSERT
		const hlc = this.hlc.now();
		const delta = await extractDelta(null, data, {
			table,
			rowId,
			clientId: this.clientId,
			hlc,
			schema,
		});

		if (delta) {
			const pushResult = await this.queue.push(delta);
			if (!pushResult.ok) return pushResult;
		}

		return Ok(undefined);
	}

	/**
	 * Update an existing row in the specified table.
	 *
	 * Reads the current row state, applies partial updates, and pushes
	 * an UPDATE delta containing only the changed columns.
	 *
	 * @param table - The target table name
	 * @param rowId - The unique row identifier
	 * @param data - Column name/value pairs to update (partial)
	 * @returns Ok on success, Err if the row is not found or on failure
	 */
	async update(
		table: string,
		rowId: string,
		data: Record<string, unknown>,
	): Promise<Result<void, LakeSyncError>> {
		// Fetch schema for delta extraction filtering
		const schemaResult = await getSchema(this.db, table);
		if (!schemaResult.ok) return schemaResult;
		const schema = schemaResult.value ?? undefined;

		// Read current row
		const queryResult = await this.db.query<Record<string, unknown>>(
			`SELECT * FROM ${table} WHERE _rowId = ?`,
			[rowId],
		);
		if (!queryResult.ok) return queryResult;

		const rows = queryResult.value;
		if (rows.length === 0 || !rows[0]) {
			return Err(
				new LakeSyncError(
					`Row "${rowId}" not found in table "${table}"`,
					"ROW_NOT_FOUND",
				),
			);
		}

		const currentRow = rows[0];

		// Build the before state excluding _rowId (not a user column)
		const before: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(currentRow)) {
			if (key !== "_rowId") {
				before[key] = value;
			}
		}

		// Build SET clause from data keys
		const columns = Object.keys(data);
		const setClauses = columns.map((col) => `${col} = ?`).join(", ");
		const values = [...columns.map((col) => data[col]), rowId];

		const sql = `UPDATE ${table} SET ${setClauses} WHERE _rowId = ?`;
		const execResult = await this.db.exec(sql, values);
		if (!execResult.ok) return execResult;

		// Build the after state: merge current row with updates
		const after: Record<string, unknown> = { ...before, ...data };

		// Extract delta: only changed columns
		const hlc = this.hlc.now();
		const delta = await extractDelta(before, after, {
			table,
			rowId,
			clientId: this.clientId,
			hlc,
			schema,
		});

		if (delta) {
			const pushResult = await this.queue.push(delta);
			if (!pushResult.ok) return pushResult;
		}

		return Ok(undefined);
	}

	/**
	 * Delete a row from the specified table.
	 *
	 * Reads the current row state for delta extraction, removes the row
	 * from SQLite, and pushes a DELETE delta to the queue.
	 *
	 * @param table - The target table name
	 * @param rowId - The unique row identifier
	 * @returns Ok on success, Err if the row is not found or on failure
	 */
	async delete(
		table: string,
		rowId: string,
	): Promise<Result<void, LakeSyncError>> {
		// Fetch schema for delta extraction filtering
		const schemaResult = await getSchema(this.db, table);
		if (!schemaResult.ok) return schemaResult;
		const schema = schemaResult.value ?? undefined;

		// Read current row for delta extraction
		const queryResult = await this.db.query<Record<string, unknown>>(
			`SELECT * FROM ${table} WHERE _rowId = ?`,
			[rowId],
		);
		if (!queryResult.ok) return queryResult;

		const rows = queryResult.value;
		if (rows.length === 0 || !rows[0]) {
			return Err(
				new LakeSyncError(
					`Row "${rowId}" not found in table "${table}"`,
					"ROW_NOT_FOUND",
				),
			);
		}

		const currentRow = rows[0];

		// Build the before state excluding _rowId
		const before: Record<string, unknown> = {};
		for (const [key, value] of Object.entries(currentRow)) {
			if (key !== "_rowId") {
				before[key] = value;
			}
		}

		// Delete the row
		const execResult = await this.db.exec(
			`DELETE FROM ${table} WHERE _rowId = ?`,
			[rowId],
		);
		if (!execResult.ok) return execResult;

		// Extract delta: data -> null means DELETE
		const hlc = this.hlc.now();
		const delta = await extractDelta(before, null, {
			table,
			rowId,
			clientId: this.clientId,
			hlc,
			schema,
		});

		if (delta) {
			const pushResult = await this.queue.push(delta);
			if (!pushResult.ok) return pushResult;
		}

		return Ok(undefined);
	}

	/**
	 * Query the local database.
	 *
	 * Pass-through to the underlying LocalDB query method.
	 *
	 * @param sql - The SQL query to execute
	 * @param params - Optional bind parameters
	 * @returns The query results as typed rows, or a DbError on failure
	 */
	async query<T>(
		sql: string,
		params?: unknown[],
	): Promise<Result<T[], DbError>> {
		return this.db.query<T>(sql, params);
	}
}

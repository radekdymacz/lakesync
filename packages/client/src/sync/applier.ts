import type { ConflictResolver, HLCTimestamp, Result, RowDelta } from "@lakesync/core";
import {
	assertValidIdentifier,
	Err,
	HLC,
	LakeSyncError,
	Ok,
	quoteIdentifier,
} from "@lakesync/core";
import type { LocalDB } from "../db/local-db";
import type { QueueEntry, SyncQueue } from "../queue/types";

/**
 * Apply remote deltas to the local SQLite database.
 *
 * For each remote delta:
 * 1. Check if the same rowId has a pending local delta in the queue
 * 2. If conflict: use the resolver to determine winner
 *    - Remote wins: apply to SQLite, remove local from queue
 *    - Local wins: skip remote, keep local in queue
 * 3. If no conflict: apply remote delta directly
 *
 * After the batch, the `_sync_cursor` table is updated with the maximum
 * HLC from applied deltas so that subsequent syncs can resume from the
 * correct position.
 *
 * @param db - The local SQLite database
 * @param deltas - Remote deltas to apply
 * @param resolver - Conflict resolution strategy
 * @param pendingQueue - The local sync queue to check for conflicts
 * @returns The number of applied deltas, or an error
 */
export async function applyRemoteDeltas(
	db: LocalDB,
	deltas: RowDelta[],
	resolver: ConflictResolver,
	pendingQueue: SyncQueue,
): Promise<Result<number, LakeSyncError>> {
	if (deltas.length === 0) {
		return Ok(0);
	}

	// Ensure the _sync_cursor table exists
	const cursorTableResult = await db.exec(`
		CREATE TABLE IF NOT EXISTS _sync_cursor (
			table_name TEXT PRIMARY KEY,
			last_synced_hlc TEXT NOT NULL
		)
	`);
	if (!cursorTableResult.ok) {
		return Err(
			new LakeSyncError(
				"Failed to create _sync_cursor table",
				"APPLY_ERROR",
				cursorTableResult.error,
			),
		);
	}

	// Begin a manual transaction for atomicity
	const beginResult = await db.exec("BEGIN");
	if (!beginResult.ok) {
		return Err(
			new LakeSyncError(
				"Failed to begin transaction for remote delta application",
				"APPLY_ERROR",
				beginResult.error,
			),
		);
	}

	const peekResult = await pendingQueue.peek(Number.MAX_SAFE_INTEGER);
	if (!peekResult.ok) {
		await db.exec("ROLLBACK");
		return Err(
			new LakeSyncError(
				"Failed to peek pending queue for conflict detection",
				"APPLY_ERROR",
				peekResult.error,
			),
		);
	}
	const pendingMap = new Map<string, QueueEntry>();
	for (const entry of peekResult.value) {
		pendingMap.set(`${entry.delta.table}:${entry.delta.rowId}`, entry);
	}

	let appliedCount = 0;
	/** Track the maximum HLC per table for cursor advancement */
	const maxHlcPerTable = new Map<string, HLCTimestamp>();

	for (const remoteDelta of deltas) {
		const result = await applyOneDelta(db, remoteDelta, resolver, pendingQueue, pendingMap);
		if (!result.ok) {
			// Rollback on any failure
			await db.exec("ROLLBACK");
			return result;
		}

		if (result.value) {
			appliedCount++;
		}

		// Track the maximum HLC regardless of whether we applied (cursor should still advance)
		const currentMax = maxHlcPerTable.get(remoteDelta.table);
		if (currentMax === undefined || HLC.compare(remoteDelta.hlc, currentMax) > 0) {
			maxHlcPerTable.set(remoteDelta.table, remoteDelta.hlc);
		}
	}

	// Update the sync cursor for each table
	for (const [tableName, hlc] of maxHlcPerTable) {
		const cursorResult = await db.exec(
			"INSERT OR REPLACE INTO _sync_cursor (table_name, last_synced_hlc) VALUES (?, ?)",
			[tableName, hlc.toString()],
		);
		if (!cursorResult.ok) {
			await db.exec("ROLLBACK");
			return Err(
				new LakeSyncError(
					`Failed to update sync cursor for table "${tableName}"`,
					"APPLY_ERROR",
					cursorResult.error,
				),
			);
		}
	}

	// Commit the transaction
	const commitResult = await db.exec("COMMIT");
	if (!commitResult.ok) {
		await db.exec("ROLLBACK");
		return Err(
			new LakeSyncError(
				"Failed to commit transaction for remote delta application",
				"APPLY_ERROR",
				commitResult.error,
			),
		);
	}

	return Ok(appliedCount);
}

/**
 * Apply a single remote delta, checking for conflicts with local pending deltas.
 *
 * @param db - The local SQLite database
 * @param remoteDelta - The remote delta to apply
 * @param resolver - Conflict resolution strategy
 * @param pendingQueue - The local sync queue to check for conflicts
 * @returns Ok(true) if the delta was applied, Ok(false) if skipped (local wins), or Err on failure
 */
async function applyOneDelta(
	db: LocalDB,
	remoteDelta: RowDelta,
	resolver: ConflictResolver,
	pendingQueue: SyncQueue,
	pendingMap: Map<string, QueueEntry>,
): Promise<Result<boolean, LakeSyncError>> {
	const conflictingEntry = pendingMap.get(`${remoteDelta.table}:${remoteDelta.rowId}`);

	if (conflictingEntry) {
		const localDelta = conflictingEntry.delta;
		const resolveResult = resolver.resolve(localDelta, remoteDelta);
		if (!resolveResult.ok) {
			return Err(
				new LakeSyncError(
					`Conflict resolution failed for row "${remoteDelta.rowId}" in table "${remoteDelta.table}"`,
					"APPLY_ERROR",
					resolveResult.error,
				),
			);
		}

		const resolved = resolveResult.value;

		// Determine who won by comparing the resolved delta's clientId and hlc to the remote
		const remoteWon =
			resolved.clientId === remoteDelta.clientId && resolved.hlc === remoteDelta.hlc;

		if (remoteWon) {
			// Remote wins: apply the resolved delta to SQLite and ack the local entry
			const applyResult = await applySqlDelta(db, resolved);
			if (!applyResult.ok) {
				return applyResult;
			}

			const ackResult = await pendingQueue.ack([conflictingEntry.id]);
			if (!ackResult.ok) {
				return Err(
					new LakeSyncError(
						`Failed to ack local queue entry "${conflictingEntry.id}" after remote win`,
						"APPLY_ERROR",
						ackResult.error,
					),
				);
			}

			return Ok(true);
		}

		// Local wins: skip this remote delta, keep local in queue
		return Ok(false);
	}

	// No conflict: apply the remote delta directly
	const applyResult = await applySqlDelta(db, remoteDelta);
	if (!applyResult.ok) {
		return applyResult;
	}

	return Ok(true);
}

/**
 * Apply a single delta as SQL against the local database.
 *
 * - INSERT: creates a new row with the given columns
 * - UPDATE: sets the specified columns on an existing row
 * - DELETE: removes the row from the table
 *
 * @param db - The local SQLite database
 * @param delta - The delta to apply
 * @returns Ok(true) on success, or Err on failure
 */
async function applySqlDelta(
	db: LocalDB,
	delta: RowDelta,
): Promise<Result<boolean, LakeSyncError>> {
	const tableCheck = assertValidIdentifier(delta.table);
	if (!tableCheck.ok) {
		return Err(new LakeSyncError(tableCheck.error.message, "APPLY_ERROR"));
	}
	for (const col of delta.columns) {
		const colCheck = assertValidIdentifier(col.column);
		if (!colCheck.ok) {
			return Err(new LakeSyncError(colCheck.error.message, "APPLY_ERROR"));
		}
	}

	const quotedTable = quoteIdentifier(delta.table);

	switch (delta.op) {
		case "INSERT": {
			const colNames = delta.columns.map((c) => quoteIdentifier(c.column));
			const allColumns = ["_rowId", ...colNames];
			const placeholders = allColumns.map(() => "?").join(", ");
			const values = [delta.rowId, ...delta.columns.map((c) => c.value)];
			const sql = `INSERT INTO ${quotedTable} (${allColumns.join(", ")}) VALUES (${placeholders})`;

			const result = await db.exec(sql, values);
			if (!result.ok) {
				return Err(
					new LakeSyncError(
						`Failed to apply INSERT for row "${delta.rowId}" in table "${delta.table}"`,
						"APPLY_ERROR",
						result.error,
					),
				);
			}
			return Ok(true);
		}

		case "UPDATE": {
			if (delta.columns.length === 0) {
				// No columns to update — nothing to do
				return Ok(true);
			}

			const setClauses = delta.columns.map((c) => `${quoteIdentifier(c.column)} = ?`).join(", ");
			const values = [...delta.columns.map((c) => c.value), delta.rowId];
			const sql = `UPDATE ${quotedTable} SET ${setClauses} WHERE _rowId = ?`;

			const result = await db.exec(sql, values);
			if (!result.ok) {
				return Err(
					new LakeSyncError(
						`Failed to apply UPDATE for row "${delta.rowId}" in table "${delta.table}"`,
						"APPLY_ERROR",
						result.error,
					),
				);
			}
			return Ok(true);
		}

		case "DELETE": {
			const sql = `DELETE FROM ${quotedTable} WHERE _rowId = ?`;
			const result = await db.exec(sql, [delta.rowId]);
			if (!result.ok) {
				return Err(
					new LakeSyncError(
						`Failed to apply DELETE for row "${delta.rowId}" in table "${delta.table}"`,
						"APPLY_ERROR",
						result.error,
					),
				);
			}
			return Ok(true);
		}
	}
}

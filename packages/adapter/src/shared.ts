import { AdapterError, type ColumnDelta, Err, Ok, type Result } from "@lakesync/core";

/**
 * Group raw delta rows by row_id, merge to latest state, and partition into upserts and deletes.
 */
export function groupAndMerge(
	rows: Array<{ row_id: string; columns: string | ColumnDelta[]; op: string }>,
): { upserts: Array<{ rowId: string; state: Record<string, unknown> }>; deleteIds: string[] } {
	const byRowId = new Map<string, Array<{ columns: string | ColumnDelta[]; op: string }>>();
	for (const row of rows) {
		let arr = byRowId.get(row.row_id);
		if (!arr) {
			arr = [];
			byRowId.set(row.row_id, arr);
		}
		arr.push(row);
	}

	const upserts: Array<{ rowId: string; state: Record<string, unknown> }> = [];
	const deleteIds: string[] = [];

	for (const [rowId, group] of byRowId) {
		const state = mergeLatestState(group);
		if (state !== null) {
			upserts.push({ rowId, state });
		} else {
			deleteIds.push(rowId);
		}
	}

	return { upserts, deleteIds };
}

/** Normalise a caught value into an Error or undefined. */
export function toCause(error: unknown): Error | undefined {
	return error instanceof Error ? error : undefined;
}

/** Execute an async operation and wrap errors into an AdapterError Result. */
export async function wrapAsync<T>(
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
 * Merge delta rows into final state using column-level LWW.
 * Shared by Postgres, MySQL, and BigQuery getLatestState implementations.
 * Rows must be sorted by HLC ascending.
 */
export function mergeLatestState(
	rows: Array<{ columns: string | ColumnDelta[]; op: string }>,
): Record<string, unknown> | null {
	if (rows.length === 0) return null;

	const lastRow = rows[rows.length - 1]!;
	if (lastRow.op === "DELETE") return null;

	const state: Record<string, unknown> = {};

	for (const row of rows) {
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
}

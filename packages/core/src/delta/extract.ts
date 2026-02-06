import equal from "fast-deep-equal";
import stableStringify from "fast-json-stable-stringify";
import type { HLCTimestamp } from "../hlc/types";
import type { ColumnDelta, RowDelta, TableSchema } from "./types";

/**
 * Extract a column-level delta between two row states.
 *
 * - `before` null/undefined + `after` present -> INSERT (all columns)
 * - `before` present + `after` null/undefined -> DELETE (empty columns)
 * - Both present -> compare each column, emit only changed columns as UPDATE
 * - No columns changed -> returns null (no-op)
 *
 * If `schema` is provided, only columns listed in the schema are considered.
 *
 * @param before - The previous row state, or null/undefined for a new row
 * @param after - The current row state, or null/undefined for a deleted row
 * @param opts - Table name, row ID, client ID, HLC timestamp, and optional schema
 * @returns The extracted RowDelta, or null if nothing changed
 */
export async function extractDelta(
	before: Record<string, unknown> | null | undefined,
	after: Record<string, unknown> | null | undefined,
	opts: {
		table: string;
		rowId: string;
		clientId: string;
		hlc: HLCTimestamp;
		schema?: TableSchema;
	},
): Promise<RowDelta | null> {
	const { table, rowId, clientId, hlc, schema } = opts;

	const beforeExists = before != null;
	const afterExists = after != null;

	if (!beforeExists && !afterExists) {
		return null;
	}

	// INSERT: no previous state, new state exists
	if (!beforeExists && afterExists) {
		const columns = buildColumns(after, schema);
		const deltaId = await generateDeltaId({ clientId, hlc, table, rowId, columns });
		return { op: "INSERT", table, rowId, clientId, columns, hlc, deltaId };
	}

	// DELETE: previous state exists, no new state
	if (beforeExists && !afterExists) {
		const columns: ColumnDelta[] = [];
		const deltaId = await generateDeltaId({ clientId, hlc, table, rowId, columns });
		return { op: "DELETE", table, rowId, clientId, columns, hlc, deltaId };
	}

	// UPDATE: both states exist — compare columns
	const columns = diffColumns(before!, after!, schema);
	if (columns.length === 0) {
		return null;
	}

	const deltaId = await generateDeltaId({ clientId, hlc, table, rowId, columns });
	return { op: "UPDATE", table, rowId, clientId, columns, hlc, deltaId };
}

/** Build an allow-set from a schema, or null if no schema is provided. */
function allowedSet(schema?: TableSchema): Set<string> | null {
	return schema ? new Set(schema.columns.map((c) => c.name)) : null;
}

/**
 * Build column deltas from a row, optionally filtered by schema.
 * Skips columns whose value is undefined (treated as absent).
 */
function buildColumns(row: Record<string, unknown>, schema?: TableSchema): ColumnDelta[] {
	const allowed = allowedSet(schema);
	const columns: ColumnDelta[] = [];

	for (const [key, value] of Object.entries(row)) {
		if (value === undefined) continue;
		if (allowed && !allowed.has(key)) continue;
		columns.push({ column: key, value });
	}

	return columns;
}

/**
 * Diff two row objects and return only the changed columns.
 * Uses Object.is() for primitives and fast-deep-equal for objects/arrays.
 */
function diffColumns(
	before: Record<string, unknown>,
	after: Record<string, unknown>,
	schema?: TableSchema,
): ColumnDelta[] {
	const allowed = allowedSet(schema);
	const allKeys = new Set([...Object.keys(before), ...Object.keys(after)]);
	const columns: ColumnDelta[] = [];

	for (const key of allKeys) {
		if (allowed && !allowed.has(key)) continue;

		const beforeVal = before[key];
		const afterVal = after[key];

		// Skip absent or removed columns
		if (afterVal === undefined) continue;

		// New column — before was undefined
		if (beforeVal === undefined) {
			columns.push({ column: key, value: afterVal });
			continue;
		}

		// Exact primitive equality (handles NaN, +0/-0)
		if (Object.is(beforeVal, afterVal)) continue;

		// Deep equality for objects/arrays (key-order-agnostic)
		if (
			typeof beforeVal === "object" &&
			beforeVal !== null &&
			typeof afterVal === "object" &&
			afterVal !== null &&
			equal(beforeVal, afterVal)
		) {
			continue;
		}

		columns.push({ column: key, value: afterVal });
	}

	return columns;
}

/**
 * Generate a deterministic delta ID using SHA-256.
 * Uses the Web Crypto API (works in both Bun and browsers).
 */
async function generateDeltaId(params: {
	clientId: string;
	hlc: HLCTimestamp;
	table: string;
	rowId: string;
	columns: ColumnDelta[];
}): Promise<string> {
	const payload = stableStringify({
		clientId: params.clientId,
		hlc: params.hlc.toString(),
		table: params.table,
		rowId: params.rowId,
		columns: params.columns,
	});

	const data = new TextEncoder().encode(payload);
	const hashBuffer = await crypto.subtle.digest("SHA-256", data);
	const bytes = new Uint8Array(hashBuffer);

	let hex = "";
	for (const b of bytes) {
		hex += b.toString(16).padStart(2, "0");
	}
	return hex;
}

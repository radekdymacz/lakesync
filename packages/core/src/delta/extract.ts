import equal from "fast-deep-equal";
import stableStringify from "fast-json-stable-stringify";
import type { HLCTimestamp } from "../hlc/types";
import type { ColumnDelta, RowDelta, TableSchema } from "./types";

/**
 * Extract a column-level delta between two row states.
 *
 * - `before` null/undefined + `after` present → INSERT (all columns)
 * - `before` present + `after` null/undefined → DELETE (empty columns)
 * - Both present → compare each column, emit only changed columns as UPDATE
 * - No columns changed → returns null (no-op)
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

	// Both absent — nothing to do
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
	// TypeScript narrowing: at this point both before and after are non-null
	const columns = diffColumns(before!, after!, schema);

	if (columns.length === 0) {
		return null;
	}

	const deltaId = await generateDeltaId({ clientId, hlc, table, rowId, columns });
	return { op: "UPDATE", table, rowId, clientId, columns, hlc, deltaId };
}

/**
 * Build column deltas from a row, optionally filtered by schema.
 * Skips columns whose value is undefined (treated as absent).
 */
function buildColumns(row: Record<string, unknown>, schema?: TableSchema): ColumnDelta[] {
	const allowedColumns = schema ? new Set(schema.columns.map((c) => c.name)) : null;

	const columns: ColumnDelta[] = [];

	for (const [key, value] of Object.entries(row)) {
		if (value === undefined) continue;
		if (allowedColumns && !allowedColumns.has(key)) continue;
		columns.push({ column: key, value });
	}

	return columns;
}

/**
 * Diff two row objects and return only the changed columns.
 * Uses Object.is() for primitives and fast-deep-equal for objects/arrays.
 * Skips columns whose value is undefined in both before and after.
 */
function diffColumns(
	before: Record<string, unknown>,
	after: Record<string, unknown>,
	schema?: TableSchema,
): ColumnDelta[] {
	const allowedColumns = schema ? new Set(schema.columns.map((c) => c.name)) : null;

	// Collect all unique keys from both objects
	const allKeys = new Set([...Object.keys(before), ...Object.keys(after)]);

	const columns: ColumnDelta[] = [];

	for (const key of allKeys) {
		if (allowedColumns && !allowedColumns.has(key)) continue;

		const beforeVal = before[key];
		const afterVal = after[key];

		// Skip if both are undefined (absent in both states)
		if (beforeVal === undefined && afterVal === undefined) continue;

		// Skip columns where after is undefined (column removed — treat as absent)
		if (afterVal === undefined) continue;

		// If before is undefined but after is not, it's a new column
		if (beforeVal === undefined) {
			columns.push({ column: key, value: afterVal });
			continue;
		}

		// For primitives, use Object.is for exact comparison (handles NaN, +0/-0)
		if (Object.is(beforeVal, afterVal)) continue;

		// For objects/arrays, use deep equality (key-order-agnostic)
		if (
			typeof beforeVal === "object" &&
			beforeVal !== null &&
			typeof afterVal === "object" &&
			afterVal !== null
		) {
			if (equal(beforeVal, afterVal)) continue;
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

	const encoder = new TextEncoder();
	const data = encoder.encode(payload);
	const hashBuffer = await crypto.subtle.digest("SHA-256", data);
	const hashArray = new Uint8Array(hashBuffer);
	return Array.from(hashArray)
		.map((b) => b.toString(16).padStart(2, "0"))
		.join("");
}

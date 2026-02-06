import type { RowDelta } from "./types";

/**
 * Apply a delta to an existing row, returning the merged result.
 *
 * - DELETE → returns null
 * - INSERT → creates a new row from delta columns
 * - UPDATE → merges delta columns onto existing row (immutable — returns a new object)
 *
 * @param row - The current row state, or null if no row exists
 * @param delta - The delta to apply
 * @returns The merged row, or null for DELETE operations
 */
export function applyDelta(
	row: Record<string, unknown> | null,
	delta: RowDelta,
): Record<string, unknown> | null {
	if (delta.op === "DELETE") return null;

	const base: Record<string, unknown> = row ? { ...row } : {};
	for (const col of delta.columns) {
		base[col.column] = col.value;
	}
	return base;
}

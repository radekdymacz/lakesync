import type { RowDelta, ColumnDelta, DeltaOp } from '../delta/types';
import type { HLCTimestamp } from '../hlc/types';
import { HLC } from '../hlc/hlc';
import { Ok, Err } from '../result/result';
import type { Result } from '../result/result';
import { ConflictError } from '../result/errors';
import type { ConflictResolver } from './resolver';

/**
 * Column-level Last-Write-Wins conflict resolver.
 *
 * For each column present in both deltas, the one with the higher HLC wins.
 * Equal HLC tiebreak: lexicographically higher clientId wins (deterministic).
 * Columns only present in one delta are always included in the result.
 */
export class LWWResolver implements ConflictResolver {
	/**
	 * Resolve two conflicting deltas for the same row, returning the merged result.
	 *
	 * Rules:
	 * - Both DELETE: the delta with the higher HLC (or clientId tiebreak) wins.
	 * - One DELETE, one non-DELETE: the delta with the higher HLC wins.
	 *   If the DELETE wins, the row is tombstoned (empty columns).
	 *   If the non-DELETE wins, the row is resurrected.
	 * - Both non-DELETE: columns are merged per-column using LWW semantics.
	 *
	 * @param local  - The locally held delta for this row.
	 * @param remote - The incoming remote delta for this row.
	 * @returns A `Result` containing the resolved `RowDelta`, or a
	 *          `ConflictError` if the deltas refer to different tables/rows.
	 */
	resolve(local: RowDelta, remote: RowDelta): Result<RowDelta, ConflictError> {
		// Validate same table + rowId
		if (local.table !== remote.table || local.rowId !== remote.rowId) {
			return Err(
				new ConflictError(
					`Cannot resolve conflict: mismatched table/rowId ` +
						`(${local.table}:${local.rowId} vs ${remote.table}:${remote.rowId})`,
				),
			);
		}

		// Determine which delta has higher HLC (for op-level decisions)
		const winner = pickWinner(local, remote);

		// Both DELETE — winner takes all (no columns to merge)
		if (local.op === 'DELETE' && remote.op === 'DELETE') {
			return Ok({ ...winner, columns: [] });
		}

		// One is DELETE
		if (local.op === 'DELETE' || remote.op === 'DELETE') {
			const deleteDelta = local.op === 'DELETE' ? local : remote;
			const otherDelta = local.op === 'DELETE' ? remote : local;

			// If the DELETE has higher/equal priority, tombstone wins
			if (deleteDelta === winner) {
				return Ok({ ...deleteDelta, columns: [] });
			}
			// Otherwise the UPDATE/INSERT wins (resurrection)
			return Ok({ ...otherDelta });
		}

		// Both are INSERT or UPDATE — merge columns
		const mergedColumns = mergeColumns(local, remote);

		// Determine the resulting op: INSERT only if both are INSERT, otherwise UPDATE
		const op: DeltaOp =
			local.op === 'INSERT' && remote.op === 'INSERT' ? 'INSERT' : 'UPDATE';

		return Ok({
			op,
			table: local.table,
			rowId: local.rowId,
			clientId: winner.clientId,
			columns: mergedColumns,
			hlc: winner.hlc,
			deltaId: winner.deltaId,
		});
	}
}

/**
 * Pick the winning delta based on HLC comparison with clientId tiebreak.
 *
 * @param local  - The locally held delta.
 * @param remote - The incoming remote delta.
 * @returns The delta that wins the comparison.
 */
function pickWinner(local: RowDelta, remote: RowDelta): RowDelta {
	const hlcCmp = HLC.compare(local.hlc, remote.hlc);
	if (hlcCmp > 0) return local;
	if (hlcCmp < 0) return remote;
	// Equal HLC — lexicographically higher clientId wins
	return local.clientId > remote.clientId ? local : remote;
}

/**
 * Merge column-level changes from two non-DELETE deltas using LWW semantics.
 *
 * - Columns present in only one delta are included unconditionally.
 * - Columns present in both: the value from the delta with the higher HLC wins;
 *   equal HLC uses lexicographic clientId tiebreak.
 *
 * @param local  - The locally held delta.
 * @param remote - The incoming remote delta.
 * @returns The merged array of column deltas.
 */
function mergeColumns(local: RowDelta, remote: RowDelta): ColumnDelta[] {
	const localMap = new Map(local.columns.map((c) => [c.column, c]));
	const remoteMap = new Map(remote.columns.map((c) => [c.column, c]));
	const allColumns = new Set([...localMap.keys(), ...remoteMap.keys()]);

	const merged: ColumnDelta[] = [];

	for (const col of allColumns) {
		const localCol = localMap.get(col);
		const remoteCol = remoteMap.get(col);

		if (localCol && !remoteCol) {
			merged.push(localCol);
		} else if (!localCol && remoteCol) {
			merged.push(remoteCol);
		} else if (localCol && remoteCol) {
			// Both have this column — HLC wins, clientId tiebreak
			const cmp = HLC.compare(local.hlc, remote.hlc);
			if (cmp > 0) {
				merged.push(localCol);
			} else if (cmp < 0) {
				merged.push(remoteCol);
			} else {
				// Equal HLC — lexicographically higher clientId wins
				merged.push(
					local.clientId > remote.clientId ? localCol : remoteCol,
				);
			}
		}
	}

	return merged;
}

/**
 * Convenience function — resolves two conflicting deltas using the
 * column-level Last-Write-Wins strategy.
 *
 * @param local  - The locally held delta for this row.
 * @param remote - The incoming remote delta for this row.
 * @returns A `Result` containing the resolved `RowDelta`, or a
 *          `ConflictError` if the deltas refer to different tables/rows.
 */
export function resolveLWW(
	local: RowDelta,
	remote: RowDelta,
): Result<RowDelta, ConflictError> {
	const resolver = new LWWResolver();
	return resolver.resolve(local, remote);
}

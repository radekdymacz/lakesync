import type { LakeSyncError, Result } from "@lakesync/core";
import { useCallback } from "react";
import { useLakeSyncData, useLakeSyncStable } from "./context";

/** Return type of `useMutation`. */
export interface UseMutationResult {
	insert: (
		table: string,
		rowId: string,
		data: Record<string, unknown>,
	) => Promise<Result<void, LakeSyncError>>;
	update: (
		table: string,
		rowId: string,
		data: Record<string, unknown>,
	) => Promise<Result<void, LakeSyncError>>;
	remove: (table: string, rowId: string) => Promise<Result<void, LakeSyncError>>;
}

/**
 * Wraps SyncTracker mutations with automatic query invalidation.
 *
 * After each successful mutation, the affected table's version is
 * incremented so only `useQuery` hooks reading from that table re-run.
 */
export function useMutation(): UseMutationResult {
	const { tracker } = useLakeSyncStable();
	const { invalidateTables } = useLakeSyncData();

	const insert = useCallback(
		async (
			table: string,
			rowId: string,
			data: Record<string, unknown>,
		): Promise<Result<void, LakeSyncError>> => {
			const result = await tracker.insert(table, rowId, data);
			if (result.ok) invalidateTables([table]);
			return result;
		},
		[tracker, invalidateTables],
	);

	const update = useCallback(
		async (
			table: string,
			rowId: string,
			data: Record<string, unknown>,
		): Promise<Result<void, LakeSyncError>> => {
			const result = await tracker.update(table, rowId, data);
			if (result.ok) invalidateTables([table]);
			return result;
		},
		[tracker, invalidateTables],
	);

	const remove = useCallback(
		async (table: string, rowId: string): Promise<Result<void, LakeSyncError>> => {
			const result = await tracker.delete(table, rowId);
			if (result.ok) invalidateTables([table]);
			return result;
		},
		[tracker, invalidateTables],
	);

	return { insert, update, remove };
}

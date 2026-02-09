import type { LakeSyncError, Result } from "@lakesync/core";
import { useCallback } from "react";
import { useLakeSync } from "./context";

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
 * After each successful mutation, `dataVersion` is incremented so all
 * active `useQuery` hooks re-run.
 */
export function useMutation(): UseMutationResult {
	const { tracker, invalidate } = useLakeSync();

	const insert = useCallback(
		async (
			table: string,
			rowId: string,
			data: Record<string, unknown>,
		): Promise<Result<void, LakeSyncError>> => {
			const result = await tracker.insert(table, rowId, data);
			if (result.ok) invalidate();
			return result;
		},
		[tracker, invalidate],
	);

	const update = useCallback(
		async (
			table: string,
			rowId: string,
			data: Record<string, unknown>,
		): Promise<Result<void, LakeSyncError>> => {
			const result = await tracker.update(table, rowId, data);
			if (result.ok) invalidate();
			return result;
		},
		[tracker, invalidate],
	);

	const remove = useCallback(
		async (table: string, rowId: string): Promise<Result<void, LakeSyncError>> => {
			const result = await tracker.delete(table, rowId);
			if (result.ok) invalidate();
			return result;
		},
		[tracker, invalidate],
	);

	return { insert, update, remove };
}

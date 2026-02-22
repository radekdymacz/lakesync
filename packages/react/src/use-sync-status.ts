import { useCallback, useEffect, useState } from "react";
import { useLakeSyncStable } from "./context";

/** Return type of `useSyncStatus`. */
export interface UseSyncStatusResult {
	isSyncing: boolean;
	lastSyncTime: Date | null;
	queueDepth: number;
	error: Error | null;
}

/**
 * Observe the sync lifecycle.
 *
 * Reads engine state directly for sync status and subscribes to
 * events only for invalidation (re-reading the state snapshot).
 * Uses the stable context so it does not re-render on data version changes.
 */
export function useSyncStatus(): UseSyncStatusResult {
	const { coordinator } = useLakeSyncStable();
	const [status, setStatus] = useState<UseSyncStatusResult>({
		isSyncing: false,
		lastSyncTime: null,
		queueDepth: 0,
		error: null,
	});

	const refreshStatus = useCallback(async () => {
		const { engine } = coordinator;
		const depth = await coordinator.queueDepth();
		setStatus((prev) => ({
			isSyncing: engine.syncing,
			lastSyncTime: engine.lastSyncTime,
			queueDepth: depth,
			error: prev.error,
		}));
	}, [coordinator]);

	useEffect(() => {
		// Initial status read
		refreshStatus();

		return coordinator.subscribe({
			onSyncStart: () => {
				setStatus((prev) => ({ ...prev, isSyncing: true }));
			},
			onSyncComplete: () => {
				const { engine } = coordinator;
				setStatus((prev) => ({
					...prev,
					isSyncing: false,
					lastSyncTime: engine.lastSyncTime,
					error: null,
				}));
				refreshStatus();
			},
			onError: (err: Error) => {
				setStatus((prev) => ({
					...prev,
					isSyncing: false,
					error: err,
				}));
			},
			onChange: () => {
				refreshStatus();
			},
		});
	}, [coordinator, refreshStatus]);

	return status;
}

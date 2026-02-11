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
 * Reads `coordinator.state` directly for sync status and subscribes to
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
		const state = coordinator.state;
		const depth = await coordinator.queueDepth();
		setStatus((prev) => ({
			isSyncing: state.syncing,
			lastSyncTime: state.lastSyncTime,
			queueDepth: depth,
			error: prev.error,
		}));
	}, [coordinator]);

	useEffect(() => {
		const handleSyncStart = () => {
			setStatus((prev) => ({ ...prev, isSyncing: true }));
		};

		const handleSyncComplete = () => {
			const state = coordinator.state;
			setStatus((prev) => ({
				...prev,
				isSyncing: false,
				lastSyncTime: state.lastSyncTime,
				error: null,
			}));
			refreshStatus();
		};

		const handleError = (err: Error) => {
			setStatus((prev) => ({
				...prev,
				isSyncing: false,
				error: err,
			}));
		};

		const handleChange = () => {
			refreshStatus();
		};

		coordinator.on("onSyncStart", handleSyncStart);
		coordinator.on("onSyncComplete", handleSyncComplete);
		coordinator.on("onError", handleError);
		coordinator.on("onChange", handleChange);

		// Initial status read
		refreshStatus();

		return () => {
			coordinator.off("onSyncStart", handleSyncStart);
			coordinator.off("onSyncComplete", handleSyncComplete);
			coordinator.off("onError", handleError);
			coordinator.off("onChange", handleChange);
		};
	}, [coordinator, refreshStatus]);

	return status;
}

import { useCallback, useEffect, useState } from "react";
import { useLakeSync } from "./context";

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
 * Tracks whether a sync is in progress, last successful sync time,
 * outbox queue depth, and the most recent sync error (cleared on success).
 */
export function useSyncStatus(): UseSyncStatusResult {
	const { coordinator, dataVersion } = useLakeSync();
	const [isSyncing, setIsSyncing] = useState(false);
	const [lastSyncTime, setLastSyncTime] = useState<Date | null>(null);
	const [queueDepth, setQueueDepth] = useState(0);
	const [error, setError] = useState<Error | null>(null);

	const refreshQueueDepth = useCallback(async () => {
		const depth = await coordinator.queueDepth();
		setQueueDepth(depth);
	}, [coordinator]);

	useEffect(() => {
		const handleSyncComplete = () => {
			setIsSyncing(false);
			setLastSyncTime(coordinator.lastSyncTime);
			setError(null);
			refreshQueueDepth();
		};

		const handleError = (err: Error) => {
			setIsSyncing(false);
			setError(err);
		};

		const handleChange = () => {
			refreshQueueDepth();
		};

		coordinator.on("onSyncComplete", handleSyncComplete);
		coordinator.on("onError", handleError);
		coordinator.on("onChange", handleChange);

		// Initial queue depth
		refreshQueueDepth();

		return () => {
			coordinator.off("onSyncComplete", handleSyncComplete);
			coordinator.off("onError", handleError);
			coordinator.off("onChange", handleChange);
		};
	}, [coordinator, refreshQueueDepth]);

	// Also refresh queue depth when dataVersion changes (local mutations)
	useEffect(() => {
		void dataVersion;
		refreshQueueDepth();
	}, [dataVersion, refreshQueueDepth]);

	return { isSyncing, lastSyncTime, queueDepth, error };
}

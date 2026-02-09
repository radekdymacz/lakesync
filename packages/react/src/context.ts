import type { SyncCoordinator, SyncTracker } from "@lakesync/client";
import {
	createContext,
	createElement,
	useCallback,
	useContext,
	useEffect,
	useMemo,
	useState,
} from "react";

/** Internal context value shared across all hooks. */
export interface LakeSyncContextValue {
	coordinator: SyncCoordinator;
	tracker: SyncTracker;
	/** Monotonically increasing counter — bumped on every data change. */
	dataVersion: number;
	/** Increment dataVersion to trigger query re-runs. */
	invalidate: () => void;
}

const LakeSyncContext = createContext<LakeSyncContextValue | null>(null);

/** Props for the LakeSyncProvider component. */
export interface LakeSyncProviderProps {
	/** An already-constructed SyncCoordinator instance. */
	coordinator: SyncCoordinator;
	children: React.ReactNode;
}

/**
 * Provides LakeSync context to the component tree.
 *
 * Subscribes to `onChange` events from the coordinator and maintains a
 * `dataVersion` counter that increments on every remote delta application,
 * triggering reactive query re-runs in `useQuery`.
 */
export function LakeSyncProvider(props: LakeSyncProviderProps) {
	const { coordinator, children } = props;
	const [dataVersion, setDataVersion] = useState(0);

	const invalidate = useCallback(() => {
		setDataVersion((v) => v + 1);
	}, []);

	useEffect(() => {
		const handleChange = () => {
			setDataVersion((v) => v + 1);
		};
		coordinator.on("onChange", handleChange);
		return () => {
			coordinator.off("onChange", handleChange);
		};
	}, [coordinator]);

	const value = useMemo<LakeSyncContextValue>(
		() => ({
			coordinator,
			tracker: coordinator.tracker,
			dataVersion,
			invalidate,
		}),
		[coordinator, dataVersion, invalidate],
	);

	return createElement(LakeSyncContext.Provider, { value }, children);
}

/**
 * Access the raw LakeSync SDK instances from context.
 *
 * @throws if called outside a `<LakeSyncProvider>`.
 */
export function useLakeSync(): LakeSyncContextValue {
	const ctx = useContext(LakeSyncContext);
	if (!ctx) {
		throw new Error("useLakeSync must be used within a <LakeSyncProvider>");
	}
	return ctx;
}

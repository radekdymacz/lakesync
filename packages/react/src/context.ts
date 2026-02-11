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

/** Stable context value — coordinator and tracker references that never change. */
export interface LakeSyncStableContextValue {
	coordinator: SyncCoordinator;
	tracker: SyncTracker;
}

/** Reactive context value — data version that changes on every delta. */
export interface LakeSyncDataContextValue {
	/** Monotonically increasing counter — bumped on every data change. */
	dataVersion: number;
	/** Increment dataVersion to trigger query re-runs. */
	invalidate: () => void;
}

/**
 * Combined context value for backwards compatibility.
 * @see useLakeSync
 */
export interface LakeSyncContextValue
	extends LakeSyncStableContextValue,
		LakeSyncDataContextValue {}

const LakeSyncStableContext = createContext<LakeSyncStableContextValue | null>(null);
const LakeSyncDataContext = createContext<LakeSyncDataContextValue | null>(null);

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
 *
 * Uses a split context pattern: stable refs (coordinator, tracker) are
 * provided separately from reactive data (dataVersion) so that hooks
 * like `useSyncStatus` and `useAction` do not re-render on data changes.
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

	const stableValue = useMemo<LakeSyncStableContextValue>(
		() => ({
			coordinator,
			tracker: coordinator.tracker,
		}),
		[coordinator],
	);

	const dataValue = useMemo<LakeSyncDataContextValue>(
		() => ({
			dataVersion,
			invalidate,
		}),
		[dataVersion, invalidate],
	);

	return createElement(
		LakeSyncStableContext.Provider,
		{ value: stableValue },
		createElement(LakeSyncDataContext.Provider, { value: dataValue }, children),
	);
}

/**
 * Access the stable LakeSync SDK instances from context.
 *
 * This hook does NOT cause re-renders when dataVersion changes.
 * Use {@link useLakeSyncData} for reactive data.
 *
 * @throws if called outside a `<LakeSyncProvider>`.
 */
export function useLakeSyncStable(): LakeSyncStableContextValue {
	const ctx = useContext(LakeSyncStableContext);
	if (!ctx) {
		throw new Error("useLakeSyncStable must be used within a <LakeSyncProvider>");
	}
	return ctx;
}

/**
 * Access reactive data (dataVersion, invalidate) from context.
 *
 * @throws if called outside a `<LakeSyncProvider>`.
 */
export function useLakeSyncData(): LakeSyncDataContextValue {
	const ctx = useContext(LakeSyncDataContext);
	if (!ctx) {
		throw new Error("useLakeSyncData must be used within a <LakeSyncProvider>");
	}
	return ctx;
}

/**
 * Access the raw LakeSync SDK instances from context.
 *
 * Returns both stable and reactive values for backwards compatibility.
 *
 * @throws if called outside a `<LakeSyncProvider>`.
 */
export function useLakeSync(): LakeSyncContextValue {
	const stable = useLakeSyncStable();
	const data = useLakeSyncData();
	return { ...stable, ...data };
}

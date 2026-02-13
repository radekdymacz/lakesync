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
	/**
	 * Global version counter — bumped when table info is unavailable.
	 * Used by `useQuery` as a fallback to ensure all queries re-run
	 * when the specific affected tables are unknown.
	 */
	globalVersion: number;
	/** Per-table version counters — bumped only for affected tables. */
	tableVersions: ReadonlyMap<string, number>;
	/** Increment dataVersion to trigger query re-runs (all tables). */
	invalidate: () => void;
	/** Increment version for specific tables only. */
	invalidateTables: (tables: string[]) => void;
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
 * Also maintains per-table version counters (`tableVersions`) so that
 * `useQuery` can subscribe to only the tables it reads from, avoiding
 * unnecessary re-renders when unrelated tables change.
 *
 * Uses a split context pattern: stable refs (coordinator, tracker) are
 * provided separately from reactive data (dataVersion) so that hooks
 * like `useSyncStatus` and `useAction` do not re-render on data changes.
 */
export function LakeSyncProvider(props: LakeSyncProviderProps) {
	const { coordinator, children } = props;
	const [dataVersion, setDataVersion] = useState(0);
	const [globalVersion, setGlobalVersion] = useState(0);
	const [tableVersions, setTableVersions] = useState<ReadonlyMap<string, number>>(new Map());

	const invalidateTables = useCallback((tables: string[]) => {
		setDataVersion((v) => v + 1);
		setTableVersions((prev) => {
			const next = new Map(prev);
			for (const table of tables) {
				next.set(table, (next.get(table) ?? 0) + 1);
			}
			return next;
		});
	}, []);

	const invalidate = useCallback(() => {
		setDataVersion((v) => v + 1);
		setGlobalVersion((v) => v + 1);
	}, []);

	useEffect(() => {
		const handleChange = (_count: number, tables?: string[]) => {
			if (tables && tables.length > 0) {
				invalidateTables(tables);
			} else {
				invalidate();
			}
		};
		coordinator.on("onChange", handleChange);
		return () => {
			coordinator.off("onChange", handleChange);
		};
	}, [coordinator, invalidate, invalidateTables]);

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
			globalVersion,
			tableVersions,
			invalidate,
			invalidateTables,
		}),
		[dataVersion, globalVersion, tableVersions, invalidate, invalidateTables],
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
 * Access reactive data (dataVersion, tableVersions, invalidate) from context.
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

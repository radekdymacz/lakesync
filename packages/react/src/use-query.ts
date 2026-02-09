import type { DbError } from "@lakesync/client";
import { useCallback, useEffect, useRef, useState } from "react";
import { useLakeSync } from "./context";

/** Return type of `useQuery`. */
export interface UseQueryResult<T> {
	data: T[];
	error: DbError | null;
	isLoading: boolean;
	refetch: () => void;
}

/**
 * Reactive SQL query hook.
 *
 * Re-runs automatically when:
 * - Remote deltas are applied (via `onChange`)
 * - A local mutation completes (via `invalidate()`)
 * - The `sql` or `params` arguments change
 *
 * @param sql - SQL query string
 * @param params - Optional bind parameters
 */
export function useQuery<T>(sql: string, params?: unknown[]): UseQueryResult<T> {
	const { tracker, dataVersion } = useLakeSync();
	const [data, setData] = useState<T[]>([]);
	const [error, setError] = useState<DbError | null>(null);
	const [isLoading, setIsLoading] = useState(true);
	const [manualTrigger, setManualTrigger] = useState(0);

	// Stable serialisation of params for dependency comparison
	const paramsKey = JSON.stringify(params ?? []);
	const paramsRef = useRef(params);
	paramsRef.current = params;

	const refetch = useCallback(() => {
		setManualTrigger((v) => v + 1);
	}, []);

	// Combine all trigger values into a single version for the effect.
	// This satisfies exhaustive-deps: the effect reads `version` directly.
	const version = `${dataVersion}:${manualTrigger}:${paramsKey}`;

	useEffect(() => {
		// Read version to establish dependency
		void version;
		let cancelled = false;

		const run = async () => {
			const result = await tracker.query<T>(sql, paramsRef.current);
			if (cancelled) return;

			if (result.ok) {
				setData(result.value);
				setError(null);
			} else {
				setError(result.error);
			}
			setIsLoading(false);
		};

		run();

		return () => {
			cancelled = true;
		};
	}, [tracker, sql, version]);

	return { data, error, isLoading, refetch };
}

import type { DbError } from "@lakesync/client";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useLakeSyncData, useLakeSyncStable } from "./context";
import { extractTables } from "./extract-tables";

/** Options for `useQuery`. */
export interface UseQueryOptions {
	/**
	 * Explicit list of table names this query depends on.
	 * When provided, skips automatic table extraction from the SQL string.
	 * Useful for queries with CTEs, subqueries, or dynamically-built SQL
	 * where regex-based extraction may not be accurate.
	 */
	tables?: string[];
}

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
 * - Remote deltas are applied to tables referenced by this query (via `onChange`)
 * - A local mutation completes (via `invalidate()` or `invalidateTables()`)
 * - The `sql` or `params` arguments change
 *
 * Uses table-scoped reactivity: a delta to table A will not re-run a query
 * that only reads from table B. Table names are extracted from FROM and JOIN
 * clauses via basic regex matching.
 *
 * @param sql - SQL query string
 * @param params - Optional bind parameters
 * @param options - Optional configuration (e.g. explicit table list)
 */
export function useQuery<T>(
	sql: string,
	params?: unknown[],
	options?: UseQueryOptions,
): UseQueryResult<T> {
	const { tracker } = useLakeSyncStable();
	const { globalVersion, tableVersions } = useLakeSyncData();
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

	// Use explicit tables if provided, otherwise extract from SQL
	const tables = useMemo(() => options?.tables ?? extractTables(sql), [sql, options?.tables]);

	// Compute a composite version key from only the tables this query reads from.
	// When a table has no entry in tableVersions yet, it defaults to 0.
	// globalVersion acts as a fallback — bumped when table info is unavailable
	// (e.g. from an onChange without table names), ensuring all queries re-run.
	const tableVersionKey = useMemo(() => {
		if (tables.length === 0) {
			return `unknown`;
		}
		return tables.map((t) => `${t}:${tableVersions.get(t) ?? 0}`).join(",");
	}, [tables, tableVersions]);

	// Combine all trigger values into a single version for the effect.
	const version = `${globalVersion}:${tableVersionKey}:${manualTrigger}:${paramsKey}`;

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

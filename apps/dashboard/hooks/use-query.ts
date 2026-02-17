"use client";

import { useCallback, useEffect, useRef, useState } from "react";

interface QueryState<T> {
	data: T | null;
	loading: boolean;
	error: string | null;
}

export function useApiQuery<T>(url: string | null): QueryState<T> & { refetch: () => void } {
	const [state, setState] = useState<QueryState<T>>({
		data: null,
		loading: url !== null,
		error: null,
	});
	const controllerRef = useRef<AbortController | null>(null);

	const fetchData = useCallback(
		async (signal: AbortSignal) => {
			if (!url) return;
			setState((prev) => ({ ...prev, loading: true, error: null }));
			try {
				const res = await fetch(url, { signal });
				if (!res.ok) {
					setState((prev) => ({
						...prev,
						loading: false,
						error: `HTTP ${res.status}`,
					}));
					return;
				}
				const data = (await res.json()) as T;
				setState({ data, loading: false, error: null });
			} catch (err) {
				if (signal.aborted) return;
				setState((prev) => ({
					...prev,
					loading: false,
					error: err instanceof Error ? err.message : "Unknown error",
				}));
			}
		},
		[url],
	);

	useEffect(() => {
		if (!url) {
			setState({ data: null, loading: false, error: null });
			return;
		}
		controllerRef.current?.abort();
		const controller = new AbortController();
		controllerRef.current = controller;
		fetchData(controller.signal);
		return () => controller.abort();
	}, [url, fetchData]);

	const refetch = useCallback(() => {
		if (!url) return;
		controllerRef.current?.abort();
		const controller = new AbortController();
		controllerRef.current = controller;
		fetchData(controller.signal);
	}, [url, fetchData]);

	return { ...state, refetch };
}

"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export type QueryResult<T> =
	| { status: "idle" }
	| { status: "loading" }
	| { status: "error"; error: string }
	| { status: "success"; data: T };

export function useApiQuery<T>(url: string | null): QueryResult<T> & { refetch: () => void } {
	const [state, setState] = useState<QueryResult<T>>(
		url !== null ? { status: "loading" } : { status: "idle" },
	);
	const controllerRef = useRef<AbortController | null>(null);

	const fetchData = useCallback(
		async (signal: AbortSignal) => {
			if (!url) return;
			setState({ status: "loading" });
			try {
				const res = await fetch(url, { signal });
				if (!res.ok) {
					setState({ status: "error", error: `HTTP ${res.status}` });
					return;
				}
				const data = (await res.json()) as T;
				setState({ status: "success", data });
			} catch (err) {
				if (signal.aborted) return;
				setState({
					status: "error",
					error: err instanceof Error ? err.message : "Unknown error",
				});
			}
		},
		[url],
	);

	useEffect(() => {
		if (!url) {
			setState({ status: "idle" });
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

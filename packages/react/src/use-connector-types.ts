import type { ConnectorDescriptor, LakeSyncError } from "@lakesync/core";
import { useCallback, useEffect, useState } from "react";
import { useLakeSync } from "./context";

/** Return type of `useConnectorTypes`. */
export interface UseConnectorTypesResult {
	/** Array of available connector type descriptors. */
	types: ConnectorDescriptor[];
	/** Whether discovery is loading. */
	isLoading: boolean;
	/** Error from the last fetch, or null. */
	error: LakeSyncError | null;
	/** Manually re-fetch connector types. */
	refetch: () => void;
}

/**
 * Discover available connector types and their configuration schemas.
 *
 * Calls `coordinator.listConnectorTypes()` on mount and returns the
 * result reactively. Use this to build dynamic "Add Connector" forms.
 *
 * ```ts
 * const { types, isLoading } = useConnectorTypes();
 *
 * for (const t of types) {
 *   console.log(t.displayName, t.configSchema);
 * }
 * ```
 */
export function useConnectorTypes(): UseConnectorTypesResult {
	const { coordinator } = useLakeSync();
	const [types, setTypes] = useState<ConnectorDescriptor[]>([]);
	const [isLoading, setIsLoading] = useState(true);
	const [error, setError] = useState<LakeSyncError | null>(null);
	const [trigger, setTrigger] = useState(0);

	const refetch = useCallback(() => {
		setTrigger((v) => v + 1);
	}, []);

	useEffect(() => {
		void trigger;
		let cancelled = false;

		const run = async () => {
			setIsLoading(true);
			const result = await coordinator.listConnectorTypes();
			if (cancelled) return;

			if (result.ok) {
				setTypes(result.value);
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
	}, [coordinator, trigger]);

	return { types, isLoading, error, refetch };
}

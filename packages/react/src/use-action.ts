import type {
	ActionDescriptor,
	ActionErrorResult,
	ActionResult,
	LakeSyncError,
} from "@lakesync/core";
import { useCallback, useEffect, useRef, useState } from "react";
import { useLakeSyncStable } from "./context";

/** Parameters for a single action execution. */
export interface ActionParams {
	connector: string;
	actionType: string;
	params: Record<string, unknown>;
	idempotencyKey?: string;
}

/** Return type of `useAction`. */
export interface UseActionResult {
	/** Execute an action against a connector via the gateway. */
	execute: (params: ActionParams) => Promise<void>;
	/** Last action result (success or error). Null before first execution. */
	lastResult: ActionResult | ActionErrorResult | null;
	/** Whether an action is currently in flight. */
	isPending: boolean;
}

/**
 * Execute imperative actions against external systems via the gateway.
 *
 * Wraps `SyncCoordinator.executeAction()` and subscribes to
 * `onActionComplete` events to track the latest result.
 *
 * Uses a `pendingRef` to track whether we are waiting for a completion.
 * When `execute()` is called, we set the ref to `true` so the next
 * `onActionComplete` event is captured. This avoids the identity bug
 * where a stale action completion from a different hook instance would
 * overwrite state.
 *
 * ```ts
 * const { execute, lastResult, isPending } = useAction();
 *
 * await execute({
 *   connector: "slack",
 *   actionType: "send_message",
 *   params: { channel: "#general", text: "Hello" },
 * });
 * ```
 */
export function useAction(): UseActionResult {
	const { coordinator } = useLakeSyncStable();
	const [lastResult, setLastResult] = useState<ActionResult | ActionErrorResult | null>(null);
	const [isPending, setIsPending] = useState(false);
	const waitingForCompletion = useRef(false);

	useEffect(() => {
		const handleComplete = (_actionId: string, result: ActionResult | ActionErrorResult) => {
			if (waitingForCompletion.current) {
				waitingForCompletion.current = false;
				setLastResult(result);
				setIsPending(false);
			}
		};

		coordinator.on("onActionComplete", handleComplete);
		return () => {
			coordinator.off("onActionComplete", handleComplete);
		};
	}, [coordinator]);

	const execute = useCallback(
		async (params: ActionParams): Promise<void> => {
			setIsPending(true);
			setLastResult(null);
			waitingForCompletion.current = true;
			await coordinator.executeAction(params);
		},
		[coordinator],
	);

	return { execute, lastResult, isPending };
}

/** Return type of `useActionDiscovery`. */
export interface UseActionDiscoveryResult {
	/** Map of connector name to supported action descriptors. */
	connectors: Record<string, ActionDescriptor[]>;
	/** Whether discovery is loading. */
	isLoading: boolean;
	/** Error from the last fetch, or null. */
	error: LakeSyncError | null;
	/** Manually re-fetch available actions. */
	refetch: () => void;
}

/**
 * Discover available connectors and their supported action types.
 *
 * Calls `transport.describeActions()` on mount and returns the result
 * reactively. Use this to build dynamic UI based on available actions.
 *
 * ```ts
 * const { connectors, isLoading } = useActionDiscovery();
 *
 * for (const [name, actions] of Object.entries(connectors)) {
 *   console.log(name, actions.map(a => a.actionType));
 * }
 * ```
 */
export function useActionDiscovery(): UseActionDiscoveryResult {
	const { coordinator } = useLakeSyncStable();
	const [connectors, setConnectors] = useState<Record<string, ActionDescriptor[]>>({});
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
			const result = await coordinator.describeActions();
			if (cancelled) return;

			if (result.ok) {
				setConnectors(result.value.connectors);
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

	return { connectors, isLoading, error, refetch };
}

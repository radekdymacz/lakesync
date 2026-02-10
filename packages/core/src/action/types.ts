import type { ActionDescriptor } from "../connector/action-handler";
import type { HLCTimestamp } from "../hlc/types";

/** Discovery response listing available connectors and their supported actions. */
export interface ActionDiscovery {
	/** Map of connector name to its supported action descriptors. */
	connectors: Record<string, ActionDescriptor[]>;
}

/** An imperative action to execute against an external system. */
export interface Action {
	/** Unique action identifier (deterministic SHA-256 hash). */
	actionId: string;
	/** Client that initiated the action. */
	clientId: string;
	/** HLC timestamp when the action was created. */
	hlc: HLCTimestamp;
	/** Target connector name (e.g. "github", "slack", "linear"). */
	connector: string;
	/** Action type within the connector (e.g. "create_pr", "send_message"). */
	actionType: string;
	/** Action parameters — connector-specific payload. */
	params: Record<string, unknown>;
	/** Optional idempotency key for at-most-once delivery. */
	idempotencyKey?: string;
}

/** Successful result of executing an action. */
export interface ActionResult {
	/** The action that was executed. */
	actionId: string;
	/** Result data returned by the connector. */
	data: Record<string, unknown>;
	/** Server HLC after processing. */
	serverHlc: HLCTimestamp;
}

/** Error result of executing an action. */
export interface ActionErrorResult {
	/** The action that failed. */
	actionId: string;
	/** Error code. */
	code: string;
	/** Human-readable error message. */
	message: string;
	/** Whether the client can retry this action. */
	retryable: boolean;
}

/** Batch of actions pushed by a client. */
export interface ActionPush {
	/** Client identifier. */
	clientId: string;
	/** Actions to execute. */
	actions: Action[];
}

/** Gateway response to an action push. */
export interface ActionResponse {
	/** Results for each action (success or error). */
	results: Array<ActionResult | ActionErrorResult>;
	/** Server HLC after processing. */
	serverHlc: HLCTimestamp;
}

/** Type guard: check whether a result is an error. */
export function isActionError(
	result: ActionResult | ActionErrorResult,
): result is ActionErrorResult {
	return "code" in result && "retryable" in result;
}

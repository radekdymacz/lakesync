import type { ActionExecutionError, ActionNotSupportedError } from "../action/errors";
import type { Action, ActionResult } from "../action/types";
import type { Result } from "../result/result";
import type { ResolvedClaims } from "../sync-rules/types";

/** Describes an action type supported by a connector. */
export interface ActionDescriptor {
	/** The action type identifier (e.g. "create_pr", "send_message"). */
	actionType: string;
	/** Human-readable description of what this action does. */
	description: string;
	/** Optional JSON Schema for the action's params. */
	paramsSchema?: Record<string, unknown>;
}

/** Authentication context passed to action handlers for permission checks. */
export interface AuthContext {
	/** Resolved JWT claims for resource-level permission checks. */
	claims: ResolvedClaims;
}

/**
 * Interface for connectors that can execute imperative actions.
 *
 * Separate from `DatabaseAdapter` — not all connectors support actions
 * (e.g. S3 doesn't). A connector can implement `DatabaseAdapter` (read/write
 * data), `ActionHandler` (execute commands), or both.
 */
export interface ActionHandler {
	/** Descriptors for all action types this handler supports. */
	readonly supportedActions: ActionDescriptor[];
	/** Execute a single action against the external system. */
	executeAction(
		action: Action,
		context?: AuthContext,
	): Promise<Result<ActionResult, ActionExecutionError | ActionNotSupportedError>>;
}

/** Type guard: check whether an object implements the ActionHandler interface. */
export function isActionHandler(obj: unknown): obj is ActionHandler {
	if (obj === null || typeof obj !== "object") return false;
	const candidate = obj as Record<string, unknown>;
	return Array.isArray(candidate.supportedActions) && typeof candidate.executeAction === "function";
}

import { Err, Ok, type Result } from "../result/result";
import { ActionValidationError } from "./errors";
import type { Action } from "./types";

/**
 * Validate the structural integrity of an Action.
 *
 * Checks that all required fields are present and of the correct type.
 * Returns a `Result` so callers never need to catch.
 */
export function validateAction(action: unknown): Result<Action, ActionValidationError> {
	if (action === null || typeof action !== "object") {
		return Err(new ActionValidationError("Action must be a non-null object"));
	}

	const a = action as Record<string, unknown>;

	if (typeof a.actionId !== "string" || a.actionId.length === 0) {
		return Err(new ActionValidationError("actionId must be a non-empty string"));
	}

	if (typeof a.clientId !== "string" || a.clientId.length === 0) {
		return Err(new ActionValidationError("clientId must be a non-empty string"));
	}

	if (typeof a.hlc !== "bigint") {
		return Err(new ActionValidationError("hlc must be a bigint"));
	}

	if (typeof a.connector !== "string" || a.connector.length === 0) {
		return Err(new ActionValidationError("connector must be a non-empty string"));
	}

	if (typeof a.actionType !== "string" || a.actionType.length === 0) {
		return Err(new ActionValidationError("actionType must be a non-empty string"));
	}

	if (a.params === null || typeof a.params !== "object" || Array.isArray(a.params)) {
		return Err(new ActionValidationError("params must be a non-null object"));
	}

	if (a.idempotencyKey !== undefined && typeof a.idempotencyKey !== "string") {
		return Err(new ActionValidationError("idempotencyKey must be a string if provided"));
	}

	return Ok(action as Action);
}

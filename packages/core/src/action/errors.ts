import { LakeSyncError } from "../result/errors";

/** Error during action execution (may be retryable). */
export class ActionExecutionError extends LakeSyncError {
	readonly retryable: boolean;

	constructor(message: string, retryable: boolean, cause?: Error) {
		super(message, "ACTION_EXECUTION_ERROR", cause);
		this.retryable = retryable;
	}
}

/** The requested action type is not supported by the connector. */
export class ActionNotSupportedError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "ACTION_NOT_SUPPORTED", cause);
	}
}

/** Action payload failed structural validation. */
export class ActionValidationError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "ACTION_VALIDATION_ERROR", cause);
	}
}

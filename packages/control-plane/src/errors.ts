import { Err, LakeSyncError, Ok, type Result } from "@lakesync/core";

/** Error code for control plane operations */
export type ControlPlaneErrorCode =
	| "NOT_FOUND"
	| "DUPLICATE"
	| "QUOTA_EXCEEDED"
	| "INVALID_INPUT"
	| "INTERNAL";

/** Error type for all control plane operations */
export class ControlPlaneError extends LakeSyncError {
	override readonly code: ControlPlaneErrorCode;

	constructor(message: string, code: ControlPlaneErrorCode, cause?: Error) {
		super(message, code, cause);
		this.code = code;
	}
}

/** Coerce an unknown thrown value into an Error instance. */
function toCause(error: unknown): Error | undefined {
	return error instanceof Error ? error : undefined;
}

/** Execute an async operation and wrap errors into a ControlPlaneError Result. */
export async function wrapControlPlane<T>(
	operation: () => Promise<T>,
	errorMessage: string,
	code: ControlPlaneErrorCode = "INTERNAL",
): Promise<Result<T, ControlPlaneError>> {
	try {
		const value = await operation();
		return Ok(value);
	} catch (error) {
		if (error instanceof ControlPlaneError) {
			return Err(error);
		}
		return Err(new ControlPlaneError(errorMessage, code, toCause(error)));
	}
}

/** Base error class for all LakeSync errors */
export class LakeSyncError extends Error {
	readonly code: string;
	override readonly cause?: Error;

	constructor(message: string, code: string, cause?: Error) {
		super(message);
		this.name = this.constructor.name;
		this.code = code;
		this.cause = cause;
	}
}

/** Clock drift exceeds maximum allowed threshold */
export class ClockDriftError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "CLOCK_DRIFT", cause);
	}
}

/** Conflict resolution failure */
export class ConflictError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "CONFLICT", cause);
	}
}

/** Flush operation failure */
export class FlushError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "FLUSH_FAILED", cause);
	}
}

/** Schema mismatch or validation failure */
export class SchemaError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "SCHEMA_MISMATCH", cause);
	}
}

/** Lake adapter operation failure */
export class AdapterError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "ADAPTER_ERROR", cause);
	}
}

/** Named source adapter not found in gateway configuration */
export class AdapterNotFoundError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "ADAPTER_NOT_FOUND", cause);
	}
}

/** Buffer backpressure limit exceeded — push rejected to prevent OOM. */
export class BackpressureError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "BACKPRESSURE", cause);
	}
}

/** Flush queue publish failure */
export class FlushQueueError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "FLUSH_QUEUE_ERROR", cause);
	}
}

/** Structured error codes for API responses. */
export const API_ERROR_CODES = {
	VALIDATION_ERROR: "VALIDATION_ERROR",
	SCHEMA_ERROR: "SCHEMA_ERROR",
	BACKPRESSURE_ERROR: "BACKPRESSURE_ERROR",
	CLOCK_DRIFT: "CLOCK_DRIFT",
	AUTH_ERROR: "AUTH_ERROR",
	FORBIDDEN: "FORBIDDEN",
	NOT_FOUND: "NOT_FOUND",
	RATE_LIMITED: "RATE_LIMITED",
	ADAPTER_ERROR: "ADAPTER_ERROR",
	FLUSH_ERROR: "FLUSH_ERROR",
	INTERNAL_ERROR: "INTERNAL_ERROR",
} as const;

/** A single error code value from {@link API_ERROR_CODES}. */
export type ApiErrorCode = (typeof API_ERROR_CODES)[keyof typeof API_ERROR_CODES];

/** Coerce an unknown thrown value into an Error instance. */
export function toError(err: unknown): Error {
	return err instanceof Error ? err : new Error(String(err));
}

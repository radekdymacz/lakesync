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

/** Coerce an unknown thrown value into an Error instance. */
export function toError(err: unknown): Error {
	return err instanceof Error ? err : new Error(String(err));
}

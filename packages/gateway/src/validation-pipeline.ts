import { Ok, type Result, type RowDelta, type SchemaError } from "@lakesync/core";

/**
 * A single validation step for an incoming delta.
 *
 * Returns `Ok` when the delta is valid, or `Err(SchemaError)` when it fails.
 * Validators MUST be side-effect-free — they inspect but never mutate.
 */
export type DeltaValidator = (delta: RowDelta) => Result<void, SchemaError>;

/**
 * Composable pipeline of delta validators.
 *
 * Runs each registered validator in order against a delta. Stops at the
 * first failure and returns its error. The pipeline is side-effect-free —
 * safe to run as a pre-check before any buffer mutation.
 */
export class ValidationPipeline {
	private readonly validators: DeltaValidator[] = [];

	/** Append a validator to the pipeline. */
	add(validator: DeltaValidator): void {
		this.validators.push(validator);
	}

	/** Run all validators against a single delta. */
	validate(delta: RowDelta): Result<void, SchemaError> {
		for (const validator of this.validators) {
			const result = validator(delta);
			if (!result.ok) return result;
		}
		return Ok(undefined);
	}
}

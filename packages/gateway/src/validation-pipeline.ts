import { Ok, type Result, type RowDelta, type SchemaError } from "@lakesync/core";

/**
 * A single validation step for an incoming delta.
 *
 * Returns `Ok` when the delta is valid, or `Err(SchemaError)` when it fails.
 * Validators MUST be side-effect-free — they inspect but never mutate.
 */
export type DeltaValidator = (delta: RowDelta) => Result<void, SchemaError>;

/**
 * Compose multiple delta validators into a single validator.
 *
 * Runs each validator in order against a delta. Stops at the
 * first failure and returns its error. The composed validator is
 * side-effect-free — safe to run as a pre-check before any buffer mutation.
 */
export function composePipeline(...validators: DeltaValidator[]): DeltaValidator {
	return (delta: RowDelta): Result<void, SchemaError> => {
		for (const validator of validators) {
			const result = validator(delta);
			if (!result.ok) return result;
		}
		return Ok(undefined);
	};
}

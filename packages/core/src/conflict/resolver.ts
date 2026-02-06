import type { RowDelta } from "../delta/types";
import type { ConflictError } from "../result/errors";
import type { Result } from "../result/result";

/** Strategy for resolving conflicting row deltas */
export interface ConflictResolver {
	/** Resolve two conflicting deltas for the same row, returning the merged result */
	resolve(local: RowDelta, remote: RowDelta): Result<RowDelta, ConflictError>;
}

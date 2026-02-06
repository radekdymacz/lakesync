import type { RowDelta } from '../delta/types';
import type { Result } from '../result/result';
import type { ConflictError } from '../result/errors';

/** Strategy for resolving conflicting row deltas */
export interface ConflictResolver {
	/** Resolve two conflicting deltas for the same row, returning the merged result */
	resolve(local: RowDelta, remote: RowDelta): Result<RowDelta, ConflictError>;
}

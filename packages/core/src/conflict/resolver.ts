import type { RowDelta } from "../delta/types";
import type { ConflictError } from "../result/errors";
import type { Result } from "../result/result";

/** Strategy for resolving conflicting row deltas */
export type ConflictResolver = (
	local: RowDelta,
	remote: RowDelta,
) => Result<RowDelta, ConflictError>;

import type { RowDelta } from "../delta/types";
import { Err, Ok, type Result } from "../result/result";
import { SyncRuleError } from "./errors";
import type {
	BucketDefinition,
	ResolvedClaims,
	SyncRuleFilter,
	SyncRulesConfig,
	SyncRulesContext,
} from "./types";

/**
 * Resolve a filter value, substituting JWT claim references.
 *
 * Values prefixed with `jwt:` are looked up in the claims record.
 * Literal values are returned as-is (wrapped in an array for uniform handling).
 *
 * @param value - The filter value string (e.g. "jwt:sub" or "tenant-1")
 * @param claims - Resolved JWT claims
 * @returns An array of resolved values, or an empty array if the claim is missing
 */
export function resolveFilterValue(value: string, claims: ResolvedClaims): string[] {
	if (!value.startsWith("jwt:")) {
		return [value];
	}

	const claimKey = value.slice(4);
	const claimValue = claims[claimKey];

	if (claimValue === undefined) {
		return [];
	}

	return Array.isArray(claimValue) ? claimValue : [claimValue];
}

/**
 * Check whether a delta matches a single bucket definition.
 *
 * A delta matches if:
 * 1. The bucket's `tables` list is empty (matches all tables) or includes the delta's table
 * 2. All filters match (conjunctive AND):
 *    - `eq`: the delta column value equals one of the resolved filter values
 *    - `in`: the delta column value is contained in the resolved filter values
 *
 * @param delta - The row delta to evaluate
 * @param bucket - The bucket definition
 * @param claims - Resolved JWT claims
 * @returns true if the delta matches this bucket
 */
export function deltaMatchesBucket(
	delta: RowDelta,
	bucket: BucketDefinition,
	claims: ResolvedClaims,
): boolean {
	// Table filter: empty tables list = match all
	if (bucket.tables.length > 0 && !bucket.tables.includes(delta.table)) {
		return false;
	}

	// All filters must match (conjunctive AND)
	for (const filter of bucket.filters) {
		if (!filterMatchesDelta(delta, filter, claims)) {
			return false;
		}
	}

	return true;
}

/**
 * Compare two values using a comparison operator.
 * Attempts numeric comparison first; falls back to string localeCompare.
 */
function compareValues(
	deltaValue: string,
	filterValue: string,
	op: "gt" | "lt" | "gte" | "lte",
): boolean {
	const numDelta = parseFloat(deltaValue);
	const numFilter = parseFloat(filterValue);
	const useNumeric = !Number.isNaN(numDelta) && !Number.isNaN(numFilter);

	if (useNumeric) {
		switch (op) {
			case "gt":
				return numDelta > numFilter;
			case "lt":
				return numDelta < numFilter;
			case "gte":
				return numDelta >= numFilter;
			case "lte":
				return numDelta <= numFilter;
		}
	}

	const cmp = deltaValue.localeCompare(filterValue);
	switch (op) {
		case "gt":
			return cmp > 0;
		case "lt":
			return cmp < 0;
		case "gte":
			return cmp >= 0;
		case "lte":
			return cmp <= 0;
	}
}

const FILTER_OPS: Record<string, (dv: string, rv: string[]) => boolean> = {
	eq: (dv, rv) => rv.includes(dv),
	in: (dv, rv) => rv.includes(dv),
	neq: (dv, rv) => !rv.includes(dv),
	gt: (dv, rv) => compareValues(dv, rv[0]!, "gt"),
	lt: (dv, rv) => compareValues(dv, rv[0]!, "lt"),
	gte: (dv, rv) => compareValues(dv, rv[0]!, "gte"),
	lte: (dv, rv) => compareValues(dv, rv[0]!, "lte"),
};

/**
 * Check whether a single filter matches a delta's column values.
 */
function filterMatchesDelta(
	delta: RowDelta,
	filter: SyncRuleFilter,
	claims: ResolvedClaims,
): boolean {
	const col = delta.columns.find((c) => c.column === filter.column);
	if (!col) {
		// Column not present in delta — filter does not match
		return false;
	}

	const deltaValue = String(col.value);
	const resolvedValues = resolveFilterValue(filter.value, claims);

	if (resolvedValues.length === 0) {
		// JWT claim missing — filter cannot match
		return false;
	}

	return FILTER_OPS[filter.op]?.(deltaValue, resolvedValues) ?? false;
}

/**
 * Filter an array of deltas by sync rules.
 *
 * A delta is included if it matches **any** bucket (union across buckets).
 * If no sync rules are configured (empty buckets), all deltas pass through.
 *
 * @param deltas - The deltas to filter
 * @param context - Sync rules context (rules + resolved claims)
 * @returns Filtered array of deltas
 */
export function filterDeltas(deltas: RowDelta[], context: SyncRulesContext): RowDelta[] {
	if (context.rules.buckets.length === 0) {
		return deltas;
	}

	return deltas.filter((delta) =>
		context.rules.buckets.some((bucket) => deltaMatchesBucket(delta, bucket, context.claims)),
	);
}

/**
 * Determine which buckets a client matches based on their claims.
 *
 * A client matches a bucket if the bucket has no table-level restrictions
 * or if the client's claims satisfy all filter conditions for at least
 * one possible row. This is used for bucket-level access decisions, not
 * row-level filtering.
 *
 * @param rules - The sync rules configuration
 * @param claims - Resolved JWT claims
 * @returns Array of bucket names the client matches
 */
export function resolveClientBuckets(rules: SyncRulesConfig, claims: ResolvedClaims): string[] {
	return rules.buckets
		.filter((bucket) => {
			// A client matches a bucket if all JWT-referenced filters
			// can be resolved (i.e. the required claims exist)
			for (const filter of bucket.filters) {
				if (filter.value.startsWith("jwt:")) {
					const resolved = resolveFilterValue(filter.value, claims);
					if (resolved.length === 0) {
						return false;
					}
				}
			}
			return true;
		})
		.map((b) => b.name);
}

/**
 * Validate a sync rules configuration for structural correctness.
 *
 * Checks:
 * - Version is a positive integer
 * - Buckets is an array
 * - Each bucket has a non-empty name, valid tables array, valid filters
 * - Filter operators are "eq" or "in"
 * - Filter values and columns are non-empty strings
 * - Bucket names are unique
 *
 * @param config - The sync rules configuration to validate
 * @returns Ok(void) if valid, Err(SyncRuleError) with details if invalid
 */
export function validateSyncRules(config: unknown): Result<void, SyncRuleError> {
	if (typeof config !== "object" || config === null) {
		return Err(new SyncRuleError("Sync rules config must be an object"));
	}

	const obj = config as Record<string, unknown>;

	if (typeof obj.version !== "number" || !Number.isInteger(obj.version) || obj.version < 1) {
		return Err(new SyncRuleError("Sync rules version must be a positive integer"));
	}

	if (!Array.isArray(obj.buckets)) {
		return Err(new SyncRuleError("Sync rules buckets must be an array"));
	}

	const seenNames = new Set<string>();

	for (let i = 0; i < obj.buckets.length; i++) {
		const bucket = obj.buckets[i] as Record<string, unknown>;

		if (typeof bucket !== "object" || bucket === null) {
			return Err(new SyncRuleError(`Bucket at index ${i} must be an object`));
		}

		if (typeof bucket.name !== "string" || bucket.name.length === 0) {
			return Err(new SyncRuleError(`Bucket at index ${i} must have a non-empty name`));
		}

		if (seenNames.has(bucket.name as string)) {
			return Err(new SyncRuleError(`Duplicate bucket name: "${bucket.name}"`));
		}
		seenNames.add(bucket.name as string);

		if (!Array.isArray(bucket.tables)) {
			return Err(new SyncRuleError(`Bucket "${bucket.name}" tables must be an array`));
		}

		for (const table of bucket.tables as unknown[]) {
			if (typeof table !== "string" || table.length === 0) {
				return Err(
					new SyncRuleError(`Bucket "${bucket.name}" tables must contain non-empty strings`),
				);
			}
		}

		if (!Array.isArray(bucket.filters)) {
			return Err(new SyncRuleError(`Bucket "${bucket.name}" filters must be an array`));
		}

		for (let j = 0; j < (bucket.filters as unknown[]).length; j++) {
			const filter = (bucket.filters as Record<string, unknown>[])[j]!;

			if (typeof filter !== "object" || filter === null) {
				return Err(
					new SyncRuleError(`Bucket "${bucket.name}" filter at index ${j} must be an object`),
				);
			}

			if (typeof filter.column !== "string" || (filter.column as string).length === 0) {
				return Err(
					new SyncRuleError(
						`Bucket "${bucket.name}" filter at index ${j} must have a non-empty column`,
					),
				);
			}

			const validOps = ["eq", "in", "neq", "gt", "lt", "gte", "lte"];
			if (!validOps.includes(filter.op as string)) {
				return Err(
					new SyncRuleError(
						`Bucket "${bucket.name}" filter at index ${j} op must be one of: ${validOps.join(", ")}`,
					),
				);
			}

			if (typeof filter.value !== "string" || (filter.value as string).length === 0) {
				return Err(
					new SyncRuleError(
						`Bucket "${bucket.name}" filter at index ${j} must have a non-empty value`,
					),
				);
			}
		}
	}

	return Ok(undefined);
}

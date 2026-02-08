/** Supported comparison operators for sync rule filters */
export type SyncRuleOp = "eq" | "in" | "neq" | "gt" | "lt" | "gte" | "lte";

/** A single filter condition within a bucket definition */
export interface SyncRuleFilter {
	/** Column name to match against */
	column: string;
	/** Comparison operator */
	op: SyncRuleOp;
	/** Literal value or JWT claim reference (prefixed with "jwt:") */
	value: string;
}

/** A named bucket defining which rows a client should receive */
export interface BucketDefinition {
	/** Unique bucket name */
	name: string;
	/** Tables this bucket applies to. Empty array = all tables. */
	tables: string[];
	/** Row-level filter conditions (conjunctive — all must match). Empty = no row filtering. */
	filters: SyncRuleFilter[];
}

/** Top-level sync rules configuration */
export interface SyncRulesConfig {
	/** Configuration version (for future schema evolution) */
	version: number;
	/** Bucket definitions */
	buckets: BucketDefinition[];
}

/** Resolved JWT claims as a flat record */
export type ResolvedClaims = Record<string, string | string[]>;

/** Context for evaluating sync rules against a specific client */
export interface SyncRulesContext {
	/** Resolved JWT claims for the requesting client */
	claims: ResolvedClaims;
	/** The sync rules configuration to evaluate */
	rules: SyncRulesConfig;
}

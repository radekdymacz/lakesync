import { describe, expect, it } from "vitest";
import type { RowDelta } from "../../delta/types";
import { HLC } from "../../hlc/hlc";
import type { HLCTimestamp } from "../../hlc/types";
import {
	deltaMatchesBucket,
	filterDeltas,
	resolveClientBuckets,
	resolveFilterValue,
	validateSyncRules,
} from "../evaluator";
import type { BucketDefinition, ResolvedClaims, SyncRulesConfig, SyncRulesContext } from "../types";

function makeDelta(overrides: Partial<RowDelta> & { hlc: HLCTimestamp }): RowDelta {
	return {
		op: "INSERT",
		table: "todos",
		rowId: "row-1",
		clientId: "client-a",
		columns: [],
		deltaId: `delta-${Math.random().toString(36).slice(2)}`,
		...overrides,
	};
}

const hlc1 = HLC.encode(1_000_000, 0);
const hlc2 = HLC.encode(2_000_000, 0);
const hlc3 = HLC.encode(3_000_000, 0);

// ---------------------------------------------------------------------------
// resolveFilterValue
// ---------------------------------------------------------------------------

describe("resolveFilterValue", () => {
	it("returns literal values as-is", () => {
		expect(resolveFilterValue("tenant-1", {})).toEqual(["tenant-1"]);
	});

	it("resolves jwt: string claims", () => {
		const claims: ResolvedClaims = { sub: "user-42" };
		expect(resolveFilterValue("jwt:sub", claims)).toEqual(["user-42"]);
	});

	it("resolves jwt: array claims", () => {
		const claims: ResolvedClaims = { roles: ["admin", "editor"] };
		expect(resolveFilterValue("jwt:roles", claims)).toEqual(["admin", "editor"]);
	});

	it("returns empty array for missing claims", () => {
		expect(resolveFilterValue("jwt:missing", {})).toEqual([]);
	});
});

// ---------------------------------------------------------------------------
// deltaMatchesBucket
// ---------------------------------------------------------------------------

describe("deltaMatchesBucket", () => {
	it("matches when bucket has no tables and no filters (catch-all)", () => {
		const bucket: BucketDefinition = {
			name: "all",
			tables: [],
			filters: [],
		};
		const delta = makeDelta({ hlc: hlc1 });
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(true);
	});

	it("matches when delta table is in bucket tables", () => {
		const bucket: BucketDefinition = {
			name: "todo-bucket",
			tables: ["todos"],
			filters: [],
		};
		const delta = makeDelta({ hlc: hlc1, table: "todos" });
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(true);
	});

	it("rejects when delta table is not in bucket tables", () => {
		const bucket: BucketDefinition = {
			name: "todo-bucket",
			tables: ["notes"],
			filters: [],
		};
		const delta = makeDelta({ hlc: hlc1, table: "todos" });
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(false);
	});

	it("matches eq filter against JWT claim", () => {
		const bucket: BucketDefinition = {
			name: "user-data",
			tables: [],
			filters: [{ column: "user_id", op: "eq", value: "jwt:sub" }],
		};
		const claims: ResolvedClaims = { sub: "user-42" };
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "user_id", value: "user-42" }],
		});
		expect(deltaMatchesBucket(delta, bucket, claims)).toBe(true);
	});

	it("rejects eq filter when claim does not match", () => {
		const bucket: BucketDefinition = {
			name: "user-data",
			tables: [],
			filters: [{ column: "user_id", op: "eq", value: "jwt:sub" }],
		};
		const claims: ResolvedClaims = { sub: "user-99" };
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "user_id", value: "user-42" }],
		});
		expect(deltaMatchesBucket(delta, bucket, claims)).toBe(false);
	});

	it("matches in filter against JWT array claim", () => {
		const bucket: BucketDefinition = {
			name: "org-data",
			tables: [],
			filters: [{ column: "org_id", op: "in", value: "jwt:orgs" }],
		};
		const claims: ResolvedClaims = { orgs: ["org-1", "org-2", "org-3"] };
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "org_id", value: "org-2" }],
		});
		expect(deltaMatchesBucket(delta, bucket, claims)).toBe(true);
	});

	it("matches eq filter with literal value", () => {
		const bucket: BucketDefinition = {
			name: "public",
			tables: [],
			filters: [{ column: "visibility", op: "eq", value: "public" }],
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "visibility", value: "public" }],
		});
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(true);
	});

	it("applies multiple filters as AND", () => {
		const bucket: BucketDefinition = {
			name: "scoped",
			tables: ["todos"],
			filters: [
				{ column: "user_id", op: "eq", value: "jwt:sub" },
				{ column: "org_id", op: "eq", value: "jwt:org_id" },
			],
		};
		const claims: ResolvedClaims = { sub: "user-1", org_id: "org-a" };

		// Both filters match
		const matching = makeDelta({
			hlc: hlc1,
			table: "todos",
			columns: [
				{ column: "user_id", value: "user-1" },
				{ column: "org_id", value: "org-a" },
			],
		});
		expect(deltaMatchesBucket(matching, bucket, claims)).toBe(true);

		// Only one filter matches
		const partial = makeDelta({
			hlc: hlc1,
			table: "todos",
			columns: [
				{ column: "user_id", value: "user-1" },
				{ column: "org_id", value: "org-b" },
			],
		});
		expect(deltaMatchesBucket(partial, bucket, claims)).toBe(false);
	});

	it("rejects when referenced column is missing from delta", () => {
		const bucket: BucketDefinition = {
			name: "user-data",
			tables: [],
			filters: [{ column: "user_id", op: "eq", value: "jwt:sub" }],
		};
		const claims: ResolvedClaims = { sub: "user-42" };
		const delta = makeDelta({ hlc: hlc1, columns: [] });
		expect(deltaMatchesBucket(delta, bucket, claims)).toBe(false);
	});

	it("rejects when jwt claim is missing", () => {
		const bucket: BucketDefinition = {
			name: "user-data",
			tables: [],
			filters: [{ column: "user_id", op: "eq", value: "jwt:sub" }],
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "user_id", value: "user-42" }],
		});
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// filterDeltas
// ---------------------------------------------------------------------------

describe("filterDeltas", () => {
	it("passes all deltas when no buckets are configured", () => {
		const context: SyncRulesContext = {
			claims: {},
			rules: { version: 1, buckets: [] },
		};
		const deltas = [makeDelta({ hlc: hlc1 }), makeDelta({ hlc: hlc2 })];
		expect(filterDeltas(deltas, context)).toHaveLength(2);
	});

	it("filters deltas by bucket union (multi-bucket)", () => {
		const context: SyncRulesContext = {
			claims: { sub: "user-1" },
			rules: {
				version: 1,
				buckets: [
					{
						name: "user-todos",
						tables: ["todos"],
						filters: [{ column: "user_id", op: "eq", value: "jwt:sub" }],
					},
					{
						name: "public-notes",
						tables: ["notes"],
						filters: [{ column: "visibility", op: "eq", value: "public" }],
					},
				],
			},
		};

		const deltas = [
			// Matches user-todos bucket
			makeDelta({
				hlc: hlc1,
				table: "todos",
				columns: [{ column: "user_id", value: "user-1" }],
			}),
			// Does not match any bucket (wrong user)
			makeDelta({
				hlc: hlc2,
				table: "todos",
				columns: [{ column: "user_id", value: "user-2" }],
			}),
			// Matches public-notes bucket
			makeDelta({
				hlc: hlc3,
				table: "notes",
				columns: [{ column: "visibility", value: "public" }],
			}),
		];

		const filtered = filterDeltas(deltas, context);
		expect(filtered).toHaveLength(2);
		expect(filtered[0]!.hlc).toBe(hlc1);
		expect(filtered[1]!.hlc).toBe(hlc3);
	});

	it("handles numeric column values via string coercion", () => {
		const context: SyncRulesContext = {
			claims: { org_id: "42" },
			rules: {
				version: 1,
				buckets: [
					{
						name: "org",
						tables: [],
						filters: [{ column: "org_id", op: "eq", value: "jwt:org_id" }],
					},
				],
			},
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "org_id", value: 42 }],
		});
		expect(filterDeltas([delta], context)).toHaveLength(1);
	});
});

// ---------------------------------------------------------------------------
// resolveClientBuckets
// ---------------------------------------------------------------------------

describe("resolveClientBuckets", () => {
	const rules: SyncRulesConfig = {
		version: 1,
		buckets: [
			{
				name: "user-data",
				tables: [],
				filters: [{ column: "user_id", op: "eq", value: "jwt:sub" }],
			},
			{
				name: "public",
				tables: [],
				filters: [],
			},
			{
				name: "org-data",
				tables: [],
				filters: [{ column: "org_id", op: "eq", value: "jwt:org_id" }],
			},
		],
	};

	it("returns buckets where all JWT claims resolve", () => {
		const claims: ResolvedClaims = { sub: "user-1" };
		const buckets = resolveClientBuckets(rules, claims);
		expect(buckets).toEqual(["user-data", "public"]);
	});

	it("returns all buckets when all claims present", () => {
		const claims: ResolvedClaims = { sub: "user-1", org_id: "org-a" };
		const buckets = resolveClientBuckets(rules, claims);
		expect(buckets).toEqual(["user-data", "public", "org-data"]);
	});

	it("returns only catch-all bucket when no claims", () => {
		const buckets = resolveClientBuckets(rules, {});
		expect(buckets).toEqual(["public"]);
	});
});

// ---------------------------------------------------------------------------
// validateSyncRules
// ---------------------------------------------------------------------------

describe("validateSyncRules", () => {
	it("accepts a valid config", () => {
		const result = validateSyncRules({
			version: 1,
			buckets: [
				{
					name: "user-data",
					tables: ["todos"],
					filters: [{ column: "user_id", op: "eq", value: "jwt:sub" }],
				},
			],
		});
		expect(result.ok).toBe(true);
	});

	it("accepts empty buckets", () => {
		const result = validateSyncRules({ version: 1, buckets: [] });
		expect(result.ok).toBe(true);
	});

	it("rejects non-object config", () => {
		const result = validateSyncRules("not an object");
		expect(result.ok).toBe(false);
	});

	it("rejects missing version", () => {
		const result = validateSyncRules({ buckets: [] });
		expect(result.ok).toBe(false);
	});

	it("rejects zero version", () => {
		const result = validateSyncRules({ version: 0, buckets: [] });
		expect(result.ok).toBe(false);
	});

	it("rejects missing buckets", () => {
		const result = validateSyncRules({ version: 1 });
		expect(result.ok).toBe(false);
	});

	it("rejects bucket without name", () => {
		const result = validateSyncRules({
			version: 1,
			buckets: [{ name: "", tables: [], filters: [] }],
		});
		expect(result.ok).toBe(false);
	});

	it("rejects duplicate bucket names", () => {
		const result = validateSyncRules({
			version: 1,
			buckets: [
				{ name: "a", tables: [], filters: [] },
				{ name: "a", tables: [], filters: [] },
			],
		});
		expect(result.ok).toBe(false);
	});

	it("rejects invalid filter operator", () => {
		const result = validateSyncRules({
			version: 1,
			buckets: [
				{
					name: "b",
					tables: [],
					filters: [{ column: "x", op: "like", value: "y" }],
				},
			],
		});
		expect(result.ok).toBe(false);
	});

	it("rejects filter with empty column", () => {
		const result = validateSyncRules({
			version: 1,
			buckets: [
				{
					name: "b",
					tables: [],
					filters: [{ column: "", op: "eq", value: "y" }],
				},
			],
		});
		expect(result.ok).toBe(false);
	});

	it("rejects filter with empty value", () => {
		const result = validateSyncRules({
			version: 1,
			buckets: [
				{
					name: "b",
					tables: [],
					filters: [{ column: "x", op: "eq", value: "" }],
				},
			],
		});
		expect(result.ok).toBe(false);
	});

	it("rejects empty table name in tables array", () => {
		const result = validateSyncRules({
			version: 1,
			buckets: [{ name: "b", tables: [""], filters: [] }],
		});
		expect(result.ok).toBe(false);
	});
});

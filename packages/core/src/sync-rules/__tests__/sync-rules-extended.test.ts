import { describe, expect, it } from "vitest";
import type { RowDelta } from "../../delta/types";
import { HLC } from "../../hlc/hlc";
import type { HLCTimestamp } from "../../hlc/types";
import { deltaMatchesBucket, filterDeltas, validateSyncRules } from "../evaluator";
import type { BucketDefinition, ResolvedClaims, SyncRulesContext } from "../types";

function makeDelta(overrides: Partial<RowDelta> & { hlc: HLCTimestamp }): RowDelta {
	return {
		op: "INSERT",
		table: "items",
		rowId: "row-1",
		clientId: "client-a",
		columns: [],
		deltaId: `delta-${Math.random().toString(36).slice(2)}`,
		...overrides,
	};
}

const hlc1 = HLC.encode(1_000_000, 0);

// ---------------------------------------------------------------------------
// neq operator
// ---------------------------------------------------------------------------

describe("neq operator", () => {
	it("matches when value does not equal filter value", () => {
		const bucket: BucketDefinition = {
			name: "not-draft",
			tables: [],
			filters: [{ column: "status", op: "neq", value: "draft" }],
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "status", value: "published" }],
		});
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(true);
	});

	it("rejects when value equals filter value", () => {
		const bucket: BucketDefinition = {
			name: "not-draft",
			tables: [],
			filters: [{ column: "status", op: "neq", value: "draft" }],
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "status", value: "draft" }],
		});
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(false);
	});

	it("works with JWT claims", () => {
		const bucket: BucketDefinition = {
			name: "other-users",
			tables: [],
			filters: [{ column: "user_id", op: "neq", value: "jwt:sub" }],
		};
		const claims: ResolvedClaims = { sub: "user-1" };

		const ownDelta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "user_id", value: "user-1" }],
		});
		expect(deltaMatchesBucket(ownDelta, bucket, claims)).toBe(false);

		const otherDelta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "user_id", value: "user-2" }],
		});
		expect(deltaMatchesBucket(otherDelta, bucket, claims)).toBe(true);
	});

	it("returns false when column is missing", () => {
		const bucket: BucketDefinition = {
			name: "not-draft",
			tables: [],
			filters: [{ column: "status", op: "neq", value: "draft" }],
		};
		const delta = makeDelta({ hlc: hlc1, columns: [] });
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// gt operator
// ---------------------------------------------------------------------------

describe("gt operator", () => {
	it("matches numeric values greater than threshold", () => {
		const bucket: BucketDefinition = {
			name: "high-priority",
			tables: [],
			filters: [{ column: "priority", op: "gt", value: "100" }],
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "priority", value: 150 }],
		});
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(true);
	});

	it("rejects numeric values equal to threshold", () => {
		const bucket: BucketDefinition = {
			name: "high-priority",
			tables: [],
			filters: [{ column: "priority", op: "gt", value: "100" }],
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "priority", value: 100 }],
		});
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(false);
	});

	it("rejects numeric values less than threshold", () => {
		const bucket: BucketDefinition = {
			name: "high-priority",
			tables: [],
			filters: [{ column: "priority", op: "gt", value: "100" }],
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "priority", value: 50 }],
		});
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(false);
	});

	it("compares strings via localeCompare", () => {
		const bucket: BucketDefinition = {
			name: "after-b",
			tables: [],
			filters: [{ column: "name", op: "gt", value: "b" }],
		};
		const deltaC = makeDelta({
			hlc: hlc1,
			columns: [{ column: "name", value: "c" }],
		});
		expect(deltaMatchesBucket(deltaC, bucket, {})).toBe(true);

		const deltaA = makeDelta({
			hlc: hlc1,
			columns: [{ column: "name", value: "a" }],
		});
		expect(deltaMatchesBucket(deltaA, bucket, {})).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// lt operator
// ---------------------------------------------------------------------------

describe("lt operator", () => {
	it("matches numeric values less than threshold", () => {
		const bucket: BucketDefinition = {
			name: "low-priority",
			tables: [],
			filters: [{ column: "priority", op: "lt", value: "50" }],
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "priority", value: 30 }],
		});
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(true);
	});

	it("rejects numeric values equal to threshold", () => {
		const bucket: BucketDefinition = {
			name: "low-priority",
			tables: [],
			filters: [{ column: "priority", op: "lt", value: "50" }],
		};
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "priority", value: 50 }],
		});
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(false);
	});

	it("compares strings via localeCompare", () => {
		const bucket: BucketDefinition = {
			name: "before-m",
			tables: [],
			filters: [{ column: "name", op: "lt", value: "m" }],
		};
		const deltaA = makeDelta({
			hlc: hlc1,
			columns: [{ column: "name", value: "a" }],
		});
		expect(deltaMatchesBucket(deltaA, bucket, {})).toBe(true);

		const deltaZ = makeDelta({
			hlc: hlc1,
			columns: [{ column: "name", value: "z" }],
		});
		expect(deltaMatchesBucket(deltaZ, bucket, {})).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// gte operator
// ---------------------------------------------------------------------------

describe("gte operator", () => {
	it("matches numeric values greater than or equal to threshold", () => {
		const bucket: BucketDefinition = {
			name: "gte-check",
			tables: [],
			filters: [{ column: "score", op: "gte", value: "80" }],
		};
		const deltaAbove = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 90 }],
		});
		expect(deltaMatchesBucket(deltaAbove, bucket, {})).toBe(true);

		const deltaEqual = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 80 }],
		});
		expect(deltaMatchesBucket(deltaEqual, bucket, {})).toBe(true);

		const deltaBelow = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 79 }],
		});
		expect(deltaMatchesBucket(deltaBelow, bucket, {})).toBe(false);
	});

	it("compares strings via localeCompare", () => {
		const bucket: BucketDefinition = {
			name: "gte-str",
			tables: [],
			filters: [{ column: "grade", op: "gte", value: "B" }],
		};
		const deltaB = makeDelta({
			hlc: hlc1,
			columns: [{ column: "grade", value: "B" }],
		});
		expect(deltaMatchesBucket(deltaB, bucket, {})).toBe(true);

		const deltaC = makeDelta({
			hlc: hlc1,
			columns: [{ column: "grade", value: "C" }],
		});
		expect(deltaMatchesBucket(deltaC, bucket, {})).toBe(true);

		const deltaA = makeDelta({
			hlc: hlc1,
			columns: [{ column: "grade", value: "A" }],
		});
		expect(deltaMatchesBucket(deltaA, bucket, {})).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// lte operator
// ---------------------------------------------------------------------------

describe("lte operator", () => {
	it("matches numeric values less than or equal to threshold", () => {
		const bucket: BucketDefinition = {
			name: "lte-check",
			tables: [],
			filters: [{ column: "score", op: "lte", value: "50" }],
		};
		const deltaBelow = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 30 }],
		});
		expect(deltaMatchesBucket(deltaBelow, bucket, {})).toBe(true);

		const deltaEqual = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 50 }],
		});
		expect(deltaMatchesBucket(deltaEqual, bucket, {})).toBe(true);

		const deltaAbove = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 51 }],
		});
		expect(deltaMatchesBucket(deltaAbove, bucket, {})).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// Mixed operators
// ---------------------------------------------------------------------------

describe("mixed operators in same bucket", () => {
	it("combines eq + gt as AND", () => {
		const bucket: BucketDefinition = {
			name: "active-high-priority",
			tables: ["tasks"],
			filters: [
				{ column: "status", op: "eq", value: "active" },
				{ column: "priority", op: "gt", value: "5" },
			],
		};

		const matching = makeDelta({
			hlc: hlc1,
			table: "tasks",
			columns: [
				{ column: "status", value: "active" },
				{ column: "priority", value: 10 },
			],
		});
		expect(deltaMatchesBucket(matching, bucket, {})).toBe(true);

		const wrongStatus = makeDelta({
			hlc: hlc1,
			table: "tasks",
			columns: [
				{ column: "status", value: "closed" },
				{ column: "priority", value: 10 },
			],
		});
		expect(deltaMatchesBucket(wrongStatus, bucket, {})).toBe(false);

		const lowPriority = makeDelta({
			hlc: hlc1,
			table: "tasks",
			columns: [
				{ column: "status", value: "active" },
				{ column: "priority", value: 3 },
			],
		});
		expect(deltaMatchesBucket(lowPriority, bucket, {})).toBe(false);
	});

	it("combines gte + lte for range query", () => {
		const bucket: BucketDefinition = {
			name: "mid-range",
			tables: [],
			filters: [
				{ column: "score", op: "gte", value: "10" },
				{ column: "score", op: "lte", value: "20" },
			],
		};

		const inside = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 15 }],
		});
		expect(deltaMatchesBucket(inside, bucket, {})).toBe(true);

		const atLowerBound = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 10 }],
		});
		expect(deltaMatchesBucket(atLowerBound, bucket, {})).toBe(true);

		const atUpperBound = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 20 }],
		});
		expect(deltaMatchesBucket(atUpperBound, bucket, {})).toBe(true);

		const below = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 5 }],
		});
		expect(deltaMatchesBucket(below, bucket, {})).toBe(false);

		const above = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: 25 }],
		});
		expect(deltaMatchesBucket(above, bucket, {})).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// filterDeltas with new operators
// ---------------------------------------------------------------------------

describe("filterDeltas with extended operators", () => {
	it("filters across buckets with new operators", () => {
		const context: SyncRulesContext = {
			claims: {},
			rules: {
				version: 1,
				buckets: [
					{
						name: "high-value",
						tables: ["orders"],
						filters: [{ column: "amount", op: "gt", value: "1000" }],
					},
					{
						name: "not-cancelled",
						tables: ["orders"],
						filters: [{ column: "status", op: "neq", value: "cancelled" }],
					},
				],
			},
		};

		const deltas = [
			makeDelta({
				hlc: hlc1,
				table: "orders",
				columns: [
					{ column: "amount", value: 1500 },
					{ column: "status", value: "active" },
				],
			}),
			makeDelta({
				hlc: hlc1,
				table: "orders",
				columns: [
					{ column: "amount", value: 500 },
					{ column: "status", value: "cancelled" },
				],
			}),
			makeDelta({
				hlc: hlc1,
				table: "orders",
				columns: [
					{ column: "amount", value: 200 },
					{ column: "status", value: "pending" },
				],
			}),
		];

		const filtered = filterDeltas(deltas, context);
		// First delta: matches both buckets (gt 1000 AND neq cancelled)
		// Second delta: matches neither (not gt 1000 AND is cancelled)
		// Third delta: matches not-cancelled bucket (neq cancelled)
		expect(filtered).toHaveLength(2);
	});
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

describe("comparison edge cases", () => {
	it("returns false when column is missing for comparison ops", () => {
		const bucket: BucketDefinition = {
			name: "gt-check",
			tables: [],
			filters: [{ column: "score", op: "gt", value: "50" }],
		};
		const delta = makeDelta({ hlc: hlc1, columns: [] });
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(false);
	});

	it("falls back to string comparison when value is not numeric", () => {
		const bucket: BucketDefinition = {
			name: "gt-str",
			tables: [],
			filters: [{ column: "label", op: "gt", value: "abc" }],
		};
		const deltaMatch = makeDelta({
			hlc: hlc1,
			columns: [{ column: "label", value: "xyz" }],
		});
		expect(deltaMatchesBucket(deltaMatch, bucket, {})).toBe(true);

		const deltaNoMatch = makeDelta({
			hlc: hlc1,
			columns: [{ column: "label", value: "aaa" }],
		});
		expect(deltaMatchesBucket(deltaNoMatch, bucket, {})).toBe(false);
	});

	it("falls back to string comparison when delta value is NaN", () => {
		const bucket: BucketDefinition = {
			name: "gt-nan",
			tables: [],
			filters: [{ column: "score", op: "gt", value: "50" }],
		};
		// "hello" is NaN as a number, so string comparison is used
		const delta = makeDelta({
			hlc: hlc1,
			columns: [{ column: "score", value: "hello" }],
		});
		// "hello" localeCompare "50" — 'h' > '5' in locale order
		expect(deltaMatchesBucket(delta, bucket, {})).toBe(true);
	});
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe("validateSyncRules with extended operators", () => {
	it("accepts all new operators", () => {
		for (const op of ["neq", "gt", "lt", "gte", "lte"]) {
			const result = validateSyncRules({
				version: 1,
				buckets: [
					{
						name: `bucket-${op}`,
						tables: [],
						filters: [{ column: "x", op, value: "1" }],
					},
				],
			});
			expect(result.ok, `expected op "${op}" to be valid`).toBe(true);
		}
	});

	it("still accepts eq and in", () => {
		for (const op of ["eq", "in"]) {
			const result = validateSyncRules({
				version: 1,
				buckets: [
					{
						name: `bucket-${op}`,
						tables: [],
						filters: [{ column: "x", op, value: "1" }],
					},
				],
			});
			expect(result.ok).toBe(true);
		}
	});

	it("rejects invalid operators", () => {
		const result = validateSyncRules({
			version: 1,
			buckets: [
				{
					name: "bad",
					tables: [],
					filters: [{ column: "x", op: "like", value: "y" }],
				},
			],
		});
		expect(result.ok).toBe(false);
	});

	it("rejects regex as operator", () => {
		const result = validateSyncRules({
			version: 1,
			buckets: [
				{
					name: "bad",
					tables: [],
					filters: [{ column: "x", op: "regex", value: ".*" }],
				},
			],
		});
		expect(result.ok).toBe(false);
	});
});

import { describe, expect, it } from "vitest";
import { createPassAllRules, createUserScopedRules } from "../defaults";
import { validateSyncRules } from "../evaluator";

describe("createPassAllRules", () => {
	it("returns a valid SyncRulesConfig", () => {
		const rules = createPassAllRules();
		expect(validateSyncRules(rules).ok).toBe(true);
	});

	it("has a single catch-all bucket with no filters", () => {
		const rules = createPassAllRules();
		expect(rules.version).toBe(1);
		expect(rules.buckets).toHaveLength(1);
		expect(rules.buckets[0]!.tables).toEqual([]);
		expect(rules.buckets[0]!.filters).toEqual([]);
	});
});

describe("createUserScopedRules", () => {
	it("returns a valid SyncRulesConfig", () => {
		const rules = createUserScopedRules(["todos"]);
		expect(validateSyncRules(rules).ok).toBe(true);
	});

	it("scopes to the specified tables with default user_id column", () => {
		const rules = createUserScopedRules(["todos", "notes"]);
		expect(rules.buckets).toHaveLength(1);

		const bucket = rules.buckets[0]!;
		expect(bucket.tables).toEqual(["todos", "notes"]);
		expect(bucket.filters).toEqual([{ column: "user_id", op: "eq", value: "jwt:sub" }]);
	});

	it("allows a custom user column", () => {
		const rules = createUserScopedRules(["items"], "owner_id");
		const filter = rules.buckets[0]!.filters[0]!;
		expect(filter.column).toBe("owner_id");
		expect(filter.value).toBe("jwt:sub");
	});

	it("applies to all tables when given an empty array", () => {
		const rules = createUserScopedRules([]);
		expect(rules.buckets[0]!.tables).toEqual([]);
	});
});

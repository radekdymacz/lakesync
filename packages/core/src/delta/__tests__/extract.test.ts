import { describe, expect, it } from "vitest";
import type { HLCTimestamp } from "../../hlc/types";
import { extractDelta } from "../extract";
import type { TableSchema } from "../types";

/** Helper to create a branded HLC timestamp for testing */
const hlc = BigInt(1_000_000) as HLCTimestamp;
const clientId = "client-1";
const table = "users";
const rowId = "row-1";

const baseOpts = { table, rowId, clientId, hlc } as const;

describe("extractDelta", () => {
	// Test 1: INSERT — all columns
	it("should produce an INSERT delta with all columns when before is null", async () => {
		const after = { name: "Alice", age: 30, active: true };
		const result = await extractDelta(null, after, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("INSERT");
		expect(result?.table).toBe(table);
		expect(result?.rowId).toBe(rowId);
		expect(result?.clientId).toBe(clientId);
		expect(result?.hlc).toBe(hlc);
		expect(result?.columns).toHaveLength(3);
		expect(result?.columns).toEqual(
			expect.arrayContaining([
				{ column: "name", value: "Alice" },
				{ column: "age", value: 30 },
				{ column: "active", value: true },
			]),
		);
		expect(result?.deltaId).toBeTypeOf("string");
		expect(result?.deltaId.length).toBe(64); // SHA-256 hex
	});

	// Test 1b: INSERT — before is undefined
	it("should produce an INSERT delta when before is undefined", async () => {
		const after = { name: "Bob" };
		const result = await extractDelta(undefined, after, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("INSERT");
		expect(result?.columns).toEqual([{ column: "name", value: "Bob" }]);
	});

	// Test 2: UPDATE single column — only that column
	it("should produce an UPDATE delta with only the changed column", async () => {
		const before = { name: "Alice", age: 30 };
		const after = { name: "Alice", age: 31 };
		const result = await extractDelta(before, after, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("UPDATE");
		expect(result?.columns).toHaveLength(1);
		expect(result?.columns[0]).toEqual({ column: "age", value: 31 });
	});

	// Test 3: UPDATE multiple — only changed columns
	it("should produce an UPDATE delta with multiple changed columns", async () => {
		const before = { name: "Alice", age: 30, city: "London" };
		const after = { name: "Alice", age: 31, city: "Manchester" };
		const result = await extractDelta(before, after, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("UPDATE");
		expect(result?.columns).toHaveLength(2);
		expect(result?.columns).toEqual(
			expect.arrayContaining([
				{ column: "age", value: 31 },
				{ column: "city", value: "Manchester" },
			]),
		);
	});

	// Test 4: DELETE — empty columns, op is DELETE
	it("should produce a DELETE delta with empty columns when after is null", async () => {
		const before = { name: "Alice", age: 30 };
		const result = await extractDelta(before, null, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("DELETE");
		expect(result?.columns).toHaveLength(0);
		expect(result?.deltaId).toBeTypeOf("string");
	});

	// Test 4b: DELETE — after is undefined
	it("should produce a DELETE delta when after is undefined", async () => {
		const before = { name: "Alice" };
		const result = await extractDelta(before, undefined, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("DELETE");
		expect(result?.columns).toHaveLength(0);
	});

	// Test 5: No-op — returns null
	it("should return null when nothing has changed", async () => {
		const before = { name: "Alice", age: 30 };
		const after = { name: "Alice", age: 30 };
		const result = await extractDelta(before, after, baseOpts);

		expect(result).toBeNull();
	});

	// Test 5b: Both null — returns null
	it("should return null when both before and after are null", async () => {
		const result = await extractDelta(null, null, baseOpts);
		expect(result).toBeNull();
	});

	// Test 9: null values handled (null is a valid change)
	it("should handle null as a valid column value", async () => {
		const before = { name: "Alice", age: 30 };
		const after = { name: "Alice", age: null };
		const result = await extractDelta(before, after, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("UPDATE");
		expect(result?.columns).toHaveLength(1);
		expect(result?.columns[0]).toEqual({ column: "age", value: null });
	});

	// Test 9b: null to non-null is also a valid change
	it("should detect change from null to a value", async () => {
		const before = { name: "Alice", age: null };
		const after = { name: "Alice", age: 30 };
		const result = await extractDelta(before, after, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("UPDATE");
		expect(result?.columns).toHaveLength(1);
		expect(result?.columns[0]).toEqual({ column: "age", value: 30 });
	});

	// Test 10: Nested objects with different key order — no false delta
	it("should not produce a false delta for nested objects with different key order", async () => {
		const before = { name: "Alice", meta: { city: "London", country: "UK" } };
		const after = { name: "Alice", meta: { country: "UK", city: "London" } };
		const result = await extractDelta(before, after, baseOpts);

		expect(result).toBeNull();
	});

	// Test 11: Nested objects with actual value change — correct delta
	it("should detect actual changes in nested objects", async () => {
		const before = { name: "Alice", meta: { city: "London", country: "UK" } };
		const after = { name: "Alice", meta: { city: "Manchester", country: "UK" } };
		const result = await extractDelta(before, after, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("UPDATE");
		expect(result?.columns).toHaveLength(1);
		expect(result?.columns[0]?.column).toBe("meta");
		expect(result?.columns[0]?.value).toEqual({
			city: "Manchester",
			country: "UK",
		});
	});

	// Test: Schema filtering — only considers columns in the schema
	it("should only consider columns listed in the schema", async () => {
		const schema: TableSchema = {
			table: "users",
			columns: [
				{ name: "name", type: "string" },
				{ name: "age", type: "number" },
			],
		};
		const before = { name: "Alice", age: 30, secret: "hidden" };
		const after = { name: "Alice", age: 31, secret: "changed" };
		const result = await extractDelta(before, after, { ...baseOpts, schema });

		expect(result).not.toBeNull();
		expect(result?.op).toBe("UPDATE");
		expect(result?.columns).toHaveLength(1);
		expect(result?.columns[0]).toEqual({ column: "age", value: 31 });
	});

	// Test: INSERT with schema filtering
	it("should filter INSERT columns by schema", async () => {
		const schema: TableSchema = {
			table: "users",
			columns: [{ name: "name", type: "string" }],
		};
		const after = { name: "Alice", age: 30, secret: "hidden" };
		const result = await extractDelta(null, after, { ...baseOpts, schema });

		expect(result).not.toBeNull();
		expect(result?.op).toBe("INSERT");
		expect(result?.columns).toHaveLength(1);
		expect(result?.columns[0]).toEqual({ column: "name", value: "Alice" });
	});

	// Test: undefined values are skipped (treated as absent)
	it("should skip columns with undefined values", async () => {
		const before = { name: "Alice", age: undefined };
		const after = { name: "Alice", age: undefined };
		const result = await extractDelta(before, after, baseOpts);

		expect(result).toBeNull();
	});

	// Test: Deterministic deltaId
	it("should produce the same deltaId for the same inputs", async () => {
		const before = { name: "Alice" };
		const after = { name: "Bob" };
		const result1 = await extractDelta(before, after, baseOpts);
		const result2 = await extractDelta(before, after, baseOpts);

		expect(result1).not.toBeNull();
		expect(result2).not.toBeNull();
		expect(result1?.deltaId).toBe(result2?.deltaId);
	});

	// Test: New column added in after
	it("should detect a newly added column", async () => {
		const before = { name: "Alice" };
		const after = { name: "Alice", age: 30 };
		const result = await extractDelta(before, after, baseOpts);

		expect(result).not.toBeNull();
		expect(result?.op).toBe("UPDATE");
		expect(result?.columns).toHaveLength(1);
		expect(result?.columns[0]).toEqual({ column: "age", value: 30 });
	});
});

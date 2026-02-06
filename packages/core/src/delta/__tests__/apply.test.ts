import { describe, expect, it } from "vitest";
import type { HLCTimestamp } from "../../hlc/types";
import { applyDelta } from "../apply";
import type { RowDelta } from "../types";

/** Helper to create a branded HLC timestamp for testing */
const hlc = BigInt(1_000_000) as HLCTimestamp;

/** Helper to build a RowDelta for testing */
function makeDelta(overrides: Partial<RowDelta> & Pick<RowDelta, "op" | "columns">): RowDelta {
	return {
		table: "users",
		rowId: "row-1",
		clientId: "client-1",
		hlc,
		deltaId: "test-delta-id",
		...overrides,
	};
}

describe("applyDelta", () => {
	// Test 6: applyDelta merges partial update
	it("should merge a partial UPDATE onto an existing row", () => {
		const row = { name: "Alice", age: 30, city: "London" };
		const delta = makeDelta({
			op: "UPDATE",
			columns: [
				{ column: "age", value: 31 },
				{ column: "city", value: "Manchester" },
			],
		});

		const result = applyDelta(row, delta);

		expect(result).not.toBeNull();
		expect(result).toEqual({ name: "Alice", age: 31, city: "Manchester" });
	});

	// Test 6b: applyDelta does not mutate the original row (immutable)
	it("should not mutate the original row", () => {
		const row = { name: "Alice", age: 30 };
		const delta = makeDelta({
			op: "UPDATE",
			columns: [{ column: "age", value: 31 }],
		});

		const result = applyDelta(row, delta);

		expect(result).toEqual({ name: "Alice", age: 31 });
		expect(row).toEqual({ name: "Alice", age: 30 }); // Original unchanged
	});

	// Test 7: applyDelta INSERT creates new row
	it("should create a new row from an INSERT delta", () => {
		const delta = makeDelta({
			op: "INSERT",
			columns: [
				{ column: "name", value: "Bob" },
				{ column: "age", value: 25 },
			],
		});

		const result = applyDelta(null, delta);

		expect(result).not.toBeNull();
		expect(result).toEqual({ name: "Bob", age: 25 });
	});

	// Test 8: applyDelta DELETE returns null
	it("should return null for a DELETE delta", () => {
		const row = { name: "Alice", age: 30 };
		const delta = makeDelta({
			op: "DELETE",
			columns: [],
		});

		const result = applyDelta(row, delta);

		expect(result).toBeNull();
	});

	// Test: DELETE on null row still returns null
	it("should return null for DELETE even when row is already null", () => {
		const delta = makeDelta({
			op: "DELETE",
			columns: [],
		});

		const result = applyDelta(null, delta);

		expect(result).toBeNull();
	});

	// Test: UPDATE adds new columns to existing row
	it("should add new columns when UPDATE introduces previously absent columns", () => {
		const row = { name: "Alice" };
		const delta = makeDelta({
			op: "UPDATE",
			columns: [{ column: "age", value: 30 }],
		});

		const result = applyDelta(row, delta);

		expect(result).toEqual({ name: "Alice", age: 30 });
	});

	// Test: null values are applied correctly
	it("should set column to null when delta value is null", () => {
		const row = { name: "Alice", age: 30 };
		const delta = makeDelta({
			op: "UPDATE",
			columns: [{ column: "age", value: null }],
		});

		const result = applyDelta(row, delta);

		expect(result).toEqual({ name: "Alice", age: null });
	});
});

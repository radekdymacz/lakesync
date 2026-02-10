import type { HLCTimestamp, RowDelta, TableSchema } from "@lakesync/core";
import { Ok } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { buildSchemaIndex, groupDeltasByTable, isMaterialisable } from "../materialise";

function makeDelta(overrides: Partial<RowDelta> = {}): RowDelta {
	return {
		deltaId: "delta-1",
		table: "todos",
		rowId: "row-1",
		clientId: "client-a",
		columns: [{ column: "title", value: "Buy milk" }],
		hlc: BigInt(1000) as HLCTimestamp,
		op: "INSERT",
		...overrides,
	};
}

describe("isMaterialisable", () => {
	it("returns true for an object with a materialise function", () => {
		const adapter = {
			materialise: async () => Ok(undefined),
		};
		expect(isMaterialisable(adapter)).toBe(true);
	});

	it("returns false for null", () => {
		expect(isMaterialisable(null)).toBe(false);
	});

	it("returns false for undefined", () => {
		expect(isMaterialisable(undefined)).toBe(false);
	});

	it("returns false for an object without materialise", () => {
		expect(isMaterialisable({ insertDeltas: () => {} })).toBe(false);
	});

	it("returns false for an object where materialise is not a function", () => {
		expect(isMaterialisable({ materialise: "not a function" })).toBe(false);
	});
});

describe("groupDeltasByTable", () => {
	it("returns empty map for empty deltas", () => {
		const result = groupDeltasByTable([]);
		expect(result.size).toBe(0);
	});

	it("groups deltas by table name", () => {
		const deltas = [
			makeDelta({ table: "todos", rowId: "r1" }),
			makeDelta({ table: "todos", rowId: "r2" }),
			makeDelta({ table: "users", rowId: "r3" }),
		];

		const result = groupDeltasByTable(deltas);
		expect(result.size).toBe(2);
		expect(result.get("todos")).toEqual(new Set(["r1", "r2"]));
		expect(result.get("users")).toEqual(new Set(["r3"]));
	});

	it("deduplicates row IDs within the same table", () => {
		const deltas = [
			makeDelta({ table: "todos", rowId: "r1", deltaId: "d1" }),
			makeDelta({ table: "todos", rowId: "r1", deltaId: "d2" }),
		];

		const result = groupDeltasByTable(deltas);
		expect(result.get("todos")!.size).toBe(1);
		expect(result.get("todos")).toEqual(new Set(["r1"]));
	});
});

describe("buildSchemaIndex", () => {
	it("returns empty map for empty schemas", () => {
		const result = buildSchemaIndex([]);
		expect(result.size).toBe(0);
	});

	it("indexes by table name when sourceTable is not set", () => {
		const schemas: TableSchema[] = [
			{ table: "todos", columns: [{ name: "title", type: "string" }] },
			{ table: "users", columns: [{ name: "name", type: "string" }] },
		];

		const result = buildSchemaIndex(schemas);
		expect(result.size).toBe(2);
		expect(result.get("todos")!.table).toBe("todos");
		expect(result.get("users")!.table).toBe("users");
	});

	it("indexes by sourceTable when present", () => {
		const schemas: TableSchema[] = [
			{
				table: "tickets",
				sourceTable: "jira_issues",
				columns: [{ name: "summary", type: "string" }],
			},
		];

		const result = buildSchemaIndex(schemas);
		expect(result.size).toBe(1);
		expect(result.has("jira_issues")).toBe(true);
		expect(result.has("tickets")).toBe(false);
		expect(result.get("jira_issues")!.table).toBe("tickets");
	});

	it("handles mix of sourceTable and table-only schemas", () => {
		const schemas: TableSchema[] = [
			{ table: "todos", columns: [{ name: "title", type: "string" }] },
			{
				table: "tickets",
				sourceTable: "jira_issues",
				columns: [{ name: "summary", type: "string" }],
			},
		];

		const result = buildSchemaIndex(schemas);
		expect(result.size).toBe(2);
		expect(result.get("todos")!.table).toBe("todos");
		expect(result.get("jira_issues")!.table).toBe("tickets");
	});
});

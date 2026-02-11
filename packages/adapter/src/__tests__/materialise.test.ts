import type { TableSchema } from "@lakesync/core";
import { Ok } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import {
	buildSchemaIndex,
	groupDeltasByTable,
	isMaterialisable,
	isSoftDelete,
	resolveConflictColumns,
	resolvePrimaryKey,
} from "../materialise";
import { makeDelta } from "./test-helpers";

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

describe("resolvePrimaryKey", () => {
	it("defaults to ['row_id'] when primaryKey is not set", () => {
		const schema: TableSchema = {
			table: "todos",
			columns: [{ name: "title", type: "string" }],
		};
		expect(resolvePrimaryKey(schema)).toEqual(["row_id"]);
	});

	it("returns custom primaryKey when set", () => {
		const schema: TableSchema = {
			table: "orders",
			columns: [
				{ name: "org_id", type: "string" },
				{ name: "order_id", type: "string" },
			],
			primaryKey: ["org_id", "order_id"],
		};
		expect(resolvePrimaryKey(schema)).toEqual(["org_id", "order_id"]);
	});
});

describe("resolveConflictColumns", () => {
	it("defaults to primary key when externalIdColumn is not set", () => {
		const schema: TableSchema = {
			table: "todos",
			columns: [{ name: "title", type: "string" }],
		};
		expect(resolveConflictColumns(schema)).toEqual(["row_id"]);
	});

	it("uses custom primaryKey when externalIdColumn is not set", () => {
		const schema: TableSchema = {
			table: "orders",
			columns: [
				{ name: "org_id", type: "string" },
				{ name: "order_id", type: "string" },
			],
			primaryKey: ["org_id", "order_id"],
		};
		expect(resolveConflictColumns(schema)).toEqual(["org_id", "order_id"]);
	});

	it("uses externalIdColumn when set", () => {
		const schema: TableSchema = {
			table: "users",
			columns: [
				{ name: "name", type: "string" },
				{ name: "ext_id", type: "string" },
			],
			externalIdColumn: "ext_id",
		};
		expect(resolveConflictColumns(schema)).toEqual(["ext_id"]);
	});
});

describe("isSoftDelete", () => {
	it("defaults to true when softDelete is not set", () => {
		const schema: TableSchema = {
			table: "todos",
			columns: [{ name: "title", type: "string" }],
		};
		expect(isSoftDelete(schema)).toBe(true);
	});

	it("returns true when softDelete is explicitly true", () => {
		const schema: TableSchema = {
			table: "todos",
			columns: [{ name: "title", type: "string" }],
			softDelete: true,
		};
		expect(isSoftDelete(schema)).toBe(true);
	});

	it("returns false when softDelete is explicitly false", () => {
		const schema: TableSchema = {
			table: "todos",
			columns: [{ name: "title", type: "string" }],
			softDelete: false,
		};
		expect(isSoftDelete(schema)).toBe(false);
	});
});

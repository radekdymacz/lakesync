import { describe, expect, it } from "vitest";
import { extractTables } from "../extract-tables";

describe("extractTables", () => {
	it("extracts table from simple SELECT", () => {
		expect(extractTables("SELECT * FROM todos")).toEqual(["todos"]);
	});

	it("extracts table from SELECT with WHERE clause", () => {
		expect(extractTables("SELECT * FROM todos WHERE done = 0")).toEqual(["todos"]);
	});

	it("handles case-insensitive FROM", () => {
		expect(extractTables("select * from todos")).toEqual(["todos"]);
	});

	it("extracts multiple tables from JOIN", () => {
		const sql = "SELECT t.*, u.name FROM todos t JOIN users u ON t.userId = u._rowId";
		const tables = extractTables(sql);
		expect(tables).toContain("todos");
		expect(tables).toContain("users");
		expect(tables).toHaveLength(2);
	});

	it("extracts from LEFT JOIN", () => {
		const sql = "SELECT * FROM todos LEFT JOIN users ON todos.userId = users._rowId";
		const tables = extractTables(sql);
		expect(tables).toContain("todos");
		expect(tables).toContain("users");
	});

	it("handles double-quoted identifiers", () => {
		expect(extractTables('SELECT * FROM "my-table"')).toEqual(["my-table"]);
	});

	it("handles backtick-quoted identifiers", () => {
		expect(extractTables("SELECT * FROM `my_table`")).toEqual(["my_table"]);
	});

	it("returns empty array for queries without FROM", () => {
		expect(extractTables("SELECT 1 + 1")).toEqual([]);
	});

	it("deduplicates tables", () => {
		const sql = "SELECT * FROM todos JOIN todos ON 1=1";
		expect(extractTables(sql)).toEqual(["todos"]);
	});
});

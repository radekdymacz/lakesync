import type { TableSchema } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { readEqualityDeletes, writeEqualityDeletes } from "../equality-delete";

/** Test schema for a todos table */
const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

describe("Equality delete files", () => {
	it("writes 5 deleted rowIds and reads them back as an exact match", async () => {
		const deletedRows = [
			{ table: "todos", rowId: "row-1" },
			{ table: "todos", rowId: "row-2" },
			{ table: "todos", rowId: "row-3" },
			{ table: "todos", rowId: "row-4" },
			{ table: "todos", rowId: "row-5" },
		];

		const writeResult = await writeEqualityDeletes(deletedRows, todoSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const data = writeResult.value;
		expect(data).toBeInstanceOf(Uint8Array);
		expect(data.byteLength).toBeGreaterThan(0);

		const readResult = await readEqualityDeletes(data);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(5);

		// Verify exact match — order is preserved
		for (let i = 0; i < deletedRows.length; i++) {
			expect(readResult.value[i]!.table).toBe(deletedRows[i]!.table);
			expect(readResult.value[i]!.rowId).toBe(deletedRows[i]!.rowId);
		}
	});

	it("returns empty Uint8Array for empty deletes and reads back as empty array", async () => {
		const writeResult = await writeEqualityDeletes([], todoSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const data = writeResult.value;
		expect(data).toBeInstanceOf(Uint8Array);
		expect(data.byteLength).toBe(0);

		const readResult = await readEqualityDeletes(data);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(0);
	});

	it("handles a large batch of 100 deletes in a single file with correct roundtrip", async () => {
		const deletedRows = Array.from({ length: 100 }, (_, i) => ({
			table: "todos",
			rowId: `row-${i}`,
		}));

		const writeResult = await writeEqualityDeletes(deletedRows, todoSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const data = writeResult.value;
		expect(data).toBeInstanceOf(Uint8Array);
		expect(data.byteLength).toBeGreaterThan(0);

		const readResult = await readEqualityDeletes(data);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(100);

		// Verify all 100 rows roundtrip correctly
		for (let i = 0; i < 100; i++) {
			expect(readResult.value[i]!.table).toBe("todos");
			expect(readResult.value[i]!.rowId).toBe(`row-${i}`);
		}
	});

	it("handles deletes across multiple tables", async () => {
		const deletedRows = [
			{ table: "todos", rowId: "row-1" },
			{ table: "projects", rowId: "proj-1" },
			{ table: "todos", rowId: "row-2" },
			{ table: "users", rowId: "user-42" },
		];

		const writeResult = await writeEqualityDeletes(deletedRows, todoSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const readResult = await readEqualityDeletes(writeResult.value);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(4);

		// Verify table names are preserved correctly
		expect(readResult.value[0]).toEqual({ table: "todos", rowId: "row-1" });
		expect(readResult.value[1]).toEqual({ table: "projects", rowId: "proj-1" });
		expect(readResult.value[2]).toEqual({ table: "todos", rowId: "row-2" });
		expect(readResult.value[3]).toEqual({ table: "users", rowId: "user-42" });
	});
});

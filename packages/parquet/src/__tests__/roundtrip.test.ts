import { HLC, type RowDelta, type TableSchema } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { readParquetToDeltas } from "../reader";
import { writeDeltasToParquet } from "../writer";

/**
 * Helper to create a deterministic deltaId for tests.
 * Uses a simple composite string rather than SHA-256 for test simplicity.
 */
function testDeltaId(clientId: string, hlc: bigint, index: number): string {
	return `delta-${clientId}-${hlc.toString(16)}-${index}`;
}

/** Shared test schema with mixed column types */
const mixedSchema: TableSchema = {
	table: "test_table",
	columns: [
		{ name: "title", type: "string" },
		{ name: "score", type: "number" },
		{ name: "active", type: "boolean" },
		{ name: "metadata", type: "json" },
	],
};

/** Simple string-only schema */
const simpleSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

/**
 * Creates a batch of test deltas with predictable values.
 */
function createTestDeltas(count: number, schema: TableSchema): RowDelta[] {
	const deltas: RowDelta[] = [];
	const baseWall = 1700000000000;

	for (let i = 0; i < count; i++) {
		const hlc = HLC.encode(baseWall + i, i % 100);
		const clientId = `client-${(i % 3) + 1}`;
		const deltaId = testDeltaId(clientId, hlc, i);

		const columns = schema.columns.map((col) => {
			switch (col.type) {
				case "string":
					return { column: col.name, value: `value-${i}` };
				case "number":
					return { column: col.name, value: i * 1.5 };
				case "boolean":
					return { column: col.name, value: i % 2 === 0 };
				case "json":
					return {
						column: col.name,
						value: { key: `item-${i}`, nested: { count: i } },
					};
				case "null":
					return { column: col.name, value: null };
			}
		});

		deltas.push({
			op: "INSERT",
			table: schema.table,
			rowId: `row-${i}`,
			clientId,
			columns,
			hlc,
			deltaId,
		});
	}

	return deltas;
}

describe("Parquet roundtrip", () => {
	it("should roundtrip 10 deltas through write and read", async () => {
		const deltas = createTestDeltas(10, simpleSchema);

		const writeResult = await writeDeltasToParquet(deltas, simpleSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const parquetBytes = writeResult.value;
		expect(parquetBytes).toBeInstanceOf(Uint8Array);
		expect(parquetBytes.length).toBeGreaterThan(0);

		const readResult = await readParquetToDeltas(parquetBytes);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		const restored = readResult.value;
		expect(restored).toHaveLength(10);

		for (let i = 0; i < deltas.length; i++) {
			expect(restored[i]!.op).toBe(deltas[i]!.op);
			expect(restored[i]!.table).toBe(deltas[i]!.table);
			expect(restored[i]!.rowId).toBe(deltas[i]!.rowId);
			expect(restored[i]!.clientId).toBe(deltas[i]!.clientId);
			expect(restored[i]!.deltaId).toBe(deltas[i]!.deltaId);
			expect(restored[i]!.hlc).toBe(deltas[i]!.hlc);
			expect(restored[i]!.columns).toHaveLength(deltas[i]!.columns.length);

			for (let j = 0; j < deltas[i]!.columns.length; j++) {
				expect(restored[i]!.columns[j]!.column).toBe(deltas[i]!.columns[j]!.column);
				expect(restored[i]!.columns[j]!.value).toEqual(deltas[i]!.columns[j]!.value);
			}
		}
	});

	it("should handle a large batch of 1000 deltas without errors", async () => {
		const deltas = createTestDeltas(1000, simpleSchema);

		const writeResult = await writeDeltasToParquet(deltas, simpleSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const readResult = await readParquetToDeltas(writeResult.value);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(1000);

		// Spot-check first, middle, and last deltas
		expect(readResult.value[0]!.rowId).toBe("row-0");
		expect(readResult.value[499]!.rowId).toBe("row-499");
		expect(readResult.value[999]!.rowId).toBe("row-999");
	});

	it("should preserve mixed column types: string, number, boolean, json", async () => {
		const hlc = HLC.encode(1700000000000, 1);
		const deltas: RowDelta[] = [
			{
				op: "INSERT",
				table: "test_table",
				rowId: "row-1",
				clientId: "client-a",
				columns: [
					{ column: "title", value: "Hello World" },
					{ column: "score", value: 42.5 },
					{ column: "active", value: true },
					{ column: "metadata", value: { tags: ["a", "b"], count: 3 } },
				],
				hlc,
				deltaId: "delta-mixed-1",
			},
			{
				op: "UPDATE",
				table: "test_table",
				rowId: "row-2",
				clientId: "client-b",
				columns: [
					{ column: "title", value: "Goodbye" },
					{ column: "score", value: 0 },
					{ column: "active", value: false },
					{ column: "metadata", value: { empty: true } },
				],
				hlc: HLC.encode(1700000000001, 0),
				deltaId: "delta-mixed-2",
			},
		];

		const writeResult = await writeDeltasToParquet(deltas, mixedSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const readResult = await readParquetToDeltas(writeResult.value);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		const restored = readResult.value;
		expect(restored).toHaveLength(2);

		// First row — all types
		const row0 = restored[0]!;
		expect(row0.op).toBe("INSERT");
		const titleCol = row0.columns.find((c) => c.column === "title");
		const scoreCol = row0.columns.find((c) => c.column === "score");
		const activeCol = row0.columns.find((c) => c.column === "active");
		const metadataCol = row0.columns.find((c) => c.column === "metadata");

		expect(titleCol?.value).toBe("Hello World");
		expect(scoreCol?.value).toBe(42.5);
		expect(activeCol?.value).toBe(true);
		expect(metadataCol?.value).toEqual({ tags: ["a", "b"], count: 3 });

		// Second row
		const row1 = restored[1]!;
		expect(row1.op).toBe("UPDATE");
		const score1 = row1.columns.find((c) => c.column === "score");
		const active1 = row1.columns.find((c) => c.column === "active");
		expect(score1?.value).toBe(0);
		expect(active1?.value).toBe(false);
	});

	it("should preserve HLC timestamps as branded bigints", async () => {
		const wall = 1700000000000;
		const counter = 12345;
		const hlc = HLC.encode(wall, counter);

		const deltas: RowDelta[] = [
			{
				op: "INSERT",
				table: "todos",
				rowId: "row-hlc",
				clientId: "client-x",
				columns: [
					{ column: "title", value: "HLC test" },
					{ column: "completed", value: false },
				],
				hlc,
				deltaId: "delta-hlc-test",
			},
		];

		const writeResult = await writeDeltasToParquet(deltas, simpleSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const readResult = await readParquetToDeltas(writeResult.value);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		const restored = readResult.value[0]!;
		expect(typeof restored.hlc).toBe("bigint");
		expect(restored.hlc).toBe(hlc);

		// Verify we can decode the HLC back to its components
		const decoded = HLC.decode(restored.hlc);
		expect(decoded.wall).toBe(wall);
		expect(decoded.counter).toBe(counter);
	});

	it("should roundtrip DELETE operations with empty columns array", async () => {
		const hlc = HLC.encode(1700000000000, 0);

		const deltas: RowDelta[] = [
			{
				op: "DELETE",
				table: "todos",
				rowId: "row-deleted",
				clientId: "client-z",
				columns: [],
				hlc,
				deltaId: "delta-delete-1",
			},
		];

		const writeResult = await writeDeltasToParquet(deltas, simpleSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const readResult = await readParquetToDeltas(writeResult.value);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		const restored = readResult.value;
		expect(restored).toHaveLength(1);

		const row = restored[0]!;
		expect(row.op).toBe("DELETE");
		expect(row.table).toBe("todos");
		expect(row.rowId).toBe("row-deleted");
		expect(row.clientId).toBe("client-z");
		expect(row.deltaId).toBe("delta-delete-1");
		expect(row.hlc).toBe(hlc);
		expect(row.columns).toHaveLength(0);
	});
});

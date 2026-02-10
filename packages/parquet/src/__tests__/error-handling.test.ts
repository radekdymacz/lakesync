import { HLC, type RowDelta, type TableSchema } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { readParquetToDeltas, writeDeltasToParquet } from "../index.js";

/**
 * Helper to create a test delta with predictable values.
 */
function makeDelta(
	op: RowDelta["op"],
	rowId: string,
	wall: number,
	counter: number,
	columns: RowDelta["columns"],
	table = "test_table",
	clientId = "client-1",
): RowDelta {
	const hlc = HLC.encode(wall, counter);
	return {
		op,
		table,
		rowId,
		clientId,
		columns,
		hlc,
		deltaId: `delta-${rowId}-${wall}-${counter}`,
	};
}

/** Simple string + boolean schema */
const simpleSchema: TableSchema = {
	table: "test_table",
	columns: [
		{ name: "title", type: "string" },
		{ name: "done", type: "boolean" },
	],
};

/** Schema with all column types */
const _mixedSchema: TableSchema = {
	table: "test_table",
	columns: [
		{ name: "name", type: "string" },
		{ name: "score", type: "number" },
		{ name: "active", type: "boolean" },
		{ name: "metadata", type: "json" },
	],
};

/**
 * Helper: write deltas then read them back, asserting both succeed.
 */
async function roundtrip(deltas: RowDelta[], schema: TableSchema): Promise<RowDelta[]> {
	const writeResult = await writeDeltasToParquet(deltas, schema);
	expect(writeResult.ok).toBe(true);
	if (!writeResult.ok) throw new Error("Write failed");

	const readResult = await readParquetToDeltas(writeResult.value);
	expect(readResult.ok).toBe(true);
	if (!readResult.ok) throw new Error("Read failed");

	return readResult.value;
}

describe("Error handling", () => {
	it("should return FlushError for empty Uint8Array", async () => {
		const result = await readParquetToDeltas(new Uint8Array(0));
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to read deltas from Parquet");
		}
	});

	it("should return FlushError for random garbage bytes", async () => {
		const garbage = new Uint8Array(256);
		for (let i = 0; i < garbage.length; i++) {
			garbage[i] = Math.floor(Math.random() * 256);
		}
		const result = await readParquetToDeltas(garbage);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to read deltas from Parquet");
		}
	});

	it("should return FlushError for truncated valid parquet", async () => {
		// Write a valid parquet file first
		const deltas = [
			makeDelta("INSERT", "row-1", 1700000000000, 0, [
				{ column: "title", value: "test" },
				{ column: "done", value: true },
			]),
		];
		const writeResult = await writeDeltasToParquet(deltas, simpleSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		// Truncate to first 10 bytes
		const truncated = writeResult.value.slice(0, 10);
		const result = await readParquetToDeltas(truncated);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to read deltas from Parquet");
		}
	});

	it("should produce a valid parquet file from empty deltas array", async () => {
		const result = await writeDeltasToParquet([], simpleSchema);
		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value).toBeInstanceOf(Uint8Array);
		expect(result.value.length).toBeGreaterThan(0);
	});

	it("should roundtrip empty deltas to an empty array", async () => {
		const writeResult = await writeDeltasToParquet([], simpleSchema);
		expect(writeResult.ok).toBe(true);
		if (!writeResult.ok) return;

		const readResult = await readParquetToDeltas(writeResult.value);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(0);
	});
});

describe("Single delta roundtrips", () => {
	it("should roundtrip a single INSERT delta", async () => {
		const delta = makeDelta("INSERT", "row-1", 1700000000000, 0, [
			{ column: "title", value: "Insert test" },
			{ column: "done", value: false },
		]);

		const restored = await roundtrip([delta], simpleSchema);
		expect(restored).toHaveLength(1);
		expect(restored[0]!.op).toBe("INSERT");
		expect(restored[0]!.rowId).toBe("row-1");
		expect(restored[0]!.columns.find((c) => c.column === "title")?.value).toBe("Insert test");
	});

	it("should roundtrip a single UPDATE delta", async () => {
		const delta = makeDelta("UPDATE", "row-2", 1700000001000, 5, [
			{ column: "title", value: "Updated title" },
		]);

		const restored = await roundtrip([delta], simpleSchema);
		expect(restored).toHaveLength(1);
		expect(restored[0]!.op).toBe("UPDATE");
		expect(restored[0]!.rowId).toBe("row-2");
	});

	it("should roundtrip a single DELETE delta with empty columns", async () => {
		const delta = makeDelta("DELETE", "row-3", 1700000002000, 0, []);

		const restored = await roundtrip([delta], simpleSchema);
		expect(restored).toHaveLength(1);
		expect(restored[0]!.op).toBe("DELETE");
		expect(restored[0]!.rowId).toBe("row-3");
		expect(restored[0]!.columns).toHaveLength(0);
	});
});

describe("Column value edge cases", () => {
	it("should preserve Unicode characters (emoji, CJK, accented)", async () => {
		const stringSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "text", type: "string" }],
		};

		const delta = makeDelta("INSERT", "row-unicode", 1700000000000, 0, [
			{
				column: "text",
				value: "Hello \u{1F600} \u4F60\u597D \u00E9\u00E0\u00FC \u{1F525}\u{1F30D}",
			},
		]);

		const restored = await roundtrip([delta], stringSchema);
		expect(restored[0]!.columns[0]!.value).toBe(
			"Hello \u{1F600} \u4F60\u597D \u00E9\u00E0\u00FC \u{1F525}\u{1F30D}",
		);
	});

	it("should preserve strings with newlines, tabs, and quotes", async () => {
		const stringSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "text", type: "string" }],
		};

		const value = 'line1\nline2\ttab\t"quoted"\r\nwindows';
		const delta = makeDelta("INSERT", "row-special", 1700000000000, 0, [{ column: "text", value }]);

		const restored = await roundtrip([delta], stringSchema);
		expect(restored[0]!.columns[0]!.value).toBe(value);
	});

	it("should preserve very long string column values (10KB+)", async () => {
		const stringSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "text", type: "string" }],
		};

		const longValue = "A".repeat(10240); // 10KB
		const delta = makeDelta("INSERT", "row-long", 1700000000000, 0, [
			{ column: "text", value: longValue },
		]);

		const restored = await roundtrip([delta], stringSchema);
		expect(restored[0]!.columns[0]!.value).toBe(longValue);
	});

	it("should preserve number edge cases: 0, -0, large, and small", async () => {
		const numSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "val", type: "number" }],
		};

		const testValues = [0, Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER, 1e-10, 1e20];

		for (const val of testValues) {
			const delta = makeDelta("INSERT", `row-${val}`, 1700000000000, 0, [
				{ column: "val", value: val },
			]);

			const restored = await roundtrip([delta], numSchema);
			expect(restored[0]!.columns[0]!.value).toBe(val);
		}
	});

	it("should preserve deeply nested JSON objects (5 levels)", async () => {
		const jsonSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "data", type: "json" }],
		};

		const deepValue = {
			l1: {
				l2: {
					l3: {
						l4: {
							l5: "deep",
						},
					},
				},
			},
		};

		const delta = makeDelta("INSERT", "row-deep", 1700000000000, 0, [
			{ column: "data", value: deepValue },
		]);

		const restored = await roundtrip([delta], jsonSchema);
		expect(restored[0]!.columns[0]!.value).toEqual(deepValue);
	});

	it("should preserve JSON arrays of mixed types", async () => {
		const jsonSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "data", type: "json" }],
		};

		const mixedArray = [1, "two", true, null, { nested: [3, 4] }];
		const delta = makeDelta("INSERT", "row-array", 1700000000000, 0, [
			{ column: "data", value: mixedArray },
		]);

		const restored = await roundtrip([delta], jsonSchema);
		expect(restored[0]!.columns[0]!.value).toEqual(mixedArray);
	});

	it("should handle all-null values in a column across multiple deltas", async () => {
		const stringSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "optional", type: "string" }],
		};

		// Deltas that never set the 'optional' column -> all nulls
		const deltas = [
			makeDelta("INSERT", "row-1", 1700000000000, 0, []),
			makeDelta("INSERT", "row-2", 1700000001000, 0, []),
			makeDelta("INSERT", "row-3", 1700000002000, 0, []),
		];

		const restored = await roundtrip(deltas, stringSchema);
		expect(restored).toHaveLength(3);
		// All rows should have no columns (nulls are skipped on read)
		for (const row of restored) {
			expect(row.columns).toHaveLength(0);
		}
	});

	it("should handle mixed null and non-null values in same column", async () => {
		const stringSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "optional", type: "string" }],
		};

		const deltas = [
			makeDelta("INSERT", "row-1", 1700000000000, 0, [{ column: "optional", value: "present" }]),
			makeDelta("INSERT", "row-2", 1700000001000, 0, []),
			makeDelta("INSERT", "row-3", 1700000002000, 0, [
				{ column: "optional", value: "also present" },
			]),
		];

		const restored = await roundtrip(deltas, stringSchema);
		expect(restored).toHaveLength(3);
		expect(restored[0]!.columns[0]!.value).toBe("present");
		expect(restored[1]!.columns).toHaveLength(0);
		expect(restored[2]!.columns[0]!.value).toBe("also present");
	});
});

describe("Boolean workaround (BOOL_COLUMNS_METADATA_KEY)", () => {
	it("should roundtrip all-null boolean column", async () => {
		const boolSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "flag", type: "boolean" }],
		};

		// Deltas that never set the boolean column -> all nulls
		const deltas = [
			makeDelta("INSERT", "row-1", 1700000000000, 0, []),
			makeDelta("INSERT", "row-2", 1700000001000, 0, []),
		];

		const restored = await roundtrip(deltas, boolSchema);
		expect(restored).toHaveLength(2);
		// All-null booleans should produce no columns per row
		for (const row of restored) {
			expect(row.columns).toHaveLength(0);
		}
	});

	it("should roundtrip boolean column with mixed true/false/null values", async () => {
		const boolSchema: TableSchema = {
			table: "test_table",
			columns: [{ name: "flag", type: "boolean" }],
		};

		const deltas = [
			makeDelta("INSERT", "row-1", 1700000000000, 0, [{ column: "flag", value: true }]),
			makeDelta("INSERT", "row-2", 1700000001000, 0, [{ column: "flag", value: false }]),
			makeDelta("INSERT", "row-3", 1700000002000, 0, []),
		];

		const restored = await roundtrip(deltas, boolSchema);
		expect(restored).toHaveLength(3);
		expect(restored[0]!.columns[0]!.value).toBe(true);
		expect(restored[1]!.columns[0]!.value).toBe(false);
		// Third row has null boolean -> skipped
		expect(restored[2]!.columns).toHaveLength(0);
	});
});

describe("HLC preservation", () => {
	it("should preserve HLC with counter=0 (just wall time)", async () => {
		const hlc = HLC.encode(1700000000000, 0);
		const delta = makeDelta("INSERT", "row-1", 1700000000000, 0, [
			{ column: "title", value: "test" },
			{ column: "done", value: false },
		]);

		const restored = await roundtrip([delta], simpleSchema);
		expect(restored[0]!.hlc).toBe(hlc);

		const decoded = HLC.decode(restored[0]!.hlc);
		expect(decoded.wall).toBe(1700000000000);
		expect(decoded.counter).toBe(0);
	});

	it("should preserve HLC with max 16-bit counter (65535)", async () => {
		const hlc = HLC.encode(1700000000000, 65535);
		const delta: RowDelta = {
			op: "INSERT",
			table: "test_table",
			rowId: "row-max-counter",
			clientId: "client-1",
			columns: [
				{ column: "title", value: "max counter" },
				{ column: "done", value: true },
			],
			hlc,
			deltaId: "delta-max-counter",
		};

		const restored = await roundtrip([delta], simpleSchema);
		expect(restored[0]!.hlc).toBe(hlc);

		const decoded = HLC.decode(restored[0]!.hlc);
		expect(decoded.wall).toBe(1700000000000);
		expect(decoded.counter).toBe(65535);
	});
});

describe("Schema edge cases", () => {
	it("should handle schema with many columns (20+)", async () => {
		const manyColumns: TableSchema = {
			table: "test_table",
			columns: Array.from({ length: 25 }, (_, i) => ({
				name: `col_${i}`,
				type: "string" as const,
			})),
		};

		const columns = manyColumns.columns.map((col) => ({
			column: col.name,
			value: `value_${col.name}`,
		}));

		const delta = makeDelta("INSERT", "row-many", 1700000000000, 0, columns);

		const restored = await roundtrip([delta], manyColumns);
		expect(restored).toHaveLength(1);
		expect(restored[0]!.columns).toHaveLength(25);

		for (let i = 0; i < 25; i++) {
			const col = restored[0]!.columns.find((c) => c.column === `col_${i}`);
			expect(col).toBeDefined();
			expect(col!.value).toBe(`value_col_${i}`);
		}
	});

	it("should handle schema with only system columns (no user columns)", async () => {
		const emptySchema: TableSchema = {
			table: "test_table",
			columns: [],
		};

		const delta = makeDelta("INSERT", "row-sys-only", 1700000000000, 0, []);

		const restored = await roundtrip([delta], emptySchema);
		expect(restored).toHaveLength(1);
		expect(restored[0]!.op).toBe("INSERT");
		expect(restored[0]!.rowId).toBe("row-sys-only");
		expect(restored[0]!.columns).toHaveLength(0);
	});
});

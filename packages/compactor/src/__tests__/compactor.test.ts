import type { LakeAdapter } from "@lakesync/adapter";
import {
	AdapterError,
	Err,
	HLC,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import { readParquetToDeltas, writeDeltasToParquet } from "@lakesync/parquet";
import { describe, expect, it } from "vitest";
import { Compactor } from "../compactor";
import { readEqualityDeletes } from "../equality-delete";
import { type CompactionConfig, DEFAULT_COMPACTION_CONFIG } from "../types";

/** Test schema for a todos table */
const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

/** Default test compaction config with low thresholds */
const testConfig: CompactionConfig = {
	minDeltaFiles: 2,
	maxDeltaFiles: 50,
	targetFileSizeBytes: 128 * 1024 * 1024,
};

/** Base wall clock value for test timestamps */
const BASE_WALL = 1700000000000;

/**
 * Create a simple in-memory mock adapter for testing.
 * Stores objects in a Map keyed by path.
 */
function createMockAdapter(): LakeAdapter & { stored: Map<string, Uint8Array> } {
	const stored = new Map<string, Uint8Array>();
	return {
		stored,
		async putObject(path: string, data: Uint8Array): Promise<Result<void, AdapterError>> {
			stored.set(path, data);
			return Ok(undefined);
		},
		async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
			const data = stored.get(path);
			return data ? Ok(data) : Err(new AdapterError("Not found"));
		},
		async headObject(
			path: string,
		): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
			const data = stored.get(path);
			return data
				? Ok({ size: data.length, lastModified: new Date() })
				: Err(new AdapterError("Not found"));
		},
		async listObjects(
			prefix: string,
		): Promise<Result<Array<{ key: string; size: number; lastModified: Date }>, AdapterError>> {
			const results = [...stored.entries()]
				.filter(([k]) => k.startsWith(prefix))
				.map(([key, data]) => ({
					key,
					size: data.length,
					lastModified: new Date(),
				}));
			return Ok(results);
		},
		async deleteObject(path: string): Promise<Result<void, AdapterError>> {
			stored.delete(path);
			return Ok(undefined);
		},
		async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
			for (const p of paths) stored.delete(p);
			return Ok(undefined);
		},
	};
}

/**
 * Helper to create a test delta with sensible defaults.
 */
function makeDelta(opts: {
	rowId: string;
	hlc: HLCTimestamp;
	op?: "INSERT" | "UPDATE" | "DELETE";
	columns?: Array<{ column: string; value: unknown }>;
	clientId?: string;
}): RowDelta {
	return {
		op: opts.op ?? "INSERT",
		table: "todos",
		rowId: opts.rowId,
		clientId: opts.clientId ?? "client-a",
		columns: opts.columns ?? [
			{ column: "title", value: `Todo ${opts.rowId}` },
			{ column: "completed", value: false },
		],
		hlc: opts.hlc,
		deltaId: `delta-${opts.rowId}-${opts.hlc.toString(16)}`,
	};
}

/**
 * Seed a delta file into the mock adapter by writing deltas as Parquet.
 */
async function seedDeltaFile(
	adapter: LakeAdapter & { stored: Map<string, Uint8Array> },
	key: string,
	deltas: RowDelta[],
	schema: TableSchema,
): Promise<void> {
	const result = await writeDeltasToParquet(deltas, schema);
	if (!result.ok) {
		throw new Error(`Failed to seed delta file: ${result.error.message}`);
	}
	await adapter.putObject(key, result.value);
}

describe("DEFAULT_COMPACTION_CONFIG", () => {
	it("limits maxDeltaFiles to 20 to cap compaction memory", () => {
		expect(DEFAULT_COMPACTION_CONFIG.maxDeltaFiles).toBe(20);
	});
});

describe("Compactor", () => {
	it("compacts 20 INSERT deltas across files into 1 base file with 20 rows and 0 delete files", async () => {
		const adapter = createMockAdapter();

		// Create 4 delta files with 5 INSERTs each (unique rows)
		for (let fileIdx = 0; fileIdx < 4; fileIdx++) {
			const deltas: RowDelta[] = [];
			for (let i = 0; i < 5; i++) {
				const rowIdx = fileIdx * 5 + i;
				deltas.push(
					makeDelta({
						rowId: `row-${rowIdx}`,
						hlc: HLC.encode(BASE_WALL + rowIdx, 0),
					}),
				);
			}
			await seedDeltaFile(adapter, `deltas/file-${fileIdx}.parquet`, deltas, todoSchema);
		}

		const deltaKeys = Array.from({ length: 4 }, (_, i) => `deltas/file-${i}.parquet`);
		const compactor = new Compactor(adapter, testConfig, todoSchema);
		const result = await compactor.compact(deltaKeys, "output/compacted");

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.baseFilesWritten).toBe(1);
		expect(result.value.deleteFilesWritten).toBe(0);
		expect(result.value.deltaFilesCompacted).toBe(4);
		expect(result.value.bytesRead).toBeGreaterThan(0);
		expect(result.value.bytesWritten).toBeGreaterThan(0);

		// Verify the base file was written and contains 20 rows
		const outputFiles = [...adapter.stored.keys()].filter((k) => k.startsWith("output/compacted/"));
		const baseFiles = outputFiles.filter((k) => k.includes("/base-"));
		const deleteFiles = outputFiles.filter((k) => k.includes("/delete-"));

		expect(baseFiles).toHaveLength(1);
		expect(deleteFiles).toHaveLength(0);

		// Read back the base file and verify row count
		const baseData = adapter.stored.get(baseFiles[0]!);
		expect(baseData).toBeDefined();
		const readResult = await readParquetToDeltas(baseData!);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(20);

		// All rows should be INSERT ops
		for (const delta of readResult.value) {
			expect(delta.op).toBe("INSERT");
		}
	});

	it("resolves INSERT + UPDATE on the same row to the latest values", async () => {
		const adapter = createMockAdapter();

		// File 1: INSERT row-1 with title="Original", completed=false
		await seedDeltaFile(
			adapter,
			"deltas/file-0.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL, 0),
					op: "INSERT",
					columns: [
						{ column: "title", value: "Original" },
						{ column: "completed", value: false },
					],
				}),
			],
			todoSchema,
		);

		// File 2: UPDATE row-1 with title="Updated", completed=true
		await seedDeltaFile(
			adapter,
			"deltas/file-1.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 1000, 0),
					op: "UPDATE",
					columns: [
						{ column: "title", value: "Updated" },
						{ column: "completed", value: true },
					],
				}),
			],
			todoSchema,
		);

		const compactor = new Compactor(adapter, testConfig, todoSchema);
		const result = await compactor.compact(
			["deltas/file-0.parquet", "deltas/file-1.parquet"],
			"output/compacted",
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.baseFilesWritten).toBe(1);
		expect(result.value.deleteFilesWritten).toBe(0);

		// Read back the base file
		const baseFiles = [...adapter.stored.keys()].filter(
			(k) => k.startsWith("output/compacted/") && k.includes("/base-"),
		);
		expect(baseFiles).toHaveLength(1);

		const readResult = await readParquetToDeltas(adapter.stored.get(baseFiles[0]!)!);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(1);

		const row = readResult.value[0]!;
		expect(row.op).toBe("INSERT");
		expect(row.rowId).toBe("row-1");

		const titleCol = row.columns.find((c) => c.column === "title");
		const completedCol = row.columns.find((c) => c.column === "completed");

		expect(titleCol?.value).toBe("Updated");
		expect(completedCol?.value).toBe(true);
	});

	it("resolves INSERT + DELETE on the same row into a delete file with no base row", async () => {
		const adapter = createMockAdapter();

		// File 1: INSERT row-1
		await seedDeltaFile(
			adapter,
			"deltas/file-0.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL, 0),
					op: "INSERT",
					columns: [
						{ column: "title", value: "To be deleted" },
						{ column: "completed", value: false },
					],
				}),
			],
			todoSchema,
		);

		// File 2: DELETE row-1
		await seedDeltaFile(
			adapter,
			"deltas/file-1.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 1000, 0),
					op: "DELETE",
					columns: [],
				}),
			],
			todoSchema,
		);

		const compactor = new Compactor(adapter, testConfig, todoSchema);
		const result = await compactor.compact(
			["deltas/file-0.parquet", "deltas/file-1.parquet"],
			"output/compacted",
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.baseFilesWritten).toBe(0);
		expect(result.value.deleteFilesWritten).toBe(1);

		// Verify delete file exists and base file does not
		const outputFiles = [...adapter.stored.keys()].filter((k) => k.startsWith("output/compacted/"));
		const baseFiles = outputFiles.filter((k) => k.includes("/base-"));
		const deleteFiles = outputFiles.filter((k) => k.includes("/delete-"));

		expect(baseFiles).toHaveLength(0);
		expect(deleteFiles).toHaveLength(1);

		// Read back the equality delete file
		const readResult = await readEqualityDeletes(adapter.stored.get(deleteFiles[0]!)!);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(1);

		const row = readResult.value[0]!;
		expect(row.table).toBe("todos");
		expect(row.rowId).toBe("row-1");
	});

	it("handles mixed operations: 50 rows with 10 deleted produces correct base + delete files", async () => {
		const adapter = createMockAdapter();

		// Create delta files: 50 INSERTs across 5 files (10 per file)
		for (let fileIdx = 0; fileIdx < 5; fileIdx++) {
			const deltas: RowDelta[] = [];
			for (let i = 0; i < 10; i++) {
				const rowIdx = fileIdx * 10 + i;
				deltas.push(
					makeDelta({
						rowId: `row-${rowIdx}`,
						hlc: HLC.encode(BASE_WALL + rowIdx, 0),
						op: "INSERT",
						columns: [
							{ column: "title", value: `Todo ${rowIdx}` },
							{ column: "completed", value: false },
						],
					}),
				);
			}
			await seedDeltaFile(adapter, `deltas/insert-${fileIdx}.parquet`, deltas, todoSchema);
		}

		// Create 2 more delta files with DELETEs for rows 0-9 (10 deletes)
		const deleteDeltas: RowDelta[] = [];
		for (let i = 0; i < 10; i++) {
			deleteDeltas.push(
				makeDelta({
					rowId: `row-${i}`,
					hlc: HLC.encode(BASE_WALL + 100000 + i, 0),
					op: "DELETE",
					columns: [],
				}),
			);
		}
		await seedDeltaFile(adapter, "deltas/delete-0.parquet", deleteDeltas.slice(0, 5), todoSchema);
		await seedDeltaFile(adapter, "deltas/delete-1.parquet", deleteDeltas.slice(5, 10), todoSchema);

		const deltaKeys = [
			...Array.from({ length: 5 }, (_, i) => `deltas/insert-${i}.parquet`),
			"deltas/delete-0.parquet",
			"deltas/delete-1.parquet",
		];

		const compactor = new Compactor(adapter, testConfig, todoSchema);
		const result = await compactor.compact(deltaKeys, "output/compacted");

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.baseFilesWritten).toBe(1);
		expect(result.value.deleteFilesWritten).toBe(1);
		expect(result.value.deltaFilesCompacted).toBe(7);

		// Verify base file has 40 live rows
		const baseFiles = [...adapter.stored.keys()].filter(
			(k) => k.startsWith("output/compacted/") && k.includes("/base-"),
		);
		expect(baseFiles).toHaveLength(1);

		const baseResult = await readParquetToDeltas(adapter.stored.get(baseFiles[0]!)!);
		expect(baseResult.ok).toBe(true);
		if (!baseResult.ok) return;
		expect(baseResult.value).toHaveLength(40);

		// Verify delete file has 10 deleted rows
		const deleteFiles = [...adapter.stored.keys()].filter(
			(k) => k.startsWith("output/compacted/") && k.includes("/delete-"),
		);
		expect(deleteFiles).toHaveLength(1);

		const deleteResult = await readEqualityDeletes(adapter.stored.get(deleteFiles[0]!)!);
		expect(deleteResult.ok).toBe(true);
		if (!deleteResult.ok) return;
		expect(deleteResult.value).toHaveLength(10);

		// All equality delete rows should have the correct table
		for (const row of deleteResult.value) {
			expect(row.table).toBe("todos");
		}

		// All base file rows should be INSERT ops
		for (const delta of baseResult.value) {
			expect(delta.op).toBe("INSERT");
		}
	});

	it("skips compaction and returns zero result when below minDeltaFiles threshold", async () => {
		const adapter = createMockAdapter();

		// Seed just 1 delta file (below threshold of 2)
		await seedDeltaFile(
			adapter,
			"deltas/file-0.parquet",
			[
				makeDelta({
					rowId: "row-0",
					hlc: HLC.encode(BASE_WALL, 0),
				}),
			],
			todoSchema,
		);

		const compactor = new Compactor(adapter, testConfig, todoSchema);
		const result = await compactor.compact(["deltas/file-0.parquet"], "output/compacted");

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.baseFilesWritten).toBe(0);
		expect(result.value.deleteFilesWritten).toBe(0);
		expect(result.value.deltaFilesCompacted).toBe(0);
		expect(result.value.bytesRead).toBe(0);
		expect(result.value.bytesWritten).toBe(0);

		// No output files should have been written
		const outputFiles = [...adapter.stored.keys()].filter((k) => k.startsWith("output/compacted/"));
		expect(outputFiles).toHaveLength(0);
	});

	it("resolves correctly with out-of-order HLCs across files", async () => {
		const adapter = createMockAdapter();

		// File 1: row-1 INSERT at HLC=10, row-2 INSERT at HLC=20
		await seedDeltaFile(
			adapter,
			"deltas/file-0.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 10, 0),
					op: "INSERT",
					columns: [
						{ column: "title", value: "Row1 Early" },
						{ column: "completed", value: false },
					],
				}),
				makeDelta({
					rowId: "row-2",
					hlc: HLC.encode(BASE_WALL + 20, 0),
					op: "INSERT",
					columns: [
						{ column: "title", value: "Row2 Early" },
						{ column: "completed", value: false },
					],
				}),
			],
			todoSchema,
		);

		// File 2: row-1 UPDATE at HLC=30 (later), row-2 UPDATE at HLC=5 (earlier — should lose)
		await seedDeltaFile(
			adapter,
			"deltas/file-1.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 30, 0),
					op: "UPDATE",
					columns: [
						{ column: "title", value: "Row1 Late" },
						{ column: "completed", value: true },
					],
				}),
				makeDelta({
					rowId: "row-2",
					hlc: HLC.encode(BASE_WALL + 5, 0),
					op: "UPDATE",
					columns: [
						{ column: "title", value: "Row2 Stale" },
						{ column: "completed", value: true },
					],
				}),
			],
			todoSchema,
		);

		const compactor = new Compactor(adapter, testConfig, todoSchema);
		const result = await compactor.compact(
			["deltas/file-0.parquet", "deltas/file-1.parquet"],
			"output/compacted",
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.baseFilesWritten).toBe(1);

		const baseFiles = [...adapter.stored.keys()].filter(
			(k) => k.startsWith("output/compacted/") && k.includes("/base-"),
		);
		const readResult = await readParquetToDeltas(adapter.stored.get(baseFiles[0]!)!);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(2);

		const row1 = readResult.value.find((r) => r.rowId === "row-1")!;
		const row2 = readResult.value.find((r) => r.rowId === "row-2")!;

		// row-1: UPDATE at HLC=30 wins over INSERT at HLC=10
		expect(row1.columns.find((c) => c.column === "title")?.value).toBe("Row1 Late");
		expect(row1.columns.find((c) => c.column === "completed")?.value).toBe(true);

		// row-2: INSERT at HLC=20 wins over UPDATE at HLC=5
		expect(row2.columns.find((c) => c.column === "title")?.value).toBe("Row2 Early");
		expect(row2.columns.find((c) => c.column === "completed")?.value).toBe(false);
	});

	it("handles delete-then-reinsert across files", async () => {
		const adapter = createMockAdapter();

		// File 1: INSERT row-1 at HLC=10
		await seedDeltaFile(
			adapter,
			"deltas/file-0.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 10, 0),
					op: "INSERT",
					columns: [
						{ column: "title", value: "Original" },
						{ column: "completed", value: false },
					],
				}),
			],
			todoSchema,
		);

		// File 2: DELETE row-1 at HLC=20
		await seedDeltaFile(
			adapter,
			"deltas/file-1.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 20, 0),
					op: "DELETE",
					columns: [],
				}),
			],
			todoSchema,
		);

		// File 3: INSERT row-1 at HLC=30 (re-insert after delete)
		await seedDeltaFile(
			adapter,
			"deltas/file-2.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 30, 0),
					op: "INSERT",
					columns: [
						{ column: "title", value: "Reinserted" },
						{ column: "completed", value: true },
					],
				}),
			],
			todoSchema,
		);

		const compactor = new Compactor(adapter, testConfig, todoSchema);
		const result = await compactor.compact(
			["deltas/file-0.parquet", "deltas/file-1.parquet", "deltas/file-2.parquet"],
			"output/compacted",
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		// Row should be live, not deleted
		expect(result.value.baseFilesWritten).toBe(1);
		expect(result.value.deleteFilesWritten).toBe(0);

		const baseFiles = [...adapter.stored.keys()].filter(
			(k) => k.startsWith("output/compacted/") && k.includes("/base-"),
		);
		const readResult = await readParquetToDeltas(adapter.stored.get(baseFiles[0]!)!);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(1);
		const row = readResult.value[0]!;
		expect(row.rowId).toBe("row-1");
		expect(row.columns.find((c) => c.column === "title")?.value).toBe("Reinserted");
		expect(row.columns.find((c) => c.column === "completed")?.value).toBe(true);
	});

	it("column-level LWW resolves independently per column", async () => {
		const adapter = createMockAdapter();

		// File 1: INSERT row-1 with title at HLC=20, completed at HLC=20
		await seedDeltaFile(
			adapter,
			"deltas/file-0.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 20, 0),
					op: "INSERT",
					columns: [
						{ column: "title", value: "Title@20" },
						{ column: "completed", value: false },
					],
				}),
			],
			todoSchema,
		);

		// File 2: UPDATE row-1 with only title at HLC=30 (higher — wins for title)
		await seedDeltaFile(
			adapter,
			"deltas/file-1.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 30, 0),
					op: "UPDATE",
					columns: [{ column: "title", value: "Title@30" }],
				}),
			],
			todoSchema,
		);

		// File 3: UPDATE row-1 with only completed at HLC=25 (higher than 20 — wins for completed)
		await seedDeltaFile(
			adapter,
			"deltas/file-2.parquet",
			[
				makeDelta({
					rowId: "row-1",
					hlc: HLC.encode(BASE_WALL + 25, 0),
					op: "UPDATE",
					columns: [{ column: "completed", value: true }],
				}),
			],
			todoSchema,
		);

		const compactor = new Compactor(adapter, testConfig, todoSchema);
		const result = await compactor.compact(
			["deltas/file-0.parquet", "deltas/file-1.parquet", "deltas/file-2.parquet"],
			"output/compacted",
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.baseFilesWritten).toBe(1);

		const baseFiles = [...adapter.stored.keys()].filter(
			(k) => k.startsWith("output/compacted/") && k.includes("/base-"),
		);
		const readResult = await readParquetToDeltas(adapter.stored.get(baseFiles[0]!)!);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(1);
		const row = readResult.value[0]!;

		// title: HLC=30 wins over HLC=20
		expect(row.columns.find((c) => c.column === "title")?.value).toBe("Title@30");
		// completed: HLC=25 wins over HLC=20
		expect(row.columns.find((c) => c.column === "completed")?.value).toBe(true);
	});

	it("limits compaction to maxDeltaFiles when more files are provided", async () => {
		const adapter = createMockAdapter();

		const limitConfig: CompactionConfig = {
			minDeltaFiles: 2,
			maxDeltaFiles: 3,
			targetFileSizeBytes: 128 * 1024 * 1024,
		};

		// Create 5 delta files
		for (let fileIdx = 0; fileIdx < 5; fileIdx++) {
			await seedDeltaFile(
				adapter,
				`deltas/file-${fileIdx}.parquet`,
				[
					makeDelta({
						rowId: `row-${fileIdx}`,
						hlc: HLC.encode(BASE_WALL + fileIdx, 0),
					}),
				],
				todoSchema,
			);
		}

		const deltaKeys = Array.from({ length: 5 }, (_, i) => `deltas/file-${i}.parquet`);
		const compactor = new Compactor(adapter, limitConfig, todoSchema);
		const result = await compactor.compact(deltaKeys, "output/compacted");

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		// Should only compact the first 3 files
		expect(result.value.deltaFilesCompacted).toBe(3);
		expect(result.value.baseFilesWritten).toBe(1);

		// Read back the base file — should only have 3 rows (from first 3 files)
		const baseFiles = [...adapter.stored.keys()].filter(
			(k) => k.startsWith("output/compacted/") && k.includes("/base-"),
		);
		const readResult = await readParquetToDeltas(adapter.stored.get(baseFiles[0]!)!);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		expect(readResult.value).toHaveLength(3);
	});
});

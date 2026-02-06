import type { LakeAdapter, ObjectInfo } from "@lakesync/adapter";
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
import { writeDeltasToParquet } from "@lakesync/parquet";
import { describe, expect, it } from "vitest";
import { Compactor } from "../compactor";
import { MaintenanceRunner } from "../maintenance";
import type { CompactionConfig, CompactionResult } from "../types";

/** Test schema for a todos table */
const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

/** Default test compaction config with low thresholds */
const testCompactionConfig: CompactionConfig = {
	minDeltaFiles: 2,
	maxDeltaFiles: 50,
	targetFileSizeBytes: 128 * 1024 * 1024,
};

/** Base wall clock value for test timestamps */
const BASE_WALL = 1700000000000;

/** One hour in milliseconds */
const ONE_HOUR = 60 * 60 * 1000;

/**
 * Create a simple in-memory mock adapter for testing.
 * Stores objects in a Map keyed by path, with configurable lastModified dates.
 */
function createMockAdapter(): LakeAdapter & {
	stored: Map<string, Uint8Array>;
	metadata: Map<string, { lastModified: Date }>;
	setLastModified(key: string, date: Date): void;
} {
	const stored = new Map<string, Uint8Array>();
	const metadata = new Map<string, { lastModified: Date }>();

	return {
		stored,
		metadata,
		setLastModified(key: string, date: Date): void {
			metadata.set(key, { lastModified: date });
		},
		async putObject(path: string, data: Uint8Array): Promise<Result<void, AdapterError>> {
			stored.set(path, data);
			metadata.set(path, { lastModified: new Date() });
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
			if (!data) return Err(new AdapterError("Not found"));
			const meta = metadata.get(path);
			return Ok({
				size: data.length,
				lastModified: meta?.lastModified ?? new Date(),
			});
		},
		async listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>> {
			const results = [...stored.entries()]
				.filter(([k]) => k.startsWith(prefix))
				.map(([key, data]) => {
					const meta = metadata.get(key);
					return {
						key,
						size: data.length,
						lastModified: meta?.lastModified ?? new Date(),
					};
				});
			return Ok(results);
		},
		async deleteObject(path: string): Promise<Result<void, AdapterError>> {
			stored.delete(path);
			metadata.delete(path);
			return Ok(undefined);
		},
		async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
			for (const p of paths) {
				stored.delete(p);
				metadata.delete(p);
			}
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
	adapter: ReturnType<typeof createMockAdapter>,
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

describe("MaintenanceRunner", () => {
	it("full cycle: compact + clean leaves only active files in storage", async () => {
		const adapter = createMockAdapter();

		// Seed 4 delta files (above minDeltaFiles threshold of 2)
		const deltaKeys: string[] = [];
		for (let i = 0; i < 4; i++) {
			const key = `data/deltas/file-${i}.parquet`;
			deltaKeys.push(key);
			await seedDeltaFile(
				adapter,
				key,
				[
					makeDelta({
						rowId: `row-${i}`,
						hlc: HLC.encode(BASE_WALL + i, 0),
					}),
				],
				todoSchema,
			);
			// Make delta files old enough to be orphan-eligible
			adapter.setLastModified(key, new Date(Date.now() - 2 * ONE_HOUR));
		}

		const compactor = new Compactor(adapter, testCompactionConfig, todoSchema);
		const runner = new MaintenanceRunner(compactor, adapter, {
			retainSnapshots: 5,
			orphanAgeMs: ONE_HOUR,
		});

		const result = await runner.run(deltaKeys, "data/output", "data/");

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		// Compaction should have processed all 4 delta files
		expect(result.value.compaction.deltaFilesCompacted).toBe(4);
		expect(result.value.compaction.baseFilesWritten).toBe(1);

		// Old delta files should have been removed as orphans
		expect(result.value.orphansRemoved).toBe(4);

		// Only output files should remain in storage
		const remainingKeys = [...adapter.stored.keys()];
		for (const key of remainingKeys) {
			expect(key.startsWith("data/output")).toBe(true);
		}
	});

	it("orphan removal skips files younger than orphanAgeMs", async () => {
		const adapter = createMockAdapter();

		// Seed 3 delta files
		const deltaKeys: string[] = [];
		for (let i = 0; i < 3; i++) {
			const key = `data/deltas/file-${i}.parquet`;
			deltaKeys.push(key);
			await seedDeltaFile(
				adapter,
				key,
				[
					makeDelta({
						rowId: `row-${i}`,
						hlc: HLC.encode(BASE_WALL + i, 0),
					}),
				],
				todoSchema,
			);
		}

		// Make 2 old and 1 recent
		adapter.setLastModified(deltaKeys[0]!, new Date(Date.now() - 2 * ONE_HOUR));
		adapter.setLastModified(deltaKeys[1]!, new Date(Date.now() - 2 * ONE_HOUR));
		// deltaKeys[2] stays at current time (just created)

		const compactor = new Compactor(adapter, testCompactionConfig, todoSchema);
		const runner = new MaintenanceRunner(compactor, adapter, {
			retainSnapshots: 5,
			orphanAgeMs: ONE_HOUR,
		});

		const result = await runner.run(deltaKeys, "data/output", "data/");

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		// Only 2 old delta files should be removed; the recent one is protected
		expect(result.value.orphansRemoved).toBe(2);

		// The recent delta file should still exist
		expect(adapter.stored.has(deltaKeys[2]!)).toBe(true);
	});

	it("returns 0 orphans when there are no orphans to remove", async () => {
		const adapter = createMockAdapter();

		// Seed 2 delta files
		const deltaKeys: string[] = [];
		for (let i = 0; i < 2; i++) {
			const key = `data/deltas/file-${i}.parquet`;
			deltaKeys.push(key);
			await seedDeltaFile(
				adapter,
				key,
				[
					makeDelta({
						rowId: `row-${i}`,
						hlc: HLC.encode(BASE_WALL + i, 0),
					}),
				],
				todoSchema,
			);
			// Keep files recent so they cannot be orphaned
			adapter.setLastModified(key, new Date());
		}

		const compactor = new Compactor(adapter, testCompactionConfig, todoSchema);
		const runner = new MaintenanceRunner(compactor, adapter, {
			retainSnapshots: 5,
			orphanAgeMs: ONE_HOUR,
		});

		const result = await runner.run(deltaKeys, "data/output", "data/");

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		// Compaction runs but no orphans are old enough to remove
		expect(result.value.compaction.deltaFilesCompacted).toBe(2);
		expect(result.value.orphansRemoved).toBe(0);
	});

	it("compaction below threshold skips compaction but still runs orphan cleanup", async () => {
		const adapter = createMockAdapter();

		// Seed only 1 delta file (below minDeltaFiles threshold of 2)
		const deltaKeys = ["data/deltas/file-0.parquet"];
		await seedDeltaFile(
			adapter,
			deltaKeys[0]!,
			[
				makeDelta({
					rowId: "row-0",
					hlc: HLC.encode(BASE_WALL, 0),
				}),
			],
			todoSchema,
		);

		// Also add a stale orphan file that is not a delta file
		const orphanKey = "data/stale-leftover.parquet";
		adapter.stored.set(orphanKey, new Uint8Array(10));
		adapter.setLastModified(orphanKey, new Date(Date.now() - 2 * ONE_HOUR));

		const compactor = new Compactor(adapter, testCompactionConfig, todoSchema);
		const runner = new MaintenanceRunner(compactor, adapter, {
			retainSnapshots: 5,
			orphanAgeMs: ONE_HOUR,
		});

		const result = await runner.run(deltaKeys, "data/output", "data/");

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		// Compaction was skipped (below threshold)
		expect(result.value.compaction.deltaFilesCompacted).toBe(0);
		expect(result.value.compaction.baseFilesWritten).toBe(0);

		// The delta file that was not compacted remains active
		expect(adapter.stored.has(deltaKeys[0]!)).toBe(true);

		// The stale orphan should have been removed
		expect(result.value.orphansRemoved).toBe(1);
		expect(adapter.stored.has(orphanKey)).toBe(false);
	});

	describe("removeOrphans", () => {
		it("deletes only files not in activeKeys that are old enough", async () => {
			const adapter = createMockAdapter();

			// Create 5 files: 3 active, 2 orphaned (1 old, 1 recent)
			const activeKey1 = "prefix/active-1.parquet";
			const activeKey2 = "prefix/active-2.parquet";
			const activeKey3 = "prefix/active-3.parquet";
			const orphanOld = "prefix/orphan-old.parquet";
			const orphanRecent = "prefix/orphan-recent.parquet";

			for (const key of [activeKey1, activeKey2, activeKey3, orphanOld, orphanRecent]) {
				adapter.stored.set(key, new Uint8Array(10));
				adapter.setLastModified(key, new Date());
			}

			// Make the old orphan old enough
			adapter.setLastModified(orphanOld, new Date(Date.now() - 2 * ONE_HOUR));

			const compactor = new Compactor(adapter, testCompactionConfig, todoSchema);
			const runner = new MaintenanceRunner(compactor, adapter, {
				retainSnapshots: 5,
				orphanAgeMs: ONE_HOUR,
			});

			const activeKeys = new Set([activeKey1, activeKey2, activeKey3]);
			const result = await runner.removeOrphans("prefix/", activeKeys);

			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// Only the old orphan should be removed
			expect(result.value).toBe(1);
			expect(adapter.stored.has(orphanOld)).toBe(false);

			// Active files and recent orphan should remain
			expect(adapter.stored.has(activeKey1)).toBe(true);
			expect(adapter.stored.has(activeKey2)).toBe(true);
			expect(adapter.stored.has(activeKey3)).toBe(true);
			expect(adapter.stored.has(orphanRecent)).toBe(true);
		});

		it("returns 0 when all files are in activeKeys", async () => {
			const adapter = createMockAdapter();

			const key1 = "prefix/file-1.parquet";
			const key2 = "prefix/file-2.parquet";

			for (const key of [key1, key2]) {
				adapter.stored.set(key, new Uint8Array(10));
				adapter.setLastModified(key, new Date(Date.now() - 2 * ONE_HOUR));
			}

			const compactor = new Compactor(adapter, testCompactionConfig, todoSchema);
			const runner = new MaintenanceRunner(compactor, adapter, {
				retainSnapshots: 5,
				orphanAgeMs: ONE_HOUR,
			});

			const activeKeys = new Set([key1, key2]);
			const result = await runner.removeOrphans("prefix/", activeKeys);

			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value).toBe(0);
		});

		it("returns 0 when there are no files at all", async () => {
			const adapter = createMockAdapter();

			const compactor = new Compactor(adapter, testCompactionConfig, todoSchema);
			const runner = new MaintenanceRunner(compactor, adapter, {
				retainSnapshots: 5,
				orphanAgeMs: ONE_HOUR,
			});

			const result = await runner.removeOrphans("prefix/", new Set());

			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value).toBe(0);
		});
	});
});

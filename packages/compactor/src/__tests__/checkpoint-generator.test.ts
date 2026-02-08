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
import { writeDeltasToParquet } from "@lakesync/parquet";
import { decodeSyncResponse } from "@lakesync/proto";
import { describe, expect, it } from "vitest";
import { CheckpointGenerator, type CheckpointManifest } from "../checkpoint-generator";

const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

const BASE_WALL = 1700000000000;

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
		deltaId: `delta-${opts.rowId}`,
	};
}

async function writeBaseFile(
	adapter: LakeAdapter & { stored: Map<string, Uint8Array> },
	key: string,
	deltas: RowDelta[],
): Promise<void> {
	const result = await writeDeltasToParquet(deltas, todoSchema);
	if (!result.ok) throw new Error("Failed to write test Parquet");
	adapter.stored.set(key, result.value);
}

describe("CheckpointGenerator", () => {
	it("generates a single chunk for small dataset", async () => {
		const adapter = createMockAdapter();
		const snapshotHlc = HLC.encode(BASE_WALL + 5000, 0);

		// Create a base file with 3 deltas
		const deltas = [
			makeDelta({ rowId: "r1", hlc: HLC.encode(BASE_WALL + 1000, 0) }),
			makeDelta({ rowId: "r2", hlc: HLC.encode(BASE_WALL + 2000, 0) }),
			makeDelta({ rowId: "r3", hlc: HLC.encode(BASE_WALL + 3000, 0) }),
		];
		await writeBaseFile(adapter, "base/base-001.parquet", deltas);

		const generator = new CheckpointGenerator(adapter, todoSchema, "gw-test");
		const result = await generator.generate(["base/base-001.parquet"], snapshotHlc);

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.chunksWritten).toBe(1);
		expect(result.value.snapshotHlc).toBe(snapshotHlc);
		expect(result.value.bytesWritten).toBeGreaterThan(0);

		// Verify manifest
		const manifestData = adapter.stored.get("checkpoints/gw-test/manifest.json");
		expect(manifestData).toBeDefined();
		const manifest = JSON.parse(new TextDecoder().decode(manifestData!)) as CheckpointManifest;
		expect(manifest.chunkCount).toBe(1);
		expect(manifest.totalDeltas).toBe(3);
		expect(manifest.chunks).toEqual(["chunk-000.bin"]);
		expect(manifest.snapshotHlc).toBe(snapshotHlc.toString());

		// Verify chunk is a valid proto SyncResponse
		const chunkData = adapter.stored.get("checkpoints/gw-test/chunk-000.bin");
		expect(chunkData).toBeDefined();
		const decoded = decodeSyncResponse(chunkData!);
		expect(decoded.ok).toBe(true);
		if (decoded.ok) {
			expect(decoded.value.deltas).toHaveLength(3);
		}
	});

	it("returns empty result for no base files", async () => {
		const adapter = createMockAdapter();
		const snapshotHlc = HLC.encode(BASE_WALL, 0);

		const generator = new CheckpointGenerator(adapter, todoSchema, "gw-test");
		const result = await generator.generate([], snapshotHlc);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.chunksWritten).toBe(0);
			expect(result.value.bytesWritten).toBe(0);
		}
	});

	it("splits into multiple chunks when data exceeds chunkBytes", async () => {
		const adapter = createMockAdapter();
		const snapshotHlc = HLC.encode(BASE_WALL + 10000, 0);

		// Create 50 deltas — each ~300 estimated bytes, total ~15000 bytes
		// Set chunkBytes very low to force multi-chunk
		const deltas: RowDelta[] = [];
		for (let i = 0; i < 50; i++) {
			deltas.push(makeDelta({ rowId: `r${i}`, hlc: HLC.encode(BASE_WALL + i * 100, 0) }));
		}
		await writeBaseFile(adapter, "base/base-001.parquet", deltas);

		const generator = new CheckpointGenerator(adapter, todoSchema, "gw-test", {
			chunkBytes: 1000, // Very small to force multi-chunk
		});
		const result = await generator.generate(["base/base-001.parquet"], snapshotHlc);

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.chunksWritten).toBeGreaterThan(1);

		// Verify manifest matches chunks
		const manifestData = adapter.stored.get("checkpoints/gw-test/manifest.json");
		const manifest = JSON.parse(new TextDecoder().decode(manifestData!)) as CheckpointManifest;
		expect(manifest.chunkCount).toBe(result.value.chunksWritten);
		expect(manifest.totalDeltas).toBe(50);

		// Verify all chunks are valid proto
		let totalDeltas = 0;
		for (const chunkName of manifest.chunks) {
			const chunkData = adapter.stored.get(`checkpoints/gw-test/${chunkName}`);
			expect(chunkData).toBeDefined();
			const decoded = decodeSyncResponse(chunkData!);
			expect(decoded.ok).toBe(true);
			if (decoded.ok) {
				totalDeltas += decoded.value.deltas.length;
			}
		}
		expect(totalDeltas).toBe(50);
	});

	it("processes multiple base files sequentially", async () => {
		const adapter = createMockAdapter();
		const snapshotHlc = HLC.encode(BASE_WALL + 5000, 0);

		const deltas1 = [makeDelta({ rowId: "r1", hlc: HLC.encode(BASE_WALL + 1000, 0) })];
		const deltas2 = [makeDelta({ rowId: "r2", hlc: HLC.encode(BASE_WALL + 2000, 0) })];
		await writeBaseFile(adapter, "base/base-001.parquet", deltas1);
		await writeBaseFile(adapter, "base/base-002.parquet", deltas2);

		const generator = new CheckpointGenerator(adapter, todoSchema, "gw-test");
		const result = await generator.generate(
			["base/base-001.parquet", "base/base-002.parquet"],
			snapshotHlc,
		);

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		const manifestData = adapter.stored.get("checkpoints/gw-test/manifest.json");
		const manifest = JSON.parse(new TextDecoder().decode(manifestData!)) as CheckpointManifest;
		expect(manifest.totalDeltas).toBe(2);
	});

	it("returns error when base file is missing", async () => {
		const adapter = createMockAdapter();
		const snapshotHlc = HLC.encode(BASE_WALL, 0);

		const generator = new CheckpointGenerator(adapter, todoSchema, "gw-test");
		const result = await generator.generate(["missing/file.parquet"], snapshotHlc);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("CHECKPOINT_READ_ERROR");
		}
	});

	it("getCheckpointKeys returns all keys for a given chunk count", () => {
		const adapter = createMockAdapter();
		const generator = new CheckpointGenerator(adapter, todoSchema, "gw-test");

		const keys = generator.getCheckpointKeys(3);
		expect(keys).toEqual([
			"checkpoints/gw-test/manifest.json",
			"checkpoints/gw-test/chunk-000.bin",
			"checkpoints/gw-test/chunk-001.bin",
			"checkpoints/gw-test/chunk-002.bin",
		]);
	});
});

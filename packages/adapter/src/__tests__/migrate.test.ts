import {
	type AdapterError,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import type { DatabaseAdapter } from "../db-types";
import { migrateAdapter } from "../migrate";
import { hlc } from "./test-helpers";

function makeDelta(
	overrides: Partial<RowDelta> & { table: string; hlc: HLCTimestamp; deltaId: string },
): RowDelta {
	return {
		op: "INSERT",
		rowId: "r1",
		clientId: "c1",
		columns: [],
		...overrides,
	};
}

/** Simple in-memory DatabaseAdapter for testing. Deduplicates by deltaId on insert. */
class InMemoryAdapter implements DatabaseAdapter {
	deltas: RowDelta[] = [];
	insertCallCount = 0;

	async insertDeltas(deltas: RowDelta[]): Promise<Result<void, AdapterError>> {
		this.insertCallCount++;
		for (const delta of deltas) {
			if (!this.deltas.some((d) => d.deltaId === delta.deltaId)) {
				this.deltas.push(delta);
			}
		}
		return Ok(undefined);
	}

	async queryDeltasSince(
		hlcVal: HLCTimestamp,
		tables?: string[],
	): Promise<Result<RowDelta[], AdapterError>> {
		let result = this.deltas.filter((d) => d.hlc > hlcVal);
		if (tables && tables.length > 0) {
			result = result.filter((d) => tables.includes(d.table));
		}
		result.sort((a, b) => (a.hlc < b.hlc ? -1 : a.hlc > b.hlc ? 1 : 0));
		return Ok(result);
	}

	async getLatestState(
		_table: string,
		_rowId: string,
	): Promise<Result<Record<string, unknown> | null, AdapterError>> {
		return Ok(null);
	}

	async ensureSchema(_schema: TableSchema): Promise<Result<void, AdapterError>> {
		return Ok(undefined);
	}

	async close(): Promise<void> {}
}

describe("migrateAdapter", () => {
	it("migrates all deltas from source to target", async () => {
		const source = new InMemoryAdapter();
		const target = new InMemoryAdapter();

		source.deltas = [
			makeDelta({ table: "users", hlc: hlc(1), deltaId: "d1" }),
			makeDelta({ table: "users", hlc: hlc(2), deltaId: "d2" }),
			makeDelta({ table: "events", hlc: hlc(3), deltaId: "d3" }),
		];

		const result = await migrateAdapter({ from: source, to: target });
		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.totalDeltas).toBe(3);
		expect(target.deltas).toHaveLength(3);
	});

	it("idempotent — running twice produces same result", async () => {
		const source = new InMemoryAdapter();
		const target = new InMemoryAdapter();

		source.deltas = [
			makeDelta({ table: "users", hlc: hlc(1), deltaId: "d1" }),
			makeDelta({ table: "users", hlc: hlc(2), deltaId: "d2" }),
		];

		await migrateAdapter({ from: source, to: target });
		const result = await migrateAdapter({ from: source, to: target });

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.totalDeltas).toBe(2);
		// Target should still only have 2 deltas (deduplicated by deltaId)
		expect(target.deltas).toHaveLength(2);
	});

	it("respects batchSize", async () => {
		const source = new InMemoryAdapter();
		const target = new InMemoryAdapter();

		source.deltas = [
			makeDelta({ table: "t", hlc: hlc(1), deltaId: "d1" }),
			makeDelta({ table: "t", hlc: hlc(2), deltaId: "d2" }),
			makeDelta({ table: "t", hlc: hlc(3), deltaId: "d3" }),
			makeDelta({ table: "t", hlc: hlc(4), deltaId: "d4" }),
			makeDelta({ table: "t", hlc: hlc(5), deltaId: "d5" }),
		];

		const onProgress = vi.fn();

		const result = await migrateAdapter({
			from: source,
			to: target,
			batchSize: 2,
			onProgress,
		});

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.batches).toBe(3); // 2 + 2 + 1
		expect(onProgress).toHaveBeenCalledTimes(3);
	});

	it("onProgress reports correct totals", async () => {
		const source = new InMemoryAdapter();
		const target = new InMemoryAdapter();

		source.deltas = [
			makeDelta({ table: "t", hlc: hlc(1), deltaId: "d1" }),
			makeDelta({ table: "t", hlc: hlc(2), deltaId: "d2" }),
			makeDelta({ table: "t", hlc: hlc(3), deltaId: "d3" }),
		];

		const progressCalls: Array<{ batch: number; totalSoFar: number }> = [];

		await migrateAdapter({
			from: source,
			to: target,
			batchSize: 2,
			onProgress: (info) => progressCalls.push({ ...info }),
		});

		expect(progressCalls).toEqual([
			{ batch: 1, totalSoFar: 2 },
			{ batch: 2, totalSoFar: 3 },
		]);
	});

	it("empty source produces zero result", async () => {
		const source = new InMemoryAdapter();
		const target = new InMemoryAdapter();

		const result = await migrateAdapter({ from: source, to: target });
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.totalDeltas).toBe(0);
		expect(result.value.batches).toBe(0);
	});

	it("tables filter works", async () => {
		const source = new InMemoryAdapter();
		const target = new InMemoryAdapter();

		source.deltas = [
			makeDelta({ table: "users", hlc: hlc(1), deltaId: "d1" }),
			makeDelta({ table: "events", hlc: hlc(2), deltaId: "d2" }),
			makeDelta({ table: "logs", hlc: hlc(3), deltaId: "d3" }),
		];

		const result = await migrateAdapter({
			from: source,
			to: target,
			tables: ["users"],
		});

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.totalDeltas).toBe(1);
		expect(target.deltas).toHaveLength(1);
		expect(target.deltas[0]!.table).toBe("users");
	});
});

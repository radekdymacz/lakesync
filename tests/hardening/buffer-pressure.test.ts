import { DeltaBuffer } from "@lakesync/gateway";
import { describe, expect, it } from "vitest";
import { createTestHLC, makeDelta } from "./helpers";

describe("Buffer Pressure", () => {
	it("shouldFlush triggers when byteSize exceeds maxBytes", () => {
		const buffer = new DeltaBuffer();
		const { hlc, advance } = createTestHLC();

		const maxBytes = 10_000;
		let count = 0;

		// Fill until shouldFlush triggers
		while (!buffer.shouldFlush({ maxBytes, maxAgeMs: 999_999 })) {
			advance(1);
			buffer.append(
				makeDelta({
					hlc: hlc.now(),
					rowId: `row-${count}`,
					clientId: "pressure-client",
					deltaId: `delta-pressure-${count}`,
				}),
			);
			count++;

			// Safety: don't loop forever
			if (count > 100_000) break;
		}

		expect(buffer.byteSize).toBeGreaterThanOrEqual(maxBytes);
		expect(buffer.shouldFlush({ maxBytes, maxAgeMs: 999_999 })).toBe(true);
	});

	it("100K small deltas — logSize and byteSize are proportional", () => {
		const buffer = new DeltaBuffer();
		const { hlc, advance } = createTestHLC();
		const N = 100_000;

		for (let i = 0; i < N; i++) {
			advance(1);
			buffer.append(
				makeDelta({
					hlc: hlc.now(),
					rowId: `row-${i}`,
					clientId: "bulk-client",
					deltaId: `delta-bulk-${i}`,
				}),
			);
		}

		expect(buffer.logSize).toBe(N);
		// Each delta has overhead + string lengths — at minimum ~100 bytes each
		expect(buffer.byteSize).toBeGreaterThan(N * 50);
		// Index: each row is unique so indexSize = logSize
		expect(buffer.indexSize).toBe(N);
	});

	it("10 large deltas (100 KiB strings) — byteSize exceeds 1 MiB", () => {
		const buffer = new DeltaBuffer();
		const { hlc, advance } = createTestHLC();
		const largeValue = "X".repeat(100 * 1024); // 100 KiB string

		for (let i = 0; i < 10; i++) {
			advance(1);
			buffer.append(
				makeDelta({
					hlc: hlc.now(),
					rowId: `big-row-${i}`,
					clientId: "big-client",
					columns: [{ column: "title", value: largeValue }],
					deltaId: `delta-big-${i}`,
				}),
			);
		}

		expect(buffer.logSize).toBe(10);
		// 10 * 100 KiB * 2 (UTF-16) = ~2 MiB minimum
		expect(buffer.byteSize).toBeGreaterThan(1_000_000);
	});

	it("getEventsSince binary search correctness with 10K entries", () => {
		const buffer = new DeltaBuffer();
		const { hlc, advance } = createTestHLC();
		const N = 10_000;
		const timestamps: bigint[] = [];

		for (let i = 0; i < N; i++) {
			advance(1);
			const ts = hlc.now();
			timestamps.push(ts);
			buffer.append(
				makeDelta({
					hlc: ts,
					rowId: `row-${i}`,
					clientId: "search-client",
					deltaId: `delta-search-${i}`,
				}),
			);
		}

		// Query from the start — should get first `limit` entries
		const fromStart = buffer.getEventsSince(0n as never, 100);
		expect(fromStart.deltas).toHaveLength(100);
		expect(fromStart.hasMore).toBe(true);

		// Query from mid-point
		const midIdx = Math.floor(N / 2);
		const fromMid = buffer.getEventsSince(timestamps[midIdx]!, N);
		expect(fromMid.deltas).toHaveLength(N - midIdx - 1);
		expect(fromMid.hasMore).toBe(false);

		// Query from the last timestamp — should get 0 entries
		const fromEnd = buffer.getEventsSince(timestamps[N - 1]!, 100);
		expect(fromEnd.deltas).toHaveLength(0);
		expect(fromEnd.hasMore).toBe(false);

		// Query from one before last — should get 1 entry
		const fromPenultimate = buffer.getEventsSince(timestamps[N - 2]!, 100);
		expect(fromPenultimate.deltas).toHaveLength(1);
		expect(fromPenultimate.deltas[0]!.rowId).toBe(`row-${N - 1}`);
	});

	it("paginated pull — 10K deltas, maxDeltas=100, no gaps or duplicates", () => {
		const buffer = new DeltaBuffer();
		const { hlc, advance } = createTestHLC();
		const N = 10_000;

		for (let i = 0; i < N; i++) {
			advance(1);
			buffer.append(
				makeDelta({
					hlc: hlc.now(),
					rowId: `row-${i}`,
					clientId: "page-client",
					deltaId: `delta-page-${i}`,
				}),
			);
		}

		const PAGE_SIZE = 100;
		const allRowIds = new Set<string>();
		let cursor = 0n as never;
		let pages = 0;

		while (true) {
			const result = buffer.getEventsSince(cursor, PAGE_SIZE);
			for (const delta of result.deltas) {
				expect(allRowIds.has(delta.rowId)).toBe(false); // no duplicates
				allRowIds.add(delta.rowId);
				cursor = delta.hlc;
			}
			pages++;
			if (!result.hasMore) break;
		}

		expect(allRowIds.size).toBe(N);
		expect(pages).toBe(Math.ceil(N / PAGE_SIZE));
	});

	it("drain clears buffer atomically", () => {
		const buffer = new DeltaBuffer();
		const { hlc, advance } = createTestHLC();

		for (let i = 0; i < 1_000; i++) {
			advance(1);
			buffer.append(
				makeDelta({
					hlc: hlc.now(),
					rowId: `row-${i}`,
					clientId: "drain-client",
					deltaId: `delta-drain-${i}`,
				}),
			);
		}

		expect(buffer.logSize).toBe(1_000);
		expect(buffer.byteSize).toBeGreaterThan(0);

		const drained = buffer.drain();
		expect(drained).toHaveLength(1_000);
		expect(buffer.logSize).toBe(0);
		expect(buffer.indexSize).toBe(0);
		expect(buffer.byteSize).toBe(0);
	});

	it("concurrent appends to same row — index tracks latest, log tracks all", () => {
		const buffer = new DeltaBuffer();
		const { hlc, advance } = createTestHLC();
		const N = 1_000;

		for (let i = 0; i < N; i++) {
			advance(1);
			buffer.append(
				makeDelta({
					hlc: hlc.now(),
					rowId: "same-row",
					clientId: "conflict-client",
					columns: [{ column: "title", value: `Version ${i}` }],
					deltaId: `delta-conflict-${i}`,
				}),
			);
		}

		// Log has all entries
		expect(buffer.logSize).toBe(N);
		// Index has only 1 entry (latest for this row)
		expect(buffer.indexSize).toBe(1);
	});
});

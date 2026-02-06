import { HLC, rowKey } from "@lakesync/core";
import type { DeltaOp, HLCTimestamp, RowDelta } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { DeltaBuffer } from "../buffer";

/** Helper to build a RowDelta with sensible defaults */
function makeDelta(overrides: Partial<RowDelta> & { hlc: HLCTimestamp }): RowDelta {
	return {
		op: "UPDATE" as DeltaOp,
		table: "todos",
		rowId: "row-1",
		clientId: "client-a",
		columns: [{ column: "title", value: "Test" }],
		deltaId: `delta-${Math.random().toString(36).slice(2)}`,
		...overrides,
	};
}

describe("DeltaBuffer", () => {
	const hlcLow = HLC.encode(1_000_000, 0);
	const hlcMid = HLC.encode(2_000_000, 0);
	const hlcHigh = HLC.encode(3_000_000, 0);

	it("append adds to both log and index", () => {
		const buffer = new DeltaBuffer();
		const delta = makeDelta({ hlc: hlcLow });

		buffer.append(delta);

		expect(buffer.logSize).toBe(1);
		expect(buffer.indexSize).toBe(1);

		const key = rowKey("todos", "row-1");
		expect(buffer.getRow(key)).toEqual(delta);
	});

	it("getRow returns latest state for a given row key", () => {
		const buffer = new DeltaBuffer();
		const first = makeDelta({ hlc: hlcLow, deltaId: "delta-first" });
		const second = makeDelta({ hlc: hlcHigh, deltaId: "delta-second" });

		buffer.append(first);
		buffer.append(second);

		const key = rowKey("todos", "row-1");
		// Index should reflect the latest appended delta for this row
		expect(buffer.getRow(key)).toEqual(second);
		// Log should contain both
		expect(buffer.logSize).toBe(2);
	});

	it("hasDelta detects duplicate delta IDs", () => {
		const buffer = new DeltaBuffer();
		const delta = makeDelta({ hlc: hlcLow, deltaId: "unique-id" });

		expect(buffer.hasDelta("unique-id")).toBe(false);

		buffer.append(delta);

		expect(buffer.hasDelta("unique-id")).toBe(true);
		expect(buffer.hasDelta("other-id")).toBe(false);
	});

	it("getEventsSince filters by HLC and respects limit", () => {
		const buffer = new DeltaBuffer();
		const d1 = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			deltaId: "delta-1",
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			rowId: "row-2",
			deltaId: "delta-2",
		});
		const d3 = makeDelta({
			hlc: hlcHigh,
			rowId: "row-3",
			deltaId: "delta-3",
		});

		buffer.append(d1);
		buffer.append(d2);
		buffer.append(d3);

		// Should return only deltas with HLC > hlcLow
		const result = buffer.getEventsSince(hlcLow, 10);
		expect(result.deltas).toHaveLength(2);
		expect(result.hasMore).toBe(false);
		expect(result.deltas[0]?.deltaId).toBe("delta-2");
		expect(result.deltas[1]?.deltaId).toBe("delta-3");
	});

	it("shouldFlush triggers at byte threshold", () => {
		const buffer = new DeltaBuffer();

		expect(buffer.shouldFlush({ maxBytes: 100, maxAgeMs: 60_000 })).toBe(false);

		// Append enough deltas to exceed the byte threshold
		for (let i = 0; i < 5; i++) {
			buffer.append(
				makeDelta({
					hlc: HLC.encode(1_000_000 + i, 0),
					rowId: `row-${i}`,
					deltaId: `delta-${i}`,
					columns: [{ column: "title", value: "A reasonably long value" }],
				}),
			);
		}

		expect(buffer.byteSize).toBeGreaterThan(100);
		expect(buffer.shouldFlush({ maxBytes: 100, maxAgeMs: 999_999 })).toBe(true);
	});

	it("shouldFlush triggers at age threshold", () => {
		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		// maxAgeMs=0 means any non-empty buffer is overdue
		expect(buffer.shouldFlush({ maxBytes: 999_999, maxAgeMs: 0 })).toBe(true);
	});

	it("drain clears both log and index, returns entries", () => {
		const buffer = new DeltaBuffer();
		const d1 = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			deltaId: "delta-1",
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			rowId: "row-2",
			deltaId: "delta-2",
		});

		buffer.append(d1);
		buffer.append(d2);

		expect(buffer.logSize).toBe(2);
		expect(buffer.indexSize).toBe(2);

		const drained = buffer.drain();

		expect(drained).toHaveLength(2);
		expect(drained[0]?.deltaId).toBe("delta-1");
		expect(drained[1]?.deltaId).toBe("delta-2");

		// Both structures should be cleared
		expect(buffer.logSize).toBe(0);
		expect(buffer.indexSize).toBe(0);
		expect(buffer.byteSize).toBe(0);
	});
});

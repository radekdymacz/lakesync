import { describe, expect, it } from "vitest";
import {
	BackpressureError,
	BaseSourcePoller,
	Err,
	type FlushError,
	type HLCTimestamp,
	type IngestTarget,
	isIngestTarget,
	Ok,
	type PushTarget,
	type Result,
	type RowDelta,
	type SyncPush,
} from "../index";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let deltaCounter = 0;

function makeDelta(table = "t", rowId?: string): RowDelta {
	deltaCounter++;
	return {
		op: "INSERT",
		table,
		rowId: rowId ?? `r${deltaCounter}`,
		clientId: "test",
		columns: [{ column: "name", value: `val-${deltaCounter}` }],
		hlc: BigInt(deltaCounter) as HLCTimestamp,
		deltaId: `d${deltaCounter}`,
	};
}

/** Collect all deltas from all handlePush calls. */
function collectDeltas(calls: Array<[SyncPush]>): RowDelta[] {
	const all: RowDelta[] = [];
	for (const [push] of calls) {
		for (const d of push.deltas) all.push(d);
	}
	return all;
}

/** Simple PushTarget mock (no flush support). */
function createPushTarget(): PushTarget & { calls: Array<[SyncPush]> } {
	const calls: Array<[SyncPush]> = [];
	return {
		calls,
		handlePush(push: SyncPush) {
			calls.push([push]);
			return Ok({
				serverHlc: 0n as HLCTimestamp,
				accepted: push.deltas.length,
				deltas: push.deltas,
			});
		},
	};
}

/** IngestTarget mock with configurable shouldFlush and bufferStats. */
function createIngestTarget(opts?: {
	shouldFlush?: boolean;
	bufferByteSize?: number;
	backpressureOnPush?: boolean;
}): IngestTarget & {
	calls: Array<[SyncPush]>;
	flushCalls: number;
} {
	const calls: Array<[SyncPush]> = [];
	let flushCalls = 0;
	let rejectNext = opts?.backpressureOnPush ?? false;

	const target: IngestTarget & { calls: Array<[SyncPush]>; flushCalls: number } = {
		get calls() {
			return calls;
		},
		get flushCalls() {
			return flushCalls;
		},
		handlePush(push: SyncPush) {
			if (rejectNext) {
				rejectNext = false; // Only reject once
				return Err(new BackpressureError("Buffer full"));
			}
			calls.push([push]);
			return Ok({
				serverHlc: 0n as HLCTimestamp,
				accepted: push.deltas.length,
				deltas: push.deltas,
			});
		},
		async flush(): Promise<Result<void, FlushError>> {
			flushCalls++;
			return Ok(undefined);
		},
		shouldFlush(): boolean {
			return opts?.shouldFlush ?? false;
		},
		get bufferStats() {
			return { logSize: 0, indexSize: 0, byteSize: opts?.bufferByteSize ?? 0 };
		},
	};
	return target;
}

/** Concrete test poller that exposes accumulate/flush. */
class TestPoller extends BaseSourcePoller {
	deltasToAccumulate: RowDelta[] = [];
	pollCallCount = 0;
	cursor: Record<string, unknown> = {};

	constructor(
		gateway: PushTarget,
		memory?: { chunkSize?: number; memoryBudgetBytes?: number; flushThreshold?: number },
	) {
		super({ name: "test", intervalMs: 60_000, gateway, memory });
	}

	async poll(): Promise<void> {
		this.pollCallCount++;
		for (const d of this.deltasToAccumulate) {
			await this.accumulateDelta(d);
		}
		await this.flushAccumulator();
	}

	getCursorState(): Record<string, unknown> {
		return { ...this.cursor };
	}

	setCursorState(state: Record<string, unknown>): void {
		this.cursor = { ...state };
	}

	/** Expose pushDeltas for backward-compat test. */
	legacyPush(deltas: RowDelta[]): void {
		this.pushDeltas(deltas);
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("BaseSourcePoller — accumulation", () => {
	beforeEach(() => {
		deltaCounter = 0;
	});

	it("accumulateDelta + flushAccumulator pushes all deltas in one chunk when under chunkSize", async () => {
		const target = createPushTarget();
		const poller = new TestPoller(target, { chunkSize: 10 });

		poller.deltasToAccumulate = [makeDelta(), makeDelta(), makeDelta()];
		await poller.poll();

		const all = collectDeltas(target.calls);
		expect(all).toHaveLength(3);
		// Should be one push (3 < chunkSize of 10)
		expect(target.calls).toHaveLength(1);
	});

	it("auto-pushes when chunkSize is reached", async () => {
		const target = createPushTarget();
		const poller = new TestPoller(target, { chunkSize: 2 });

		poller.deltasToAccumulate = [makeDelta(), makeDelta(), makeDelta(), makeDelta(), makeDelta()];
		await poller.poll();

		const all = collectDeltas(target.calls);
		expect(all).toHaveLength(5);
		// 5 deltas / chunkSize 2 = 2 full chunks + 1 remainder
		expect(target.calls).toHaveLength(3);
		expect(target.calls[0]![0].deltas).toHaveLength(2);
		expect(target.calls[1]![0].deltas).toHaveLength(2);
		expect(target.calls[2]![0].deltas).toHaveLength(1);
	});

	it("does not push when no deltas are accumulated", async () => {
		const target = createPushTarget();
		const poller = new TestPoller(target);

		poller.deltasToAccumulate = [];
		await poller.poll();

		expect(target.calls).toHaveLength(0);
	});
});

describe("BaseSourcePoller — IngestTarget flush", () => {
	beforeEach(() => {
		deltaCounter = 0;
	});

	it("flushes before push when shouldFlush() returns true", async () => {
		const target = createIngestTarget({ shouldFlush: true });
		const poller = new TestPoller(target, { chunkSize: 100 });

		poller.deltasToAccumulate = [makeDelta()];
		await poller.poll();

		expect(target.flushCalls).toBeGreaterThanOrEqual(1);
		expect(collectDeltas(target.calls)).toHaveLength(1);
	});

	it("flushes when memory budget threshold is exceeded", async () => {
		const target = createIngestTarget({ bufferByteSize: 80 });
		const poller = new TestPoller(target, {
			chunkSize: 100,
			memoryBudgetBytes: 100,
			flushThreshold: 0.7, // threshold = 70 bytes
		});

		poller.deltasToAccumulate = [makeDelta()];
		await poller.poll();

		// bufferByteSize (80) >= threshold (70), so flush should be called
		expect(target.flushCalls).toBeGreaterThanOrEqual(1);
	});

	it("does not flush when under memory budget", async () => {
		const target = createIngestTarget({ bufferByteSize: 50 });
		const poller = new TestPoller(target, {
			chunkSize: 100,
			memoryBudgetBytes: 100,
			flushThreshold: 0.7, // threshold = 70 bytes
		});

		poller.deltasToAccumulate = [makeDelta()];
		await poller.poll();

		// bufferByteSize (50) < threshold (70), no flush
		expect(target.flushCalls).toBe(0);
	});

	it("retries on backpressure by flushing then re-pushing", async () => {
		const target = createIngestTarget({ backpressureOnPush: true });
		const poller = new TestPoller(target, { chunkSize: 100 });

		poller.deltasToAccumulate = [makeDelta()];
		await poller.poll();

		// First push returns backpressure → flush → retry
		expect(target.flushCalls).toBe(1);
		expect(collectDeltas(target.calls)).toHaveLength(1);
	});
});

describe("BaseSourcePoller — plain PushTarget (no flush)", () => {
	beforeEach(() => {
		deltaCounter = 0;
	});

	it("degrades gracefully — pushes chunks without flush", async () => {
		const target = createPushTarget();
		const poller = new TestPoller(target, { chunkSize: 2 });

		poller.deltasToAccumulate = [makeDelta(), makeDelta(), makeDelta()];
		await poller.poll();

		expect(collectDeltas(target.calls)).toHaveLength(3);
		// No flush on a plain PushTarget
		expect(isIngestTarget(target)).toBe(false);
	});
});

describe("BaseSourcePoller — backward compat", () => {
	beforeEach(() => {
		deltaCounter = 0;
	});

	it("pushDeltas() still works as synchronous single-shot", () => {
		const target = createPushTarget();
		const poller = new TestPoller(target);

		const deltas = [makeDelta(), makeDelta()];
		poller.legacyPush(deltas);

		expect(target.calls).toHaveLength(1);
		expect(target.calls[0]![0].deltas).toHaveLength(2);
	});

	it("pushDeltas() with empty array is a no-op", () => {
		const target = createPushTarget();
		const poller = new TestPoller(target);

		poller.legacyPush([]);
		expect(target.calls).toHaveLength(0);
	});
});

describe("isIngestTarget", () => {
	it("returns true for IngestTarget", () => {
		const target = createIngestTarget();
		expect(isIngestTarget(target)).toBe(true);
	});

	it("returns false for plain PushTarget", () => {
		const target = createPushTarget();
		expect(isIngestTarget(target)).toBe(false);
	});
});

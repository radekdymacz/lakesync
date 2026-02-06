import { HLC } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { createMockAdapter, createTestGateway, createTestHLC, makeDelta } from "./helpers";

describe("Single client sync", () => {
	it("pushes deltas, buffers, and flushes to adapter", async () => {
		const adapter = createMockAdapter();
		const gateway = createTestGateway(adapter);
		const { hlc, advance } = createTestHLC();

		// Push 10 deltas
		const deltas = [];
		for (let i = 0; i < 10; i++) {
			advance(100);
			deltas.push(
				makeDelta({
					rowId: `row-${i}`,
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Todo ${i}` }],
					deltaId: `delta-${i}`,
				}),
			);
		}

		const pushResult = gateway.handlePush({
			clientId: "client-a",
			deltas,
			lastSeenHlc: HLC.encode(0, 0),
		});

		expect(pushResult.ok).toBe(true);
		if (pushResult.ok) {
			expect(pushResult.value.accepted).toBe(10);
		}

		// Gateway buffer should have 10 entries
		expect(gateway.bufferStats.logSize).toBe(10);

		// Force flush
		const flushResult = await gateway.flush();
		expect(flushResult.ok).toBe(true);

		// Adapter should have received one flushed object
		expect(adapter.stored.size).toBe(1);

		// Buffer should be empty after flush
		expect(gateway.bufferStats.logSize).toBe(0);
	});

	it("flush with no adapter returns error", async () => {
		const gateway = createTestGateway();
		const { hlc, advance } = createTestHLC();

		advance(100);
		gateway.handlePush({
			clientId: "client-a",
			deltas: [
				makeDelta({
					hlc: hlc.now(),
					deltaId: "delta-no-adapter",
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		const result = await gateway.flush();
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("FLUSH_FAILED");
		}
	});

	it("pull returns deltas after a given HLC cursor", () => {
		const gateway = createTestGateway();
		const { hlc, advance } = createTestHLC();

		// Push two deltas at different times
		advance(100);
		const firstHlc = hlc.now();
		gateway.handlePush({
			clientId: "client-a",
			deltas: [
				makeDelta({
					hlc: firstHlc,
					deltaId: "pull-delta-1",
					columns: [{ column: "title", value: "First" }],
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		advance(100);
		gateway.handlePush({
			clientId: "client-a",
			deltas: [
				makeDelta({
					rowId: "row-2",
					hlc: hlc.now(),
					deltaId: "pull-delta-2",
					columns: [{ column: "title", value: "Second" }],
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// Pull since first HLC — should only get the second delta
		const pullResult = gateway.handlePull({
			clientId: "reader",
			sinceHlc: firstHlc,
			maxDeltas: 100,
		});

		expect(pullResult.ok).toBe(true);
		if (pullResult.ok) {
			expect(pullResult.value.deltas.length).toBe(1);
			expect(pullResult.value.deltas[0]?.deltaId).toBe("pull-delta-2");
		}
	});
});

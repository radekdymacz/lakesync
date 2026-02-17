import { describe, expect, it, vi } from "vitest";
import {
	MemoryUsageRecorder,
	type UsageAggregate,
	type UsageEvent,
	type UsageStore,
} from "../usage";

function makeEvent(overrides: Partial<UsageEvent> = {}): UsageEvent {
	return {
		gatewayId: "gw-1",
		eventType: "push_deltas",
		count: 1,
		timestamp: new Date("2026-01-15T10:30:00Z"),
		...overrides,
	};
}

describe("MemoryUsageRecorder", () => {
	describe("record", () => {
		it("buffers a single event", () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent());
			expect(recorder.size).toBe(1);
		});

		it("aggregates events with same gatewayId + eventType within the same minute", () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent({ count: 5 }));
			recorder.record(makeEvent({ count: 3 }));
			expect(recorder.size).toBe(1);
			expect(recorder.entries[0]!.count).toBe(8);
		});

		it("creates separate buckets for different event types", () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent({ eventType: "push_deltas" }));
			recorder.record(makeEvent({ eventType: "pull_deltas" }));
			expect(recorder.size).toBe(2);
		});

		it("creates separate buckets for different gateways", () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent({ gatewayId: "gw-1" }));
			recorder.record(makeEvent({ gatewayId: "gw-2" }));
			expect(recorder.size).toBe(2);
		});

		it("creates separate buckets for different minutes", () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent({ timestamp: new Date("2026-01-15T10:30:00Z") }));
			recorder.record(makeEvent({ timestamp: new Date("2026-01-15T10:31:00Z") }));
			expect(recorder.size).toBe(2);
		});

		it("aggregates events within the same minute (different seconds)", () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent({ count: 2, timestamp: new Date("2026-01-15T10:30:05Z") }));
			recorder.record(makeEvent({ count: 7, timestamp: new Date("2026-01-15T10:30:45Z") }));
			expect(recorder.size).toBe(1);
			expect(recorder.entries[0]!.count).toBe(9);
		});

		it("preserves orgId from events", () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent({ orgId: "org-1" }));
			expect(recorder.entries[0]!.orgId).toBe("org-1");
		});

		it("prefers most specific orgId when aggregating", () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent()); // no orgId
			recorder.record(makeEvent({ orgId: "org-1" }));
			expect(recorder.entries[0]!.orgId).toBe("org-1");
		});
	});

	describe("flush", () => {
		it("drains buffer on flush", async () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent({ count: 10 }));
			recorder.record(makeEvent({ count: 5 }));
			expect(recorder.size).toBe(1);

			await recorder.flush();
			expect(recorder.size).toBe(0);
		});

		it("calls store.recordAggregates with aggregated data", async () => {
			const recordAggregates = vi
				.fn<(aggs: UsageAggregate[]) => Promise<void>>()
				.mockResolvedValue(undefined);
			const store: UsageStore = { recordAggregates };

			const recorder = new MemoryUsageRecorder(store);
			recorder.record(makeEvent({ count: 5 }));
			recorder.record(makeEvent({ count: 3, eventType: "pull_deltas" }));

			await recorder.flush();

			expect(recordAggregates).toHaveBeenCalledTimes(1);
			const aggregates = recordAggregates.mock.calls[0]![0];
			expect(aggregates).toHaveLength(2);

			const pushAgg = aggregates.find((a) => a.eventType === "push_deltas");
			const pullAgg = aggregates.find((a) => a.eventType === "pull_deltas");
			expect(pushAgg!.count).toBe(5);
			expect(pushAgg!.gatewayId).toBe("gw-1");
			expect(pullAgg!.count).toBe(3);
		});

		it("does not call store when buffer is empty", async () => {
			const recordAggregates = vi
				.fn<(aggs: UsageAggregate[]) => Promise<void>>()
				.mockResolvedValue(undefined);
			const store: UsageStore = { recordAggregates };

			const recorder = new MemoryUsageRecorder(store);
			await recorder.flush();

			expect(recordAggregates).not.toHaveBeenCalled();
		});

		it("captures concurrent records during flush in new buffer", async () => {
			let resolveFlush: () => void;
			const flushPromise = new Promise<void>((resolve) => {
				resolveFlush = resolve;
			});

			const recordAggregates = vi
				.fn<(aggs: UsageAggregate[]) => Promise<void>>()
				.mockImplementation(async () => {
					await flushPromise;
				});
			const store: UsageStore = { recordAggregates };

			const recorder = new MemoryUsageRecorder(store);
			recorder.record(makeEvent({ count: 10 }));

			const flush = recorder.flush();

			// Record during flush — should go to new buffer
			recorder.record(makeEvent({ count: 7 }));

			resolveFlush!();
			await flush;

			// Original flush got 10, new buffer has 7
			const flushedAggs = recordAggregates.mock.calls[0]![0];
			expect(flushedAggs[0]!.count).toBe(10);
			expect(recorder.size).toBe(1);
			expect(recorder.entries[0]!.count).toBe(7);
		});

		it("works without a store (clears buffer only)", async () => {
			const recorder = new MemoryUsageRecorder();
			recorder.record(makeEvent({ count: 10 }));

			await recorder.flush();
			expect(recorder.size).toBe(0);
		});
	});

	describe("all event types", () => {
		it("records all supported event types", () => {
			const recorder = new MemoryUsageRecorder();
			const types = [
				"push_deltas",
				"pull_deltas",
				"flush_bytes",
				"flush_deltas",
				"storage_bytes",
				"api_call",
				"ws_connection",
				"action_executed",
			] as const;

			for (const eventType of types) {
				recorder.record(makeEvent({ eventType, count: 1 }));
			}

			expect(recorder.size).toBe(8);
		});
	});

	describe("windowStart", () => {
		it("sets windowStart to the start of the minute", async () => {
			const recordAggregates = vi
				.fn<(aggs: UsageAggregate[]) => Promise<void>>()
				.mockResolvedValue(undefined);
			const store: UsageStore = { recordAggregates };

			const recorder = new MemoryUsageRecorder(store);
			recorder.record(makeEvent({ timestamp: new Date("2026-01-15T10:30:45Z") }));

			await recorder.flush();

			const agg = recordAggregates.mock.calls[0]![0][0]!;
			expect(agg.windowStart.toISOString()).toBe("2026-01-15T10:30:00.000Z");
		});
	});
});

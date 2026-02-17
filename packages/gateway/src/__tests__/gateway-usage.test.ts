import { HLC, MemoryUsageRecorder } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { SyncGateway } from "../gateway";
import type { GatewayConfig } from "../types";
import { createMockLakeAdapter, makeDelta } from "./helpers";

function makeConfig(recorder: MemoryUsageRecorder): GatewayConfig {
	return {
		gatewayId: "gw-usage",
		maxBufferBytes: 1_048_576,
		maxBufferAgeMs: 30_000,
		flushFormat: "json",
		usageRecorder: recorder,
	};
}

describe("SyncGateway usage recording", () => {
	const hlcLow = HLC.encode(1_000_000, 0);

	it("records push_deltas on handlePush", () => {
		const recorder = new MemoryUsageRecorder();
		const gw = new SyncGateway(makeConfig(recorder));
		const delta = makeDelta({ hlc: hlcLow });

		gw.handlePush({ clientId: "client-a", deltas: [delta], lastSeenHlc: hlcLow });

		expect(recorder.size).toBe(1);
		const entry = recorder.entries[0]!;
		expect(entry.eventType).toBe("push_deltas");
		expect(entry.count).toBe(1);
		expect(entry.gatewayId).toBe("gw-usage");
	});

	it("records pull_deltas on handlePull", () => {
		const recorder = new MemoryUsageRecorder();
		const gw = new SyncGateway(makeConfig(recorder));
		const delta = makeDelta({ hlc: hlcLow });
		gw.handlePush({ clientId: "client-a", deltas: [delta], lastSeenHlc: hlcLow });

		// Clear push recording
		recorder.record({ gatewayId: "clear", eventType: "push_deltas", count: 0, timestamp: new Date() });

		gw.handlePull({ clientId: "client-a", sinceHlc: 0n as import("@lakesync/core").HLCTimestamp, maxDeltas: 100 });

		const pullEntries = recorder.entries.filter((e) => e.eventType === "pull_deltas");
		expect(pullEntries.length).toBe(1);
		expect(pullEntries[0]!.count).toBe(1);
	});

	it("does not record pull_deltas when no deltas returned", () => {
		const recorder = new MemoryUsageRecorder();
		const gw = new SyncGateway(makeConfig(recorder));

		gw.handlePull({ clientId: "client-a", sinceHlc: 0n as import("@lakesync/core").HLCTimestamp, maxDeltas: 100 });

		const pullEntries = recorder.entries.filter((e) => e.eventType === "pull_deltas");
		expect(pullEntries.length).toBe(0);
	});

	it("records flush_deltas and flush_bytes on flush", async () => {
		const recorder = new MemoryUsageRecorder();
		const adapter = createMockLakeAdapter();
		const gw = new SyncGateway({ ...makeConfig(recorder), adapter });
		const delta = makeDelta({ hlc: hlcLow });
		gw.handlePush({ clientId: "client-a", deltas: [delta], lastSeenHlc: hlcLow });

		await gw.flush();

		const flushDeltaEntries = recorder.entries.filter((e) => e.eventType === "flush_deltas");
		const flushByteEntries = recorder.entries.filter((e) => e.eventType === "flush_bytes");
		expect(flushDeltaEntries.length).toBe(1);
		expect(flushDeltaEntries[0]!.count).toBe(1);
		expect(flushByteEntries.length).toBe(1);
		expect(flushByteEntries[0]!.count).toBeGreaterThan(0);
	});

	it("does not record when no usageRecorder is configured", () => {
		const gw = new SyncGateway({
			gatewayId: "gw-no-usage",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			flushFormat: "json",
		});
		const delta = makeDelta({ hlc: hlcLow });

		// Should not throw
		const result = gw.handlePush({ clientId: "client-a", deltas: [delta], lastSeenHlc: hlcLow });
		expect(result.ok).toBe(true);
	});
});

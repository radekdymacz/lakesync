import { HLC } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { SyncGateway } from "../gateway";
import type { GatewayConfig } from "../types";
import { createMockLakeAdapter, makeDelta } from "./helpers";

function makeConfig(): GatewayConfig {
	return {
		gatewayId: "gw-usage",
		maxBufferBytes: 1_048_576,
		maxBufferAgeMs: 30_000,
		flushFormat: "json",
	};
}

describe("SyncGateway without usageRecorder", () => {
	const hlcLow = HLC.encode(1_000_000, 0);

	it("handlePush works without usageRecorder", () => {
		const gw = new SyncGateway(makeConfig());
		const delta = makeDelta({ hlc: hlcLow });

		const result = gw.handlePush({ clientId: "client-a", deltas: [delta], lastSeenHlc: hlcLow });

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.accepted).toBe(1);
		}
	});

	it("pullFromBuffer works without usageRecorder", () => {
		const gw = new SyncGateway(makeConfig());
		const delta = makeDelta({ hlc: hlcLow });
		gw.handlePush({ clientId: "client-a", deltas: [delta], lastSeenHlc: hlcLow });

		const result = gw.pullFromBuffer({
			clientId: "client-a",
			sinceHlc: 0n as import("@lakesync/core").HLCTimestamp,
			maxDeltas: 100,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(1);
		}
	});

	it("flush works without usageRecorder", async () => {
		const adapter = createMockLakeAdapter();
		const gw = new SyncGateway({ ...makeConfig(), adapter });
		const delta = makeDelta({ hlc: hlcLow });
		gw.handlePush({ clientId: "client-a", deltas: [delta], lastSeenHlc: hlcLow });

		const result = await gw.flush();
		expect(result.ok).toBe(true);
	});
});

import type { DatabaseAdapter } from "@lakesync/adapter";
import { type HLCTimestamp, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { SyncGateway } from "../gateway";

function createMockAdapter(): DatabaseAdapter {
	return {
		insertDeltas: vi.fn().mockResolvedValue(Ok(undefined)),
		queryDeltasSince: vi.fn().mockResolvedValue(Ok([])),
		getLatestState: vi.fn().mockResolvedValue(Ok(null)),
		ensureSchema: vi.fn().mockResolvedValue(Ok(undefined)),
		close: vi.fn().mockResolvedValue(undefined),
	};
}

describe("SyncGateway source management", () => {
	it("listSources returns empty array initially", () => {
		const gw = new SyncGateway({
			gatewayId: "test",
			maxBufferBytes: 4_000_000,
			maxBufferAgeMs: 30_000,
		});
		expect(gw.listSources()).toEqual([]);
	});

	it("registerSource adds a named source", () => {
		const gw = new SyncGateway({
			gatewayId: "test",
			maxBufferBytes: 4_000_000,
			maxBufferAgeMs: 30_000,
		});
		const adapter = createMockAdapter();
		gw.registerSource("pg-source", adapter);
		expect(gw.listSources()).toEqual(["pg-source"]);
	});

	it("unregisterSource removes a named source", () => {
		const gw = new SyncGateway({
			gatewayId: "test",
			maxBufferBytes: 4_000_000,
			maxBufferAgeMs: 30_000,
		});
		const adapter = createMockAdapter();
		gw.registerSource("pg-source", adapter);
		gw.unregisterSource("pg-source");
		expect(gw.listSources()).toEqual([]);
	});

	it("registerSource overwrites existing source", () => {
		const gw = new SyncGateway({
			gatewayId: "test",
			maxBufferBytes: 4_000_000,
			maxBufferAgeMs: 30_000,
		});
		const adapter1 = createMockAdapter();
		const adapter2 = createMockAdapter();
		gw.registerSource("source", adapter1);
		gw.registerSource("source", adapter2);
		expect(gw.listSources()).toEqual(["source"]);
	});

	it("pull from registered source succeeds", async () => {
		const gw = new SyncGateway({
			gatewayId: "test",
			maxBufferBytes: 4_000_000,
			maxBufferAgeMs: 30_000,
		});
		const adapter = createMockAdapter();
		gw.registerSource("my-src", adapter);

		const result = await gw.handlePull({
			clientId: "c1",
			sinceHlc: 0n as HLCTimestamp,
			maxDeltas: 100,
			source: "my-src",
		});
		expect(result.ok).toBe(true);
	});

	it("pull from unregistered source returns error", async () => {
		const gw = new SyncGateway({
			gatewayId: "test",
			maxBufferBytes: 4_000_000,
			maxBufferAgeMs: 30_000,
		});

		const result = await gw.handlePull({
			clientId: "c1",
			sinceHlc: 0n as HLCTimestamp,
			maxDeltas: 100,
			source: "missing",
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("ADAPTER_NOT_FOUND");
		}
	});
});

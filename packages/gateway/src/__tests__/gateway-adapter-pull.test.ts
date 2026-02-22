import {
	AdapterError,
	AdapterNotFoundError,
	type DatabaseAdapter,
	Err,
	HLC,
	Ok,
	type RowDelta,
	type SyncRulesContext,
} from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { SyncGateway } from "../gateway";
import type { GatewayConfig } from "../types";
import { makeDelta } from "./helpers";

/** Create a mock DatabaseAdapter that returns the given deltas from queryDeltasSince. */
function mockDatabaseAdapter(
	deltas: RowDelta[],
	overrides?: Partial<DatabaseAdapter>,
): DatabaseAdapter {
	return {
		insertDeltas: () => Promise.resolve(Ok(undefined)),
		queryDeltasSince: () => Promise.resolve(Ok(deltas)),
		getLatestState: () => Promise.resolve(Ok(null)),
		ensureSchema: () => Promise.resolve(Ok(undefined)),
		close: () => Promise.resolve(),
		...overrides,
	};
}

function makeConfig(sourceAdapters?: Record<string, DatabaseAdapter>): GatewayConfig {
	return {
		gatewayId: "gw-test",
		maxBufferBytes: 10 * 1024 * 1024,
		maxBufferAgeMs: 60_000,
		flushFormat: "json",
		sourceAdapters,
	};
}

describe("pullFromAdapter with source adapter", () => {
	it("pulls deltas from a named source adapter", async () => {
		const hlc1 = HLC.encode(1_000_000, 0);
		const hlc2 = HLC.encode(2_000_000, 0);
		const adapterDeltas = [
			makeDelta({ hlc: hlc1, rowId: "row-1", deltaId: "d1" }),
			makeDelta({ hlc: hlc2, rowId: "row-2", deltaId: "d2" }),
		];

		const adapter = mockDatabaseAdapter(adapterDeltas);
		const gw = new SyncGateway(makeConfig({ postgres: adapter }));

		const result = await gw.pullFromAdapter("postgres", {
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(2);
			expect(result.value.hasMore).toBe(false);
		}
	});

	it("filters adapter results with sync rules", async () => {
		const adapterDeltas = [
			makeDelta({
				hlc: HLC.encode(1_000_000, 0),
				rowId: "row-1",
				deltaId: "d1",
				columns: [
					{ column: "title", value: "Item 1" },
					{ column: "user_id", value: "user-a" },
				],
			}),
			makeDelta({
				hlc: HLC.encode(2_000_000, 0),
				rowId: "row-2",
				deltaId: "d2",
				columns: [
					{ column: "title", value: "Item 2" },
					{ column: "user_id", value: "user-b" },
				],
			}),
			makeDelta({
				hlc: HLC.encode(3_000_000, 0),
				rowId: "row-3",
				deltaId: "d3",
				columns: [
					{ column: "title", value: "Item 3" },
					{ column: "user_id", value: "user-a" },
				],
			}),
		];

		const adapter = mockDatabaseAdapter(adapterDeltas);
		const gw = new SyncGateway(makeConfig({ postgres: adapter }));

		const context: SyncRulesContext = {
			claims: { sub: "user-a" },
			rules: {
				version: 1,
				buckets: [
					{
						name: "user-data",
						tables: [],
						filters: [{ column: "user_id", op: "eq", value: "jwt:sub" }],
					},
				],
			},
		};

		const result = await gw.pullFromAdapter(
			"postgres",
			{
				clientId: "client-b",
				sinceHlc: HLC.encode(0, 0),
				maxDeltas: 100,
			},
			context,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(2);
			for (const d of result.value.deltas) {
				const userCol = d.columns.find((c) => c.column === "user_id");
				expect(userCol?.value).toBe("user-a");
			}
		}
	});

	it("uses buffer path when pullFromBuffer is called", () => {
		const gw = new SyncGateway(makeConfig());

		gw.handlePush({
			clientId: "client-a",
			deltas: [makeDelta({ hlc: HLC.encode(1_000_000, 0), deltaId: "d1" })],
			lastSeenHlc: HLC.encode(0, 0),
		});

		const result = gw.pullFromBuffer({
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(1);
		}
	});

	it("returns AdapterNotFoundError for unknown source name", async () => {
		const gw = new SyncGateway(makeConfig({ postgres: mockDatabaseAdapter([]) }));

		const result = await gw.pullFromAdapter("unknown-db", {
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(AdapterNotFoundError);
			expect(result.error.code).toBe("ADAPTER_NOT_FOUND");
			expect(result.error.message).toContain("unknown-db");
		}
	});

	it("returns AdapterNotFoundError when no sourceAdapters configured", async () => {
		const gw = new SyncGateway(makeConfig());

		const result = await gw.pullFromAdapter("postgres", {
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(AdapterNotFoundError);
		}
	});

	it("returns empty deltas when adapter has no data", async () => {
		const adapter = mockDatabaseAdapter([]);
		const gw = new SyncGateway(makeConfig({ postgres: adapter }));

		const result = await gw.pullFromAdapter("postgres", {
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(0);
			expect(result.value.hasMore).toBe(false);
		}
	});

	it("propagates adapter error as Result Err", async () => {
		const failingAdapter = mockDatabaseAdapter([], {
			queryDeltasSince: () => Promise.resolve(Err(new AdapterError("Connection refused"))),
		});
		const gw = new SyncGateway(makeConfig({ postgres: failingAdapter }));

		const result = await gw.pullFromAdapter("postgres", {
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(AdapterError);
			expect(result.error.message).toBe("Connection refused");
		}
	});

	it("respects maxDeltas with hasMore=true for pagination", async () => {
		const adapterDeltas: RowDelta[] = [];
		for (let i = 0; i < 10; i++) {
			adapterDeltas.push(
				makeDelta({
					hlc: HLC.encode(1_000_000 + i * 1000, 0),
					rowId: `row-${i}`,
					deltaId: `d-${i}`,
				}),
			);
		}

		const adapter = mockDatabaseAdapter(adapterDeltas);
		const gw = new SyncGateway(makeConfig({ postgres: adapter }));

		const result = await gw.pullFromAdapter("postgres", {
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 5,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(5);
			expect(result.value.hasMore).toBe(true);
		}
	});

	it("returns hasMore=false when deltas fit within maxDeltas", async () => {
		const adapterDeltas = [
			makeDelta({ hlc: HLC.encode(1_000_000, 0), rowId: "row-1", deltaId: "d1" }),
			makeDelta({ hlc: HLC.encode(2_000_000, 0), rowId: "row-2", deltaId: "d2" }),
		];

		const adapter = mockDatabaseAdapter(adapterDeltas);
		const gw = new SyncGateway(makeConfig({ postgres: adapter }));

		const result = await gw.pullFromAdapter("postgres", {
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 5,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(2);
			expect(result.value.hasMore).toBe(false);
		}
	});

	it("buffer pull still works alongside source adapters config", () => {
		const adapter = mockDatabaseAdapter([]);
		const gw = new SyncGateway(makeConfig({ postgres: adapter }));

		// Push into buffer
		gw.handlePush({
			clientId: "client-a",
			deltas: [
				makeDelta({ hlc: HLC.encode(1_000_000, 0), deltaId: "d-buf-1" }),
				makeDelta({ hlc: HLC.encode(2_000_000, 0), rowId: "row-2", deltaId: "d-buf-2" }),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// Pull without source — should use buffer
		const result = gw.pullFromBuffer({
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(2);
		}
	});
});

import type { DeltaOp, HLCTimestamp, RowDelta, SyncRulesContext } from "@lakesync/core";
import { HLC } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { SyncGateway } from "../gateway";
import type { GatewayConfig } from "../types";

function makeDelta(opts: Partial<RowDelta> & { hlc: HLCTimestamp }): RowDelta {
	return {
		op: (opts.op ?? "INSERT") as DeltaOp,
		table: opts.table ?? "todos",
		rowId: opts.rowId ?? `row-${Math.random().toString(36).slice(2)}`,
		clientId: opts.clientId ?? "client-a",
		columns: opts.columns ?? [{ column: "title", value: "Test" }],
		hlc: opts.hlc,
		deltaId: opts.deltaId ?? `delta-${Math.random().toString(36).slice(2)}`,
	};
}

function makeConfig(): GatewayConfig {
	return {
		gatewayId: "gw-test",
		maxBufferBytes: 10 * 1024 * 1024,
		maxBufferAgeMs: 60_000,
		flushFormat: "json",
	};
}

describe("handlePull with SyncRulesContext", () => {
	it("returns all deltas without context (backward compat)", () => {
		const gw = new SyncGateway(makeConfig());
		const hlc1 = HLC.encode(1_000_000, 0);

		gw.handlePush({
			clientId: "client-a",
			deltas: [
				makeDelta({
					hlc: hlc1,
					columns: [{ column: "user_id", value: "user-1" }],
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		const result = gw.handlePull({
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(1);
		}
	});

	it("filters deltas by user claim", () => {
		const gw = new SyncGateway(makeConfig());

		// Push deltas for two users
		for (let i = 1; i <= 5; i++) {
			gw.handlePush({
				clientId: `client-${i % 2 === 0 ? "a" : "b"}`,
				deltas: [
					makeDelta({
						hlc: HLC.encode(1_000_000 + i * 1000, 0),
						rowId: `row-${i}`,
						columns: [
							{ column: "title", value: `Item ${i}` },
							{ column: "user_id", value: i % 2 === 0 ? "user-a" : "user-b" },
						],
					}),
				],
				lastSeenHlc: HLC.encode(0, 0),
			});
		}

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

		const result = gw.handlePull(
			{ clientId: "client-a", sinceHlc: HLC.encode(0, 0), maxDeltas: 100 },
			context,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			// user-a has deltas at i=2, i=4
			expect(result.value.deltas).toHaveLength(2);
			for (const d of result.value.deltas) {
				const userCol = d.columns.find((c) => c.column === "user_id");
				expect(userCol?.value).toBe("user-a");
			}
		}
	});

	it("returns hasMore=true when filtered results fill maxDeltas", () => {
		const gw = new SyncGateway(makeConfig());

		// Push many deltas for user-a
		for (let i = 0; i < 20; i++) {
			gw.handlePush({
				clientId: "client-a",
				deltas: [
					makeDelta({
						hlc: HLC.encode(1_000_000 + i * 1000, 0),
						rowId: `row-${i}`,
						columns: [
							{ column: "title", value: `Item ${i}` },
							{ column: "user_id", value: "user-a" },
						],
					}),
				],
				lastSeenHlc: HLC.encode(0, 0),
			});
		}

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

		const result = gw.handlePull(
			{ clientId: "client-x", sinceHlc: HLC.encode(0, 0), maxDeltas: 5 },
			context,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(5);
			expect(result.value.hasMore).toBe(true);
		}
	});

	it("returns hasMore=false when buffer is exhausted", () => {
		const gw = new SyncGateway(makeConfig());

		gw.handlePush({
			clientId: "client-a",
			deltas: [
				makeDelta({
					hlc: HLC.encode(1_000_000, 0),
					columns: [{ column: "user_id", value: "user-a" }],
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

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

		const result = gw.handlePull(
			{ clientId: "client-x", sinceHlc: HLC.encode(0, 0), maxDeltas: 100 },
			context,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(1);
			expect(result.value.hasMore).toBe(false);
		}
	});

	it("filters by table scope in bucket", () => {
		const gw = new SyncGateway(makeConfig());

		gw.handlePush({
			clientId: "client-a",
			deltas: [
				makeDelta({ hlc: HLC.encode(1_000_000, 0), table: "todos" }),
				makeDelta({ hlc: HLC.encode(2_000_000, 0), table: "notes", rowId: "row-n1" }),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		const context: SyncRulesContext = {
			claims: {},
			rules: {
				version: 1,
				buckets: [
					{
						name: "todos-only",
						tables: ["todos"],
						filters: [],
					},
				],
			},
		};

		const result = gw.handlePull(
			{ clientId: "client-x", sinceHlc: HLC.encode(0, 0), maxDeltas: 100 },
			context,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(1);
			expect(result.value.deltas[0]!.table).toBe("todos");
		}
	});

	it("handles empty results when no deltas match", () => {
		const gw = new SyncGateway(makeConfig());

		gw.handlePush({
			clientId: "client-a",
			deltas: [
				makeDelta({
					hlc: HLC.encode(1_000_000, 0),
					columns: [{ column: "user_id", value: "user-a" }],
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		const context: SyncRulesContext = {
			claims: { sub: "user-z" },
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

		const result = gw.handlePull(
			{ clientId: "client-x", sinceHlc: HLC.encode(0, 0), maxDeltas: 100 },
			context,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(0);
			expect(result.value.hasMore).toBe(false);
		}
	});
});

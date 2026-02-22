import type { DatabaseAdapter } from "@lakesync/adapter";
import {
	AdapterError,
	Err,
	HLC,
	type HLCTimestamp,
	Ok,
	type RowDelta,
	type SyncRulesContext,
} from "@lakesync/core";
import { type GatewayConfig, SyncGateway } from "@lakesync/gateway";
import { describe, expect, it, vi } from "vitest";
import { makeDelta } from "./helpers";

/**
 * Create an in-memory mock DatabaseAdapter for testing adapter-sourced pulls.
 */
function createMockDatabaseAdapter(deltas: RowDelta[] = []): DatabaseAdapter {
	return {
		insertDeltas: vi.fn(async () => Ok(undefined)),
		queryDeltasSince: vi.fn(async (hlc: HLCTimestamp) => {
			const filtered = deltas.filter((d) => d.hlc > hlc);
			return Ok(filtered);
		}),
		getLatestState: vi.fn(async () => Ok(null)),
		ensureSchema: vi.fn(async () => Ok(undefined)),
		close: vi.fn(async () => {}),
	};
}

function createGatewayWithSource(
	sourceAdapters: Record<string, DatabaseAdapter>,
	overrides?: Partial<GatewayConfig>,
): SyncGateway {
	return new SyncGateway({
		gatewayId: "test-gateway",
		maxBufferBytes: 100 * 1024 * 1024,
		maxBufferAgeMs: 60_000,
		flushFormat: "json",
		sourceAdapters,
		...overrides,
	});
}

describe("Adapter-sourced pull — integration", () => {
	it("pulls deltas from a named source adapter", async () => {
		const hlc = new HLC(() => 1_000_000);
		const adapterDeltas = [
			makeDelta({
				hlc: hlc.now(),
				table: "logs",
				rowId: "log-1",
				clientId: "bigquery",
				columns: [
					{ column: "level", value: "error" },
					{ column: "message", value: "Connection timeout" },
				],
				deltaId: "adapter-delta-1",
			}),
			makeDelta({
				hlc: hlc.now(),
				table: "logs",
				rowId: "log-2",
				clientId: "bigquery",
				columns: [
					{ column: "level", value: "warn" },
					{ column: "message", value: "High latency" },
				],
				deltaId: "adapter-delta-2",
			}),
		];

		const mockAdapter = createMockDatabaseAdapter(adapterDeltas);
		const gateway = createGatewayWithSource({ bigquery: mockAdapter });

		const result = await gateway.pullFromAdapter("bigquery", {
			clientId: "consumer-1",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(2);
			expect(result.value.deltas[0]!.table).toBe("logs");
			expect(result.value.hasMore).toBe(false);
		}
	});

	it("applies sync rules to adapter-sourced deltas", async () => {
		const hlc = new HLC(() => 1_000_000);
		const adapterDeltas = [
			makeDelta({
				hlc: hlc.now(),
				table: "logs",
				rowId: "log-1",
				clientId: "bigquery",
				columns: [
					{ column: "level", value: "error" },
					{ column: "service", value: "api" },
				],
				deltaId: "adapter-rules-1",
			}),
			makeDelta({
				hlc: hlc.now(),
				table: "logs",
				rowId: "log-2",
				clientId: "bigquery",
				columns: [
					{ column: "level", value: "info" },
					{ column: "service", value: "api" },
				],
				deltaId: "adapter-rules-2",
			}),
			makeDelta({
				hlc: hlc.now(),
				table: "logs",
				rowId: "log-3",
				clientId: "bigquery",
				columns: [
					{ column: "level", value: "error" },
					{ column: "service", value: "worker" },
				],
				deltaId: "adapter-rules-3",
			}),
		];

		const mockAdapter = createMockDatabaseAdapter(adapterDeltas);
		const gateway = createGatewayWithSource({ bigquery: mockAdapter });

		const context: SyncRulesContext = {
			claims: {},
			rules: {
				version: 1,
				buckets: [
					{
						name: "errors-only",
						tables: ["logs"],
						filters: [{ column: "level", op: "eq", value: "error" }],
					},
				],
			},
		};

		const result = await gateway.pullFromAdapter(
			"bigquery",
			{
				clientId: "consumer-1",
				sinceHlc: HLC.encode(0, 0),
				maxDeltas: 100,
			},
			context,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(2);
			for (const d of result.value.deltas) {
				const levelCol = d.columns.find((c) => c.column === "level");
				expect(levelCol?.value).toBe("error");
			}
		}
	});

	it("uses extended operators (gt/lt) to filter adapter data", async () => {
		let wall = 1_000_000;
		const hlc = new HLC(() => wall++);
		const adapterDeltas = [
			makeDelta({
				hlc: hlc.now(),
				table: "metrics",
				rowId: "m-1",
				clientId: "postgres",
				columns: [
					{ column: "cpu", value: "25" },
					{ column: "host", value: "server-1" },
				],
				deltaId: "metric-1",
			}),
			makeDelta({
				hlc: hlc.now(),
				table: "metrics",
				rowId: "m-2",
				clientId: "postgres",
				columns: [
					{ column: "cpu", value: "85" },
					{ column: "host", value: "server-2" },
				],
				deltaId: "metric-2",
			}),
			makeDelta({
				hlc: hlc.now(),
				table: "metrics",
				rowId: "m-3",
				clientId: "postgres",
				columns: [
					{ column: "cpu", value: "50" },
					{ column: "host", value: "server-3" },
				],
				deltaId: "metric-3",
			}),
		];

		const mockAdapter = createMockDatabaseAdapter(adapterDeltas);
		const gateway = createGatewayWithSource({ postgres: mockAdapter });

		const context: SyncRulesContext = {
			claims: {},
			rules: {
				version: 1,
				buckets: [
					{
						name: "high-cpu",
						tables: ["metrics"],
						filters: [{ column: "cpu", op: "gt", value: "50" }],
					},
				],
			},
		};

		const result = await gateway.pullFromAdapter(
			"postgres",
			{
				clientId: "dashboard",
				sinceHlc: HLC.encode(0, 0),
				maxDeltas: 100,
			},
			context,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(1);
			expect(result.value.deltas[0]!.rowId).toBe("m-2");
		}
	});

	it("buffer pull still works alongside source adapters", async () => {
		const hlc = new HLC(() => 1_000_000);
		const adapterDeltas = [
			makeDelta({
				hlc: hlc.now(),
				table: "logs",
				rowId: "log-1",
				clientId: "bigquery",
				columns: [{ column: "level", value: "error" }],
				deltaId: "adapter-coexist-1",
			}),
		];

		const mockAdapter = createMockDatabaseAdapter(adapterDeltas);
		const gateway = createGatewayWithSource({ bigquery: mockAdapter });

		// Push data to the buffer
		gateway.handlePush({
			clientId: "client-a",
			deltas: [
				makeDelta({
					hlc: HLC.encode(2_000_000, 0),
					table: "todos",
					rowId: "todo-1",
					columns: [{ column: "title", value: "Buffer item" }],
					deltaId: "buffer-delta-1",
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// Pull from buffer (no source) — returns buffer data
		const bufferPull = gateway.pullFromBuffer({
			clientId: "client-b",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});
		expect(bufferPull.ok).toBe(true);
		if (bufferPull.ok) {
			expect(bufferPull.value.deltas).toHaveLength(1);
			expect(bufferPull.value.deltas[0]!.table).toBe("todos");
		}

		// Pull from adapter source — returns adapter data
		const adapterPull = await gateway.pullFromAdapter("bigquery", {
			clientId: "consumer-1",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});
		expect(adapterPull.ok).toBe(true);
		if (adapterPull.ok) {
			expect(adapterPull.value.deltas).toHaveLength(1);
			expect(adapterPull.value.deltas[0]!.table).toBe("logs");
		}
	});

	it("unknown source returns AdapterNotFoundError", async () => {
		const gateway = createGatewayWithSource({});

		const result = await gateway.pullFromAdapter("nonexistent", {
			clientId: "consumer-1",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("ADAPTER_NOT_FOUND");
		}
	});

	it("adapter error propagates as Result Err", async () => {
		const failingAdapter: DatabaseAdapter = {
			insertDeltas: vi.fn(async () => Ok(undefined)),
			queryDeltasSince: vi.fn(async () => Err(new AdapterError("Connection refused"))),
			getLatestState: vi.fn(async () => Ok(null)),
			ensureSchema: vi.fn(async () => Ok(undefined)),
			close: vi.fn(async () => {}),
		};

		const gateway = createGatewayWithSource({ broken: failingAdapter });

		const result = await gateway.pullFromAdapter("broken", {
			clientId: "consumer-1",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("ADAPTER_ERROR");
			expect(result.error.message).toBe("Connection refused");
		}
	});

	it("pagination: maxDeltas respected with adapter source", async () => {
		let wall = 1_000_000;
		const hlc = new HLC(() => wall++);
		const adapterDeltas: RowDelta[] = [];
		for (let i = 0; i < 10; i++) {
			adapterDeltas.push(
				makeDelta({
					hlc: hlc.now(),
					table: "events",
					rowId: `event-${i}`,
					clientId: "postgres",
					columns: [{ column: "type", value: "click" }],
					deltaId: `paginate-${i}`,
				}),
			);
		}

		const mockAdapter = createMockDatabaseAdapter(adapterDeltas);
		const gateway = createGatewayWithSource({ postgres: mockAdapter });

		const result = await gateway.pullFromAdapter("postgres", {
			clientId: "consumer-1",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 5,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(5);
			expect(result.value.hasMore).toBe(true);
		}
	});
});

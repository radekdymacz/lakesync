import type {
	AdapterError,
	DatabaseAdapter,
	DeltaOp,
	HLCTimestamp,
	Materialisable,
	Result,
	RowDelta,
	TableSchema,
} from "@lakesync/core";
import { Err, HLC, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { DeltaBuffer } from "../buffer";
import { FlushCoordinator } from "../flush-coordinator";
import { MemoryFlushQueue } from "../flush-queue";
import { SyncGateway } from "../gateway";
import { collectMaterialisers } from "../materialisation-processor";
import type { GatewayConfig } from "../types";

/** Helper to build a RowDelta with sensible defaults. */
function makeDelta(opts: Partial<RowDelta> & { hlc: HLCTimestamp }): RowDelta {
	return {
		op: (opts.op ?? "UPDATE") as DeltaOp,
		table: opts.table ?? "todos",
		rowId: opts.rowId ?? "row-1",
		clientId: opts.clientId ?? "client-a",
		columns: opts.columns ?? [{ column: "title", value: "Test" }],
		hlc: opts.hlc,
		deltaId: opts.deltaId ?? `delta-${Math.random().toString(36).slice(2)}`,
	};
}

/** In-memory mock DB adapter that records calls. */
function createMockDbAdapter(): DatabaseAdapter & { calls: RowDelta[][] } {
	const calls: RowDelta[][] = [];
	return {
		calls,
		async insertDeltas(deltas) {
			calls.push([...deltas]);
			return Ok(undefined);
		},
		async queryDeltasSince() {
			return Ok([]);
		},
		async getLatestState() {
			return Ok(null);
		},
		async ensureSchema() {
			return Ok(undefined);
		},
		async close() {},
	};
}

/** Mock DatabaseAdapter that always fails on insertDeltas. */
function createFailingDbAdapter(): DatabaseAdapter {
	return {
		async insertDeltas(): Promise<Result<void, AdapterError>> {
			return Err({ code: "ADAPTER_ERROR", message: "Simulated DB write failure" } as AdapterError);
		},
		async queryDeltasSince() {
			return Ok([]);
		},
		async getLatestState() {
			return Ok(null);
		},
		async ensureSchema() {
			return Ok(undefined);
		},
		async close() {},
	};
}

/** Build a MemoryFlushQueue from an adapter and optional extras. */
function buildFlushQueueHelper(
	adapter: unknown,
	extras?: ReadonlyArray<Materialisable>,
	onFailure?: (table: string, deltaCount: number, error: Error) => void,
): MemoryFlushQueue | undefined {
	const targets = collectMaterialisers(adapter, extras);
	if (targets.length === 0) return undefined;
	return new MemoryFlushQueue(targets, onFailure);
}

const hlcLow = HLC.encode(1_000_000, 0);

const todoSchemas: TableSchema[] = [
	{ table: "todos", columns: [{ name: "title", type: "string" }] },
];

/** Drain the microtask queue so fire-and-forget publishToQueue completes. */
const flushMicrotasks = () => new Promise<void>((r) => setTimeout(r, 0));

describe("FlushCoordinator", () => {
	it("returns flushed entries on success", async () => {
		const dbAdapter = createMockDbAdapter();
		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-test" },
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.entries).toHaveLength(1);
		}
		expect(buffer.logSize).toBe(0);
	});

	it("returns empty entries when buffer is empty", async () => {
		const dbAdapter = createMockDbAdapter();
		const buffer = new DeltaBuffer();

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-empty" },
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.entries).toHaveLength(0);
		}
	});

	it("returns error when no adapter configured", async () => {
		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, null, {
			config: { gatewayId: "gw-no-adapter" },
		});

		expect(result.ok).toBe(false);
	});

	it("returns error when flush already in progress", async () => {
		const slowAdapter: DatabaseAdapter = {
			...createMockDbAdapter(),
			async insertDeltas() {
				await new Promise((r) => setTimeout(r, 100));
				return Ok(undefined);
			},
		};
		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const flush1 = coordinator.flush(buffer, slowAdapter, {
			config: { gatewayId: "gw-concurrent" },
		});
		const flush2 = await coordinator.flush(buffer, slowAdapter, {
			config: { gatewayId: "gw-concurrent" },
		});

		expect(flush2.ok).toBe(false);
		if (!flush2.ok) {
			expect(flush2.error.message).toContain("already in progress");
		}

		await flush1;
	});

	it("restores entries on flush failure", async () => {
		const dbAdapter = createFailingDbAdapter();
		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-fail" },
		});

		expect(result.ok).toBe(false);
		expect(buffer.logSize).toBe(1);
	});

	it("flushTable returns entries for single table", async () => {
		const dbAdapter = createMockDbAdapter();
		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow, table: "todos", deltaId: "d1" }));
		buffer.append(
			makeDelta({ hlc: HLC.encode(2_000_000, 0), table: "users", rowId: "row-2", deltaId: "d2" }),
		);

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flushTable("todos", buffer, dbAdapter, {
			config: { gatewayId: "gw-table" },
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.entries).toHaveLength(1);
			expect(result.value.entries[0]!.table).toBe("todos");
		}
		// users table should remain
		expect(buffer.logSize).toBe(1);
	});
});

describe("SyncGateway — materialisation via FlushQueue", () => {
	function makeGatewayConfig(
		adapter: DatabaseAdapter,
		opts?: {
			schemas?: TableSchema[];
			materialisers?: Materialisable[];
			flushQueue?: MemoryFlushQueue;
		},
	): GatewayConfig {
		return {
			gatewayId: "gw-mat",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			adapter,
			schemas: opts?.schemas ?? todoSchemas,
			flushQueue: opts?.flushQueue ?? buildFlushQueueHelper(adapter, opts?.materialisers),
		};
	}

	it("calls materialise on the adapter when it is materialisable and flush succeeds", async () => {
		const materialiseCalls: Array<{ deltas: RowDelta[]; schemas: TableSchema[] }> = [];
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async (deltas: RowDelta[], schemas: ReadonlyArray<TableSchema>) => {
				materialiseCalls.push({ deltas: [...deltas], schemas: [...schemas] });
				return Ok(undefined);
			},
		};

		const gw = new SyncGateway(makeGatewayConfig(dbAdapter));
		gw.handlePush({ clientId: "c1", deltas: [makeDelta({ hlc: hlcLow })], lastSeenHlc: hlcLow });

		const result = await gw.flush();
		await flushMicrotasks();

		expect(result.ok).toBe(true);
		expect(materialiseCalls).toHaveLength(1);
		expect(materialiseCalls[0]!.deltas).toHaveLength(1);
	});

	it("calls additional materialisers alongside the adapter", async () => {
		const adapterCalls: Array<{ deltas: RowDelta[] }> = [];
		const extraCalls: Array<{ deltas: RowDelta[] }> = [];

		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async (deltas: RowDelta[]) => {
				adapterCalls.push({ deltas: [...deltas] });
				return Ok(undefined);
			},
		};

		const extraMaterialiser: Materialisable = {
			materialise: async (deltas: RowDelta[]) => {
				extraCalls.push({ deltas: [...deltas] });
				return Ok(undefined);
			},
		};

		const gw = new SyncGateway(
			makeGatewayConfig(dbAdapter, { materialisers: [extraMaterialiser] }),
		);
		gw.handlePush({ clientId: "c1", deltas: [makeDelta({ hlc: hlcLow })], lastSeenHlc: hlcLow });

		const result = await gw.flush();
		await flushMicrotasks();

		expect(result.ok).toBe(true);
		expect(adapterCalls).toHaveLength(1);
		expect(extraCalls).toHaveLength(1);
	});

	it("does not call materialise when no schemas provided", async () => {
		const materialise = vi.fn().mockResolvedValue(Ok(undefined));
		const dbAdapter = { ...createMockDbAdapter(), materialise };

		const gw = new SyncGateway(makeGatewayConfig(dbAdapter, { schemas: [] }));
		gw.handlePush({ clientId: "c1", deltas: [makeDelta({ hlc: hlcLow })], lastSeenHlc: hlcLow });

		const result = await gw.flush();

		expect(result.ok).toBe(true);
		expect(materialise).not.toHaveBeenCalled();
	});

	it("flush still succeeds when materialise returns Err", async () => {
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async () => {
				return Err({ code: "ADAPTER_ERROR", message: "materialise exploded" } as AdapterError);
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const gw = new SyncGateway(makeGatewayConfig(dbAdapter));
		gw.handlePush({ clientId: "c1", deltas: [makeDelta({ hlc: hlcLow })], lastSeenHlc: hlcLow });

		const result = await gw.flush();
		await flushMicrotasks();

		expect(result.ok).toBe(true);
		expect(warnSpy).toHaveBeenCalled();
		warnSpy.mockRestore();
	});

	it("flush still succeeds when materialise throws", async () => {
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async () => {
				throw new Error("kaboom");
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const gw = new SyncGateway(makeGatewayConfig(dbAdapter));
		gw.handlePush({ clientId: "c1", deltas: [makeDelta({ hlc: hlcLow })], lastSeenHlc: hlcLow });

		const result = await gw.flush();
		await flushMicrotasks();

		expect(result.ok).toBe(true);
		expect(warnSpy).toHaveBeenCalled();
		warnSpy.mockRestore();
	});

	it("does NOT call materialisers when flush fails", async () => {
		const materialise = vi.fn().mockResolvedValue(Ok(undefined));
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createFailingDbAdapter(),
			materialise,
		};

		const gw = new SyncGateway(makeGatewayConfig(dbAdapter));
		gw.handlePush({ clientId: "c1", deltas: [makeDelta({ hlc: hlcLow })], lastSeenHlc: hlcLow });

		const result = await gw.flush();

		expect(result.ok).toBe(false);
		expect(materialise).not.toHaveBeenCalled();
	});

	it("invokes onFailure callback when materialise fails", async () => {
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async () => {
				return Err({ code: "ADAPTER_ERROR", message: "mat failed" } as AdapterError);
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const onFailure = vi.fn();

		const gw = new SyncGateway({
			...makeGatewayConfig(dbAdapter, {
				flushQueue: buildFlushQueueHelper(dbAdapter, undefined, onFailure),
			}),
		});
		gw.handlePush({
			clientId: "c1",
			deltas: [makeDelta({ hlc: hlcLow, table: "todos" })],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();
		await flushMicrotasks();

		expect(result.ok).toBe(true);
		expect(onFailure).toHaveBeenCalledTimes(1);
		expect(onFailure).toHaveBeenCalledWith("todos", 1, expect.any(Error));
		expect(onFailure.mock.calls[0]![2].message).toBe("mat failed");
		warnSpy.mockRestore();
	});

	it("calls materialisers on flushTable success", async () => {
		const materialiseCalls: RowDelta[][] = [];
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async (deltas: RowDelta[]) => {
				materialiseCalls.push([...deltas]);
				return Ok(undefined);
			},
		};

		const gw = new SyncGateway(makeGatewayConfig(dbAdapter));
		gw.handlePush({
			clientId: "c1",
			deltas: [makeDelta({ hlc: hlcLow, table: "todos" })],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flushTable("todos");
		await flushMicrotasks();

		expect(result.ok).toBe(true);
		expect(materialiseCalls).toHaveLength(1);
	});
});

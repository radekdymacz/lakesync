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
import { collectMaterialisers } from "../materialisation-processor";

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
function buildFlushQueue(
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

describe("FlushCoordinator — materialisation via FlushQueue", () => {
	it("calls materialise on the adapter when it is materialisable and flush succeeds", async () => {
		const materialiseCalls: Array<{ deltas: RowDelta[]; schemas: TableSchema[] }> = [];
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async (deltas: RowDelta[], schemas: ReadonlyArray<TableSchema>) => {
				materialiseCalls.push({ deltas: [...deltas], schemas: [...schemas] });
				return Ok(undefined);
			},
		};

		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-mat" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter),
		});

		expect(result.ok).toBe(true);
		expect(materialiseCalls).toHaveLength(1);
		expect(materialiseCalls[0]!.deltas).toHaveLength(1);
	});

	it("calls additional materialisers from deps alongside the adapter", async () => {
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

		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-multi-mat" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter, [extraMaterialiser]),
		});

		expect(result.ok).toBe(true);
		expect(adapterCalls).toHaveLength(1);
		expect(extraCalls).toHaveLength(1);
	});

	it("does not call materialise when no schemas provided", async () => {
		const materialise = vi.fn().mockResolvedValue(Ok(undefined));
		const dbAdapter = { ...createMockDbAdapter(), materialise };

		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-no-schemas" },
			flushQueue: buildFlushQueue(dbAdapter),
		});

		expect(result.ok).toBe(true);
		expect(materialise).not.toHaveBeenCalled();
	});

	it("does not call materialise when adapter is not materialisable", async () => {
		const dbAdapter = createMockDbAdapter();

		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-not-mat" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter),
		});

		expect(result.ok).toBe(true);
	});

	it("flush still succeeds when materialise returns Err", async () => {
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async () => {
				return Err({ code: "ADAPTER_ERROR", message: "materialise exploded" } as AdapterError);
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-mat-fail" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter),
		});

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
		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-mat-throw" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter),
		});

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

		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-flush-fail" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter),
		});

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

		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow, table: "todos" }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-mat-cb" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter, undefined, onFailure),
		});

		expect(result.ok).toBe(true);
		expect(onFailure).toHaveBeenCalledTimes(1);
		expect(onFailure).toHaveBeenCalledWith("todos", 1, expect.any(Error));
		expect(onFailure.mock.calls[0]![2].message).toBe("mat failed");
		warnSpy.mockRestore();
	});

	it("does NOT invoke onFailure callback when materialise succeeds", async () => {
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async () => {
				return Ok(undefined);
			},
		};

		const onFailure = vi.fn();
		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-mat-ok" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter, undefined, onFailure),
		});

		expect(result.ok).toBe(true);
		expect(onFailure).not.toHaveBeenCalled();
	});

	it("invokes onFailure callback when materialise throws", async () => {
		const dbAdapter: DatabaseAdapter & Materialisable = {
			...createMockDbAdapter(),
			materialise: async () => {
				throw new Error("explosion");
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const onFailure = vi.fn();

		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow, table: "todos" }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flush(buffer, dbAdapter, {
			config: { gatewayId: "gw-mat-throw-cb" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter, undefined, onFailure),
		});

		expect(result.ok).toBe(true);
		expect(onFailure).toHaveBeenCalledTimes(1);
		expect(onFailure).toHaveBeenCalledWith("todos", 1, expect.any(Error));
		expect(onFailure.mock.calls[0]![2].message).toBe("explosion");
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

		const buffer = new DeltaBuffer();
		buffer.append(makeDelta({ hlc: hlcLow, table: "todos" }));

		const coordinator = new FlushCoordinator();
		const result = await coordinator.flushTable("todos", buffer, dbAdapter, {
			config: { gatewayId: "gw-table-mat" },
			schemas: todoSchemas,
			flushQueue: buildFlushQueue(dbAdapter),
		});

		expect(result.ok).toBe(true);
		expect(materialiseCalls).toHaveLength(1);
	});
});

import type {
	AdapterError,
	DeltaOp,
	HLCTimestamp,
	Materialisable,
	RowDelta,
	TableSchema,
} from "@lakesync/core";
import { Err, HLC, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { collectMaterialisers, processMaterialisation } from "../materialisation-processor";

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

const hlcLow = HLC.encode(1_000_000, 0);

const todoSchemas: TableSchema[] = [
	{ table: "todos", columns: [{ name: "title", type: "string" }] },
];

describe("processMaterialisation", () => {
	it("calls all materialisers with entries and schemas", async () => {
		const calls: Array<{ deltas: RowDelta[]; schemas: ReadonlyArray<TableSchema> }> = [];
		const mat: Materialisable = {
			materialise: async (deltas, schemas) => {
				calls.push({ deltas: [...deltas], schemas });
				return Ok(undefined);
			},
		};

		const entries = [makeDelta({ hlc: hlcLow })];
		await processMaterialisation(entries, todoSchemas, { materialisers: [mat] });

		expect(calls).toHaveLength(1);
		expect(calls[0]!.deltas).toHaveLength(1);
		expect(calls[0]!.schemas).toBe(todoSchemas);
	});

	it("skips when schemas is empty", async () => {
		const mat: Materialisable = {
			materialise: vi.fn().mockResolvedValue(Ok(undefined)),
		};

		await processMaterialisation([makeDelta({ hlc: hlcLow })], [], {
			materialisers: [mat],
		});

		expect(mat.materialise).not.toHaveBeenCalled();
	});

	it("skips when materialisers is empty", async () => {
		// Should not throw
		await processMaterialisation([makeDelta({ hlc: hlcLow })], todoSchemas, {
			materialisers: [],
		});
	});

	it("calls onFailure callback when materialise returns Err", async () => {
		const mat: Materialisable = {
			materialise: async () =>
				Err({ code: "ADAPTER_ERROR", message: "mat failed" } as AdapterError),
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const onFailure = vi.fn();

		await processMaterialisation([makeDelta({ hlc: hlcLow, table: "todos" })], todoSchemas, {
			materialisers: [mat],
			onFailure,
		});

		expect(onFailure).toHaveBeenCalledTimes(1);
		expect(onFailure).toHaveBeenCalledWith("todos", 1, expect.any(Error));
		warnSpy.mockRestore();
	});

	it("calls onFailure callback when materialise throws", async () => {
		const mat: Materialisable = {
			materialise: async () => {
				throw new Error("kaboom");
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const onFailure = vi.fn();

		await processMaterialisation([makeDelta({ hlc: hlcLow, table: "todos" })], todoSchemas, {
			materialisers: [mat],
			onFailure,
		});

		expect(onFailure).toHaveBeenCalledTimes(1);
		expect(onFailure).toHaveBeenCalledWith("todos", 1, expect.any(Error));
		expect(onFailure.mock.calls[0]![2].message).toBe("kaboom");
		warnSpy.mockRestore();
	});

	it("never throws even when materialise fails", async () => {
		const mat: Materialisable = {
			materialise: async () => {
				throw new Error("boom");
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

		// Should not throw
		await processMaterialisation([makeDelta({ hlc: hlcLow })], todoSchemas, {
			materialisers: [mat],
		});

		expect(warnSpy).toHaveBeenCalled();
		warnSpy.mockRestore();
	});

	it("reports per-table failures with correct counts", async () => {
		const mat: Materialisable = {
			materialise: async () => Err({ code: "ADAPTER_ERROR", message: "fail" } as AdapterError),
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const onFailure = vi.fn();

		await processMaterialisation(
			[
				makeDelta({ hlc: hlcLow, table: "todos", rowId: "r1" }),
				makeDelta({ hlc: HLC.encode(1_000_001, 0), table: "todos", rowId: "r2" }),
				makeDelta({ hlc: HLC.encode(1_000_002, 0), table: "users", rowId: "r3" }),
			],
			todoSchemas,
			{ materialisers: [mat], onFailure },
		);

		expect(onFailure).toHaveBeenCalledTimes(2);
		const todoCall = onFailure.mock.calls.find((c) => c[0] === "todos");
		const userCall = onFailure.mock.calls.find((c) => c[0] === "users");
		expect(todoCall![1]).toBe(2);
		expect(userCall![1]).toBe(1);
		warnSpy.mockRestore();
	});
});

describe("collectMaterialisers", () => {
	it("collects from materialisable adapter", () => {
		const mat: Materialisable = {
			materialise: vi.fn().mockResolvedValue(Ok(undefined)),
		};
		const result = collectMaterialisers(mat);
		expect(result).toHaveLength(1);
		expect(result[0]).toBe(mat);
	});

	it("skips non-materialisable adapter", () => {
		const result = collectMaterialisers({});
		expect(result).toHaveLength(0);
	});

	it("includes extra materialisers", () => {
		const extra: Materialisable = {
			materialise: vi.fn().mockResolvedValue(Ok(undefined)),
		};
		const result = collectMaterialisers({}, [extra]);
		expect(result).toHaveLength(1);
		expect(result[0]).toBe(extra);
	});

	it("combines adapter and extras", () => {
		const adapter: Materialisable = {
			materialise: vi.fn().mockResolvedValue(Ok(undefined)),
		};
		const extra: Materialisable = {
			materialise: vi.fn().mockResolvedValue(Ok(undefined)),
		};
		const result = collectMaterialisers(adapter, [extra]);
		expect(result).toHaveLength(2);
	});
});

import type {
	AdapterError,
	DeltaOp,
	HLCTimestamp,
	LakeAdapter,
	Materialisable,
	Result,
	RowDelta,
	TableSchema,
} from "@lakesync/core";
import { Err, HLC, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { isFlushQueue, MemoryFlushQueue } from "../flush-queue";
import { R2FlushQueue } from "../r2-flush-queue";

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

describe("isFlushQueue", () => {
	it("returns true for objects with a publish function", () => {
		expect(isFlushQueue({ publish: async () => Ok(undefined) })).toBe(true);
	});

	it("returns false for null", () => {
		expect(isFlushQueue(null)).toBe(false);
	});

	it("returns false for objects without publish", () => {
		expect(isFlushQueue({ foo: "bar" })).toBe(false);
	});
});

describe("MemoryFlushQueue", () => {
	it("calls materialise inline and returns Ok", async () => {
		const calls: RowDelta[][] = [];
		const mat: Materialisable = {
			materialise: async (deltas) => {
				calls.push([...deltas]);
				return Ok(undefined);
			},
		};

		const queue = new MemoryFlushQueue([mat]);
		const entries = [makeDelta({ hlc: hlcLow })];
		const result = await queue.publish(entries, {
			gatewayId: "gw-1",
			schemas: todoSchemas,
		});

		expect(result.ok).toBe(true);
		expect(calls).toHaveLength(1);
		expect(calls[0]!).toHaveLength(1);
	});

	it("returns Ok even when materialise fails", async () => {
		const mat: Materialisable = {
			materialise: async () => Err({ code: "ADAPTER_ERROR", message: "fail" } as AdapterError),
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const queue = new MemoryFlushQueue([mat]);
		const result = await queue.publish([makeDelta({ hlc: hlcLow })], {
			gatewayId: "gw-1",
			schemas: todoSchemas,
		});

		expect(result.ok).toBe(true);
		warnSpy.mockRestore();
	});

	it("passes onFailure callback through", async () => {
		const mat: Materialisable = {
			materialise: async () => Err({ code: "ADAPTER_ERROR", message: "fail" } as AdapterError),
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const onFailure = vi.fn();
		const queue = new MemoryFlushQueue([mat], onFailure);

		await queue.publish([makeDelta({ hlc: hlcLow, table: "todos" })], {
			gatewayId: "gw-1",
			schemas: todoSchemas,
		});

		expect(onFailure).toHaveBeenCalledTimes(1);
		expect(onFailure).toHaveBeenCalledWith("todos", 1, expect.any(Error));
		warnSpy.mockRestore();
	});
});

describe("R2FlushQueue", () => {
	function createMockLakeAdapter(): LakeAdapter & {
		puts: Array<{ key: string; data: Uint8Array }>;
	} {
		const puts: Array<{ key: string; data: Uint8Array }> = [];
		return {
			puts,
			async putObject(path: string, data: Uint8Array): Promise<Result<void, AdapterError>> {
				puts.push({ key: path, data });
				return Ok(undefined);
			},
			async getObject(): Promise<Result<Uint8Array, AdapterError>> {
				return Ok(new Uint8Array());
			},
			async headObject(): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
				return Ok({ size: 0, lastModified: new Date() });
			},
			async listObjects(): Promise<Result<never[], AdapterError>> {
				return Ok([]);
			},
			async deleteObject(): Promise<Result<void, AdapterError>> {
				return Ok(undefined);
			},
			async deleteObjects(): Promise<Result<void, AdapterError>> {
				return Ok(undefined);
			},
		};
	}

	it("writes payload to R2 under materialise-jobs prefix", async () => {
		const adapter = createMockLakeAdapter();
		const queue = new R2FlushQueue(adapter);
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await queue.publish(entries, {
			gatewayId: "gw-1",
			schemas: todoSchemas,
		});

		expect(result.ok).toBe(true);
		expect(adapter.puts).toHaveLength(1);
		expect(adapter.puts[0]!.key).toMatch(/^materialise-jobs\/gw-1\//);
		expect(adapter.puts[0]!.key).toMatch(/\.json$/);
	});

	it("returns Ok(undefined) for empty entries", async () => {
		const adapter = createMockLakeAdapter();
		const queue = new R2FlushQueue(adapter);

		const result = await queue.publish([], {
			gatewayId: "gw-1",
			schemas: todoSchemas,
		});

		expect(result.ok).toBe(true);
		expect(adapter.puts).toHaveLength(0);
	});

	it("returns Err when adapter putObject fails", async () => {
		const adapter = createMockLakeAdapter();
		adapter.putObject = async () =>
			Err({ code: "ADAPTER_ERROR", message: "R2 error" } as AdapterError);

		const queue = new R2FlushQueue(adapter);
		const result = await queue.publish([makeDelta({ hlc: hlcLow })], {
			gatewayId: "gw-1",
			schemas: todoSchemas,
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("FLUSH_QUEUE_ERROR");
		}
	});

	it("serialised payload contains entries and schemas", async () => {
		const adapter = createMockLakeAdapter();
		const queue = new R2FlushQueue(adapter);
		const entries = [makeDelta({ hlc: hlcLow })];

		await queue.publish(entries, {
			gatewayId: "gw-1",
			schemas: todoSchemas,
		});

		const text = new TextDecoder().decode(adapter.puts[0]!.data);
		const parsed = JSON.parse(text);
		expect(parsed).toHaveProperty("entries");
		expect(parsed).toHaveProperty("schemas");
		expect(parsed.entries).toHaveLength(1);
		expect(parsed.schemas).toHaveLength(1);
	});
});

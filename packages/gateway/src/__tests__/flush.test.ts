import type { NessieCatalogueClient } from "@lakesync/catalogue";
import { CatalogueError } from "@lakesync/catalogue";
import type {
	AdapterError,
	DatabaseAdapter,
	DeltaOp,
	HLCTimestamp,
	LakeAdapter,
	Result,
	RowDelta,
	TableSchema,
} from "@lakesync/core";
import { Err, HLC, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { commitToCatalogue, type FlushDeps, flushEntries, hlcRange } from "../flush";

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

/** In-memory mock lake adapter. */
function createMockLakeAdapter(): LakeAdapter & { stored: Map<string, Uint8Array> } {
	const stored = new Map<string, Uint8Array>();
	return {
		stored,
		async putObject(path: string, data: Uint8Array): Promise<Result<void, AdapterError>> {
			stored.set(path, data);
			return Ok(undefined);
		},
		async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
			const data = stored.get(path);
			return data ? Ok(data) : Err({ code: "ADAPTER_ERROR", message: "Not found" } as AdapterError);
		},
		async headObject(
			path: string,
		): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
			const data = stored.get(path);
			return data
				? Ok({ size: data.length, lastModified: new Date() })
				: Err({ code: "ADAPTER_ERROR", message: "Not found" } as AdapterError);
		},
		async listObjects(
			prefix: string,
		): Promise<Result<Array<{ key: string; size: number; lastModified: Date }>, AdapterError>> {
			const results = [...stored.entries()]
				.filter(([k]) => k.startsWith(prefix))
				.map(([key, data]) => ({ key, size: data.length, lastModified: new Date() }));
			return Ok(results);
		},
		async deleteObject(path: string): Promise<Result<void, AdapterError>> {
			stored.delete(path);
			return Ok(undefined);
		},
		async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
			for (const p of paths) stored.delete(p);
			return Ok(undefined);
		},
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

/** Failing lake adapter. */
function createFailingLakeAdapter(): LakeAdapter {
	return {
		async putObject(): Promise<Result<void, AdapterError>> {
			return Err({ code: "ADAPTER_ERROR", message: "Simulated write failure" } as AdapterError);
		},
		async getObject(): Promise<Result<Uint8Array, AdapterError>> {
			return Err({ code: "ADAPTER_ERROR", message: "Not implemented" } as AdapterError);
		},
		async headObject(): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
			return Err({ code: "ADAPTER_ERROR", message: "Not implemented" } as AdapterError);
		},
		async listObjects(): Promise<
			Result<Array<{ key: string; size: number; lastModified: Date }>, AdapterError>
		> {
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

/** Mock catalogue with spies. */
function createMockCatalogue(): NessieCatalogueClient & {
	createNamespace: ReturnType<typeof vi.fn>;
	createTable: ReturnType<typeof vi.fn>;
	appendFiles: ReturnType<typeof vi.fn>;
} {
	return {
		createNamespace: vi.fn().mockResolvedValue(Ok(undefined)),
		createTable: vi.fn().mockResolvedValue(Ok(undefined)),
		loadTable: vi.fn().mockResolvedValue(Ok({})),
		appendFiles: vi.fn().mockResolvedValue(Ok(undefined)),
		currentSnapshot: vi.fn().mockResolvedValue(Ok(null)),
		listNamespaces: vi.fn().mockResolvedValue(Ok([])),
	} as unknown as NessieCatalogueClient & {
		createNamespace: ReturnType<typeof vi.fn>;
		createTable: ReturnType<typeof vi.fn>;
		appendFiles: ReturnType<typeof vi.fn>;
	};
}

const todoSchema: TableSchema = {
	table: "todos",
	columns: [{ name: "title", type: "string" }],
};

const hlcLow = HLC.encode(1_000_000, 0);
const hlcMid = HLC.encode(2_000_000, 0);
const hlcHigh = HLC.encode(3_000_000, 0);

describe("hlcRange", () => {
	it("returns correct min and max with multiple entries", () => {
		const entries = [
			makeDelta({ hlc: hlcMid }),
			makeDelta({ hlc: hlcLow }),
			makeDelta({ hlc: hlcHigh }),
		];
		const { min, max } = hlcRange(entries);
		expect(min).toBe(hlcLow);
		expect(max).toBe(hlcHigh);
	});

	it("returns same value for single entry", () => {
		const entries = [makeDelta({ hlc: hlcMid })];
		const { min, max } = hlcRange(entries);
		expect(min).toBe(hlcMid);
		expect(max).toBe(hlcMid);
	});
});

describe("flushEntries — DB adapter", () => {
	it("calls insertDeltas with entries and returns Ok", async () => {
		const dbAdapter = createMockDbAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-db-1" },
			restoreEntries,
		};
		const entries = [
			makeDelta({ hlc: hlcLow, deltaId: "d1" }),
			makeDelta({ hlc: hlcMid, deltaId: "d2" }),
		];

		const result = await flushEntries(entries, 100, deps);

		expect(result.ok).toBe(true);
		expect(dbAdapter.calls).toHaveLength(1);
		expect(dbAdapter.calls[0]).toHaveLength(2);
		expect(restoreEntries).not.toHaveBeenCalled();
	});

	it("restores entries on insertDeltas failure", async () => {
		const dbAdapter = createFailingDbAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-db-fail" },
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 50, deps);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Database flush failed");
		}
		expect(restoreEntries).toHaveBeenCalledWith(entries);
	});

	it("restores entries on unexpected throw", async () => {
		const throwingAdapter: DatabaseAdapter = {
			async insertDeltas() {
				throw new Error("Kaboom");
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
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: throwingAdapter,
			config: { gatewayId: "gw-db-throw" },
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 50, deps);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Unexpected database flush failure");
			expect(result.error.message).toContain("Kaboom");
		}
		expect(restoreEntries).toHaveBeenCalledWith(entries);
	});
});

describe("flushEntries — Lake + JSON", () => {
	it("writes FlushEnvelope with correct .json object key", async () => {
		const lakeAdapter = createMockLakeAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: { gatewayId: "gw-json-1", flushFormat: "json" },
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow, deltaId: "d-json-1" })];

		const result = await flushEntries(entries, 200, deps);

		expect(result.ok).toBe(true);
		expect(lakeAdapter.stored.size).toBe(1);
		const key = [...lakeAdapter.stored.keys()][0]!;
		expect(key).toMatch(/\.json$/);
		expect(key).toContain("gw-json-1");
	});

	it("envelope contains correct fields", async () => {
		const lakeAdapter = createMockLakeAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: { gatewayId: "gw-json-fields", flushFormat: "json" },
			restoreEntries,
		};
		const entries = [
			makeDelta({ hlc: hlcLow, deltaId: "d1" }),
			makeDelta({ hlc: hlcMid, deltaId: "d2" }),
		];

		await flushEntries(entries, 300, deps);

		const data = [...lakeAdapter.stored.values()][0]!;
		const envelope = JSON.parse(new TextDecoder().decode(data));
		expect(envelope.version).toBe(1);
		expect(envelope.gatewayId).toBe("gw-json-fields");
		expect(envelope.deltaCount).toBe(2);
		expect(envelope.byteSize).toBe(300);
		expect(envelope.createdAt).toBeDefined();
		expect(envelope.hlcRange).toBeDefined();
		expect(envelope.deltas).toHaveLength(2);
	});

	it("uses application/json content type", async () => {
		const putSpy = vi.fn().mockResolvedValue(Ok(undefined));
		const lakeAdapter: LakeAdapter = {
			putObject: putSpy,
			async getObject() {
				return Err({ code: "ADAPTER_ERROR", message: "n/a" } as AdapterError);
			},
			async headObject() {
				return Err({ code: "ADAPTER_ERROR", message: "n/a" } as AdapterError);
			},
			async listObjects() {
				return Ok([]);
			},
			async deleteObject() {
				return Ok(undefined);
			},
			async deleteObjects() {
				return Ok(undefined);
			},
		};
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: { gatewayId: "gw-ct", flushFormat: "json" },
			restoreEntries: vi.fn(),
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		await flushEntries(entries, 50, deps);

		expect(putSpy).toHaveBeenCalledWith(
			expect.stringContaining(".json"),
			expect.any(Uint8Array),
			"application/json",
		);
	});
});

describe("flushEntries — Lake + Parquet", () => {
	it("writes with correct .parquet object key", async () => {
		const lakeAdapter = createMockLakeAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: { gatewayId: "gw-pq-1", tableSchema: todoSchema },
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow, deltaId: "d-pq-1" })];

		const result = await flushEntries(entries, 100, deps);

		expect(result.ok).toBe(true);
		expect(lakeAdapter.stored.size).toBe(1);
		const key = [...lakeAdapter.stored.keys()][0]!;
		expect(key).toMatch(/\.parquet$/);
		expect(key).toContain("gw-pq-1");
	});

	it("missing tableSchema returns FlushError and restores entries", async () => {
		const lakeAdapter = createMockLakeAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: { gatewayId: "gw-no-schema" },
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 50, deps);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("tableSchema required");
		}
		expect(restoreEntries).toHaveBeenCalledWith(entries);
	});

	it("restores entries on putObject failure", async () => {
		const lakeAdapter = createFailingLakeAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: { gatewayId: "gw-pq-fail", flushFormat: "json" },
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 50, deps);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to write flush envelope");
		}
		expect(restoreEntries).toHaveBeenCalledWith(entries);
	});
});

describe("flushEntries — with keyPrefix", () => {
	it("object key includes prefix before HLC range", async () => {
		const lakeAdapter = createMockLakeAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: { gatewayId: "gw-prefix", flushFormat: "json" },
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		await flushEntries(entries, 50, deps, "todos");

		const key = [...lakeAdapter.stored.keys()][0]!;
		expect(key).toContain("/todos-");
		expect(key).toMatch(/\/todos-\d+-\d+\.json$/);
	});

	it("no prefix omits the dash separator", async () => {
		const lakeAdapter = createMockLakeAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: { gatewayId: "gw-no-prefix", flushFormat: "json" },
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		await flushEntries(entries, 50, deps);

		const key = [...lakeAdapter.stored.keys()][0]!;
		// Should NOT have a table name prefix
		expect(key).toMatch(/\/gw-no-prefix\/\d+-\d+\.json$/);
	});
});

describe("flushEntries — catalogue integration", () => {
	it("calls commitToCatalogue when catalogue + tableSchema configured", async () => {
		const lakeAdapter = createMockLakeAdapter();
		const catalogue = createMockCatalogue();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: {
				gatewayId: "gw-cat-int",
				flushFormat: "parquet",
				tableSchema: todoSchema,
				catalogue,
			},
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow, deltaId: "d-cat-1" })];

		const result = await flushEntries(entries, 100, deps);

		expect(result.ok).toBe(true);
		expect(catalogue.createNamespace).toHaveBeenCalledTimes(1);
		expect(catalogue.createTable).toHaveBeenCalledTimes(1);
		expect(catalogue.appendFiles).toHaveBeenCalledTimes(1);
	});

	it("does NOT call catalogue when not configured", async () => {
		const lakeAdapter = createMockLakeAdapter();
		const restoreEntries = vi.fn();
		const deps: FlushDeps = {
			adapter: lakeAdapter,
			config: { gatewayId: "gw-no-cat", flushFormat: "json" },
			restoreEntries,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 50, deps);

		expect(result.ok).toBe(true);
		// No catalogue configured — nothing to assert on catalogue, just verify flush succeeded
		expect(lakeAdapter.stored.size).toBe(1);
	});
});

describe("commitToCatalogue", () => {
	it("calls createNamespace, createTable, appendFiles", async () => {
		const catalogue = createMockCatalogue();

		await commitToCatalogue("deltas/2025-01-01/gw/file.parquet", 1024, 10, catalogue, todoSchema);

		expect(catalogue.createNamespace).toHaveBeenCalledWith(["lakesync"]);
		expect(catalogue.createTable).toHaveBeenCalledWith(
			["lakesync"],
			"todos",
			expect.objectContaining({ type: "struct" }),
			expect.objectContaining({ "spec-id": 0 }),
		);
		expect(catalogue.appendFiles).toHaveBeenCalledWith(["lakesync"], "todos", [
			expect.objectContaining({
				content: "data",
				"file-path": "deltas/2025-01-01/gw/file.parquet",
				"file-format": "PARQUET",
				"record-count": 10,
				"file-size-in-bytes": 1024,
			}),
		]);
	});

	it("409 on appendFiles triggers a single retry (called twice)", async () => {
		const catalogue = createMockCatalogue();
		catalogue.appendFiles
			.mockResolvedValueOnce(Err(new CatalogueError("Conflict", 409)))
			.mockResolvedValueOnce(Ok(undefined));

		await commitToCatalogue("deltas/file.parquet", 512, 5, catalogue, todoSchema);

		expect(catalogue.appendFiles).toHaveBeenCalledTimes(2);
	});

	it("409 on createTable still calls appendFiles (table already exists)", async () => {
		const catalogue = createMockCatalogue();
		catalogue.createTable.mockResolvedValue(Err(new CatalogueError("Already exists", 409)));

		await commitToCatalogue("deltas/file.parquet", 512, 5, catalogue, todoSchema);

		expect(catalogue.appendFiles).toHaveBeenCalledTimes(1);
	});

	it("non-409 error on createTable does NOT call appendFiles (early return)", async () => {
		const catalogue = createMockCatalogue();
		catalogue.createTable.mockResolvedValue(Err(new CatalogueError("Internal error", 500)));

		await commitToCatalogue("deltas/file.parquet", 512, 5, catalogue, todoSchema);

		expect(catalogue.appendFiles).not.toHaveBeenCalled();
	});

	it("non-409 error on appendFiles does not throw (best-effort)", async () => {
		const catalogue = createMockCatalogue();
		catalogue.appendFiles.mockResolvedValue(Err(new CatalogueError("Server error", 500)));

		// Should not throw
		await expect(
			commitToCatalogue("deltas/file.parquet", 512, 5, catalogue, todoSchema),
		).resolves.toBeUndefined();

		expect(catalogue.appendFiles).toHaveBeenCalledTimes(1);
	});
});

describe("flushEntries — materialisation", () => {
	it("calls materialise when schemas and materialisable adapter are provided", async () => {
		const materialiseCalls: Array<{ deltas: RowDelta[]; schemas: TableSchema[] }> = [];
		const dbAdapter: DatabaseAdapter & { materialise: (...args: never) => unknown } = {
			...createMockDbAdapter(),
			materialise: async (deltas: RowDelta[], schemas: ReadonlyArray<TableSchema>) => {
				materialiseCalls.push({ deltas: [...deltas], schemas: [...schemas] });
				return Ok(undefined);
			},
		};

		const schemas: TableSchema[] = [
			{ table: "todos", columns: [{ name: "title", type: "string" }] },
		];
		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-mat" },
			restoreEntries: vi.fn(),
			schemas,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 0, deps);
		expect(result.ok).toBe(true);
		expect(materialiseCalls).toHaveLength(1);
		expect(materialiseCalls[0]!.deltas).toHaveLength(1);
	});

	it("does not call materialise when no schemas provided", async () => {
		const materialise = vi.fn();
		const dbAdapter = { ...createMockDbAdapter(), materialise };

		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-no-schemas" },
			restoreEntries: vi.fn(),
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 0, deps);
		expect(result.ok).toBe(true);
		expect(materialise).not.toHaveBeenCalled();
	});

	it("does not call materialise when adapter is not materialisable", async () => {
		const dbAdapter = createMockDbAdapter();
		const schemas: TableSchema[] = [
			{ table: "todos", columns: [{ name: "title", type: "string" }] },
		];

		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-not-mat" },
			restoreEntries: vi.fn(),
			schemas,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 0, deps);
		expect(result.ok).toBe(true);
	});

	it("flush still succeeds when materialise fails", async () => {
		const dbAdapter = {
			...createMockDbAdapter(),
			materialise: async () => {
				return Err({ code: "ADAPTER_ERROR", message: "materialise exploded" } as AdapterError);
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const schemas: TableSchema[] = [
			{ table: "todos", columns: [{ name: "title", type: "string" }] },
		];

		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-mat-fail" },
			restoreEntries: vi.fn(),
			schemas,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 0, deps);
		expect(result.ok).toBe(true);
		expect(warnSpy).toHaveBeenCalled();
		warnSpy.mockRestore();
	});

	it("flush still succeeds when materialise throws", async () => {
		const dbAdapter = {
			...createMockDbAdapter(),
			materialise: async () => {
				throw new Error("kaboom");
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const schemas: TableSchema[] = [
			{ table: "todos", columns: [{ name: "title", type: "string" }] },
		];

		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-mat-throw" },
			restoreEntries: vi.fn(),
			schemas,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 0, deps);
		expect(result.ok).toBe(true);
		expect(warnSpy).toHaveBeenCalled();
		warnSpy.mockRestore();
	});

	it("invokes onMaterialisationFailure callback when materialise fails", async () => {
		const dbAdapter = {
			...createMockDbAdapter(),
			materialise: async () => {
				return Err({ code: "ADAPTER_ERROR", message: "mat failed" } as AdapterError);
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const onFailure = vi.fn();
		const schemas: TableSchema[] = [
			{ table: "todos", columns: [{ name: "title", type: "string" }] },
		];

		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-mat-cb", onMaterialisationFailure: onFailure },
			restoreEntries: vi.fn(),
			schemas,
		};
		const entries = [makeDelta({ hlc: hlcLow, table: "todos" })];

		const result = await flushEntries(entries, 0, deps);
		expect(result.ok).toBe(true);
		expect(onFailure).toHaveBeenCalledTimes(1);
		expect(onFailure).toHaveBeenCalledWith("todos", 1, expect.any(Error));
		expect(onFailure.mock.calls[0]![2].message).toBe("mat failed");
		warnSpy.mockRestore();
	});

	it("does NOT invoke onMaterialisationFailure callback when materialise succeeds", async () => {
		const dbAdapter = {
			...createMockDbAdapter(),
			materialise: async () => {
				return Ok(undefined);
			},
		};

		const onFailure = vi.fn();
		const schemas: TableSchema[] = [
			{ table: "todos", columns: [{ name: "title", type: "string" }] },
		];

		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-mat-ok", onMaterialisationFailure: onFailure },
			restoreEntries: vi.fn(),
			schemas,
		};
		const entries = [makeDelta({ hlc: hlcLow })];

		const result = await flushEntries(entries, 0, deps);
		expect(result.ok).toBe(true);
		expect(onFailure).not.toHaveBeenCalled();
	});

	it("invokes onMaterialisationFailure callback when materialise throws", async () => {
		const dbAdapter = {
			...createMockDbAdapter(),
			materialise: async () => {
				throw new Error("explosion");
			},
		};

		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		const onFailure = vi.fn();
		const schemas: TableSchema[] = [
			{ table: "todos", columns: [{ name: "title", type: "string" }] },
		];

		const deps: FlushDeps = {
			adapter: dbAdapter,
			config: { gatewayId: "gw-mat-throw-cb", onMaterialisationFailure: onFailure },
			restoreEntries: vi.fn(),
			schemas,
		};
		const entries = [makeDelta({ hlc: hlcLow, table: "todos" })];

		const result = await flushEntries(entries, 0, deps);
		expect(result.ok).toBe(true);
		expect(onFailure).toHaveBeenCalledTimes(1);
		expect(onFailure).toHaveBeenCalledWith("todos", 1, expect.any(Error));
		expect(onFailure.mock.calls[0]![2].message).toBe("explosion");
		warnSpy.mockRestore();
	});
});

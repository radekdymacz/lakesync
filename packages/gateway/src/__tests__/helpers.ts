import type { DatabaseAdapter, LakeAdapter } from "@lakesync/adapter";
import type { NessieCatalogueClient } from "@lakesync/catalogue";
import type { AdapterError, DeltaOp, HLCTimestamp, Result, RowDelta } from "@lakesync/core";
import { Err, Ok } from "@lakesync/core";
import { vi } from "vitest";

/** Helper to build a RowDelta with sensible defaults. */
export function makeDelta(opts: Partial<RowDelta> & { hlc: HLCTimestamp }): RowDelta {
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

/** Simple in-memory mock lake adapter. */
export function createMockLakeAdapter(): LakeAdapter & { stored: Map<string, Uint8Array> } {
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
				.map(([key, data]) => ({
					key,
					size: data.length,
					lastModified: new Date(),
				}));
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

/** Simple failing mock lake adapter. */
export function createFailingLakeAdapter(): LakeAdapter {
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

/** In-memory mock DatabaseAdapter that records calls. */
export function createMockDatabaseAdapter(): DatabaseAdapter & { calls: RowDelta[][] } {
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
export function createFailingDatabaseAdapter(): DatabaseAdapter {
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

/** Mock NessieCatalogueClient with spies on all public methods. */
export function createMockCatalogue(): NessieCatalogueClient & {
	createNamespace: ReturnType<typeof vi.fn>;
	createTable: ReturnType<typeof vi.fn>;
	loadTable: ReturnType<typeof vi.fn>;
	appendFiles: ReturnType<typeof vi.fn>;
	currentSnapshot: ReturnType<typeof vi.fn>;
	listNamespaces: ReturnType<typeof vi.fn>;
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
		loadTable: ReturnType<typeof vi.fn>;
		appendFiles: ReturnType<typeof vi.fn>;
		currentSnapshot: ReturnType<typeof vi.fn>;
		listNamespaces: ReturnType<typeof vi.fn>;
	};
}

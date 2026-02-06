import { HLC, Ok, SchemaError } from "@lakesync/core";
import type { DeltaOp, HLCTimestamp, RowDelta, TableSchema } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { SyncGateway } from "../gateway";
import { SchemaManager } from "../schema-manager";
import type { GatewayConfig } from "../types";

/** Helper to build a RowDelta with sensible defaults */
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

const todosSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "done", type: "boolean" },
	],
};

describe("SchemaManager", () => {
	describe("validateDelta", () => {
		it("valid columns return Ok", () => {
			const manager = new SchemaManager(todosSchema);
			const hlc = HLC.encode(1_000_000, 0);
			const delta = makeDelta({
				hlc,
				columns: [
					{ column: "title", value: "Hello" },
					{ column: "done", value: false },
				],
			});

			const result = manager.validateDelta(delta);
			expect(result.ok).toBe(true);
		});

		it("unknown column returns SchemaError", () => {
			const manager = new SchemaManager(todosSchema);
			const hlc = HLC.encode(1_000_000, 0);
			const delta = makeDelta({
				hlc,
				columns: [{ column: "nonexistent", value: "bad" }],
			});

			const result = manager.validateDelta(delta);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(SchemaError);
				expect(result.error.code).toBe("SCHEMA_MISMATCH");
				expect(result.error.message).toContain("nonexistent");
			}
		});

		it("DELETE with empty columns is always valid", () => {
			const manager = new SchemaManager(todosSchema);
			const hlc = HLC.encode(1_000_000, 0);
			const delta = makeDelta({
				hlc,
				op: "DELETE",
				columns: [],
			});

			const result = manager.validateDelta(delta);
			expect(result.ok).toBe(true);
		});

		it("sparse delta (subset of columns) is valid", () => {
			const manager = new SchemaManager(todosSchema);
			const hlc = HLC.encode(1_000_000, 0);
			const delta = makeDelta({
				hlc,
				columns: [{ column: "done", value: true }],
			});

			const result = manager.validateDelta(delta);
			expect(result.ok).toBe(true);
		});
	});

	describe("evolveSchema", () => {
		it("adding a column increments version", () => {
			const manager = new SchemaManager(todosSchema);
			expect(manager.getSchema().version).toBe(1);

			const evolved: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "done", type: "boolean" },
					{ name: "priority", type: "number" },
				],
			};

			const result = manager.evolveSchema(evolved);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.version).toBe(2);
			}
			expect(manager.getSchema().version).toBe(2);

			// New column should now be allowed
			const hlc = HLC.encode(1_000_000, 0);
			const delta = makeDelta({
				hlc,
				columns: [{ column: "priority", value: 1 }],
			});
			expect(manager.validateDelta(delta).ok).toBe(true);
		});

		it("removing a column returns SchemaError", () => {
			const manager = new SchemaManager(todosSchema);

			const shrunk: TableSchema = {
				table: "todos",
				columns: [{ name: "title", type: "string" }],
			};

			const result = manager.evolveSchema(shrunk);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(SchemaError);
				expect(result.error.message).toContain("done");
				expect(result.error.message).toContain("Cannot remove column");
			}
		});

		it("changing a column type returns SchemaError", () => {
			const manager = new SchemaManager(todosSchema);

			const changed: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "number" },
					{ name: "done", type: "boolean" },
				],
			};

			const result = manager.evolveSchema(changed);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(SchemaError);
				expect(result.error.message).toContain("title");
				expect(result.error.message).toContain("string");
				expect(result.error.message).toContain("number");
			}
		});

		it("table name mismatch returns SchemaError", () => {
			const manager = new SchemaManager(todosSchema);

			const mismatch: TableSchema = {
				table: "projects",
				columns: [
					{ name: "title", type: "string" },
					{ name: "done", type: "boolean" },
				],
			};

			const result = manager.evolveSchema(mismatch);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(SchemaError);
				expect(result.error.message).toContain("table name mismatch");
			}
		});
	});

	describe("Gateway integration", () => {
		const hlcLow = HLC.encode(1_000_000, 0);

		const configWithSchema: GatewayConfig = {
			gatewayId: "gw-schema-test",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			flushFormat: "json",
			schemaManager: new SchemaManager(todosSchema),
		};

		it("push with unknown column is rejected", () => {
			const gw = new SyncGateway({
				...configWithSchema,
				schemaManager: new SchemaManager(todosSchema),
			});

			const delta = makeDelta({
				hlc: hlcLow,
				columns: [{ column: "unknown_col", value: "bad" }],
			});

			const result = gw.handlePush({
				clientId: "client-a",
				deltas: [delta],
				lastSeenHlc: hlcLow,
			});

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(SchemaError);
				expect(result.error.code).toBe("SCHEMA_MISMATCH");
			}

			// Nothing should be in the buffer
			expect(gw.bufferStats.logSize).toBe(0);
		});

		it("push with valid columns is accepted", () => {
			const gw = new SyncGateway({
				...configWithSchema,
				schemaManager: new SchemaManager(todosSchema),
			});

			const delta = makeDelta({
				hlc: hlcLow,
				columns: [
					{ column: "title", value: "Valid task" },
					{ column: "done", value: false },
				],
			});

			const result = gw.handlePush({
				clientId: "client-a",
				deltas: [delta],
				lastSeenHlc: hlcLow,
			});

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.accepted).toBe(1);
			}
			expect(gw.bufferStats.logSize).toBe(1);
		});
	});
});

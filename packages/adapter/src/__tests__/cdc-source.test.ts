import { AdapterError, Ok, type TableSchema } from "@lakesync/core";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CdcSource } from "../cdc/cdc-source";
import type { CdcDialect, CdcRawChange } from "../cdc/dialect";

// ---------------------------------------------------------------------------
// Mock dialect
// ---------------------------------------------------------------------------

function createMockDialect(overrides?: Partial<CdcDialect>): CdcDialect {
	return {
		name: "mock",
		connect: vi.fn().mockResolvedValue(Ok(undefined)),
		ensureCapture: vi.fn().mockResolvedValue(Ok(undefined)),
		fetchChanges: vi.fn().mockResolvedValue(Ok({ changes: [], cursor: { seq: 0 } })),
		discoverSchemas: vi.fn().mockResolvedValue(Ok([])),
		close: vi.fn().mockResolvedValue(undefined),
		defaultCursor: vi.fn().mockReturnValue({ seq: 0 }),
		...overrides,
	};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("CdcSource", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	describe("construction", () => {
		it("sets name from dialect name", () => {
			const dialect = createMockDialect();
			const source = new CdcSource({ dialect });

			expect(source.name).toBe("cdc:mock");
		});

		it("sets clientId from dialect name", () => {
			const dialect = createMockDialect();
			const source = new CdcSource({ dialect });

			expect(source.clientId).toBe("cdc:mock");
		});
	});

	describe("cursor management", () => {
		it("starts with the dialect default cursor", () => {
			const dialect = createMockDialect({
				defaultCursor: () => ({ seq: 42 }),
			});
			const source = new CdcSource({ dialect });

			expect(source.getCursor()).toEqual({ seq: 42 });
		});

		it("setCursor updates the cursor", () => {
			const dialect = createMockDialect();
			const source = new CdcSource({ dialect });

			source.setCursor({ seq: 99 });
			expect(source.getCursor()).toEqual({ seq: 99 });
		});

		it("getCursor returns a copy (not a reference)", () => {
			const dialect = createMockDialect();
			const source = new CdcSource({ dialect });

			source.setCursor({ seq: 5 });
			const cursor1 = source.getCursor();
			const cursor2 = source.getCursor();
			expect(cursor1).toEqual(cursor2);
			expect(cursor1).not.toBe(cursor2);
		});
	});

	describe("start", () => {
		it("connects dialect and ensures capture", async () => {
			const dialect = createMockDialect();
			const source = new CdcSource({ dialect, pollIntervalMs: 100_000 });

			const result = await source.start(async () => {});
			expect(result.ok).toBe(true);
			expect(dialect.connect).toHaveBeenCalledOnce();
			expect(dialect.ensureCapture).toHaveBeenCalledOnce();

			await source.stop();
		});

		it("passes tables to ensureCapture", async () => {
			const dialect = createMockDialect();
			const source = new CdcSource({
				dialect,
				tables: ["users", "orders"],
				pollIntervalMs: 100_000,
			});

			await source.start(async () => {});
			expect(dialect.ensureCapture).toHaveBeenCalledWith(["users", "orders"]);

			await source.stop();
		});

		it("passes null to ensureCapture when no tables specified", async () => {
			const dialect = createMockDialect();
			const source = new CdcSource({ dialect, pollIntervalMs: 100_000 });

			await source.start(async () => {});
			expect(dialect.ensureCapture).toHaveBeenCalledWith(null);

			await source.stop();
		});

		it("returns error if connect fails", async () => {
			const dialect = createMockDialect({
				connect: vi.fn().mockResolvedValue({
					ok: false,
					error: new AdapterError("connection failed"),
				}),
			});
			const source = new CdcSource({ dialect });

			const result = await source.start(async () => {});
			expect(result.ok).toBe(false);
		});

		it("returns error if ensureCapture fails", async () => {
			const dialect = createMockDialect({
				ensureCapture: vi.fn().mockResolvedValue({
					ok: false,
					error: new AdapterError("capture failed"),
				}),
			});
			const source = new CdcSource({ dialect });

			const result = await source.start(async () => {});
			expect(result.ok).toBe(false);
		});
	});

	describe("stop", () => {
		it("calls dialect.close()", async () => {
			const dialect = createMockDialect();
			const source = new CdcSource({ dialect, pollIntervalMs: 100_000 });

			await source.start(async () => {});
			await source.stop();
			expect(dialect.close).toHaveBeenCalledOnce();
		});

		it("is idempotent", async () => {
			const dialect = createMockDialect();
			const source = new CdcSource({ dialect });

			await source.stop();
			await source.stop();
			// close called twice is fine — dialect should handle idempotency
		});
	});

	describe("discoverSchemas", () => {
		it("delegates to dialect.discoverSchemas()", async () => {
			const schemas: TableSchema[] = [
				{ table: "users", columns: [{ name: "id", type: "number" }] },
			];
			const dialect = createMockDialect({
				discoverSchemas: vi.fn().mockResolvedValue(Ok(schemas)),
			});
			const source = new CdcSource({ dialect, tables: ["users"] });

			const result = await source.discoverSchemas();
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toEqual(schemas);
			}
			expect(dialect.discoverSchemas).toHaveBeenCalledWith(["users"]);
		});
	});

	describe("polling cycle", () => {
		it("delivers deltas from dialect changes via onDeltas", async () => {
			const changes: CdcRawChange[] = [
				{
					kind: "insert",
					schema: "public",
					table: "users",
					rowId: "1",
					columns: [{ column: "name", value: "Alice" }],
				},
			];

			let callCount = 0;
			const dialect = createMockDialect({
				fetchChanges: vi.fn().mockResolvedValue(Ok({ changes, cursor: { seq: 1 } })),
			});

			const receivedDeltas: unknown[] = [];
			const source = new CdcSource({ dialect, pollIntervalMs: 10 });

			await source.start(async (deltas) => {
				receivedDeltas.push(...deltas);
				callCount++;
			});

			// Wait for at least one poll cycle
			await new Promise((r) => setTimeout(r, 50));
			await source.stop();

			expect(callCount).toBeGreaterThanOrEqual(1);
			expect(receivedDeltas.length).toBeGreaterThanOrEqual(1);

			const delta = receivedDeltas[0] as { op: string; table: string; rowId: string };
			expect(delta.op).toBe("INSERT");
			expect(delta.table).toBe("users");
			expect(delta.rowId).toBe("1");
		});

		it("updates cursor after each successful poll", async () => {
			const dialect = createMockDialect({
				fetchChanges: vi.fn().mockResolvedValue(Ok({ changes: [], cursor: { seq: 42 } })),
			});

			const source = new CdcSource({ dialect, pollIntervalMs: 10 });
			await source.start(async () => {});

			await new Promise((r) => setTimeout(r, 50));
			await source.stop();

			expect(source.getCursor()).toEqual({ seq: 42 });
		});

		it("does not call onDeltas when there are no changes", async () => {
			const dialect = createMockDialect({
				fetchChanges: vi.fn().mockResolvedValue(Ok({ changes: [], cursor: { seq: 0 } })),
			});

			const onDeltas = vi.fn();
			const source = new CdcSource({ dialect, pollIntervalMs: 10 });
			await source.start(onDeltas);

			await new Promise((r) => setTimeout(r, 50));
			await source.stop();

			expect(onDeltas).not.toHaveBeenCalled();
		});
	});
});

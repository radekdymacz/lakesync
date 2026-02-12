import type { DatabaseAdapter } from "@lakesync/adapter";
import type { HLCTimestamp, RowDelta } from "@lakesync/core";
import { Err, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { SharedBuffer } from "../shared-buffer";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createMockAdapter(overrides: Partial<DatabaseAdapter> = {}): DatabaseAdapter {
	return {
		insertDeltas: vi.fn<DatabaseAdapter["insertDeltas"]>().mockResolvedValue(Ok(undefined)),
		queryDeltasSince: vi.fn<DatabaseAdapter["queryDeltasSince"]>().mockResolvedValue(Ok([])),
		getLatestState: vi.fn<DatabaseAdapter["getLatestState"]>().mockResolvedValue(Ok(null)),
		ensureSchema: vi.fn<DatabaseAdapter["ensureSchema"]>().mockResolvedValue(Ok(undefined)),
		close: vi.fn<DatabaseAdapter["close"]>().mockResolvedValue(undefined),
		...overrides,
	};
}

function makeDelta(overrides: Partial<RowDelta> = {}): RowDelta {
	return {
		deltaId: overrides.deltaId ?? crypto.randomUUID(),
		table: overrides.table ?? "tasks",
		rowId: overrides.rowId ?? crypto.randomUUID(),
		clientId: overrides.clientId ?? "client-1",
		hlc: overrides.hlc ?? ((BigInt(Date.now()) << 16n) as HLCTimestamp),
		op: overrides.op ?? "INSERT",
		columns: overrides.columns ?? [{ column: "title", value: "Test" }],
	};
}

// ---------------------------------------------------------------------------
// SharedBuffer consistency mode
// ---------------------------------------------------------------------------

describe("SharedBuffer", () => {
	describe("default mode is eventual (backwards compatible)", () => {
		it("defaults to eventual consistency", async () => {
			const adapter = createMockAdapter({
				insertDeltas: vi.fn().mockRejectedValue(new Error("db down")),
			});
			const buffer = new SharedBuffer(adapter);

			const result = await buffer.writeThroughPush([makeDelta()]);

			expect(result.ok).toBe(true);
		});
	});

	describe("eventual mode", () => {
		it("returns Ok when write succeeds", async () => {
			const adapter = createMockAdapter();
			const buffer = new SharedBuffer(adapter, { consistencyMode: "eventual" });

			const result = await buffer.writeThroughPush([makeDelta()]);

			expect(result.ok).toBe(true);
			expect(adapter.insertDeltas).toHaveBeenCalledOnce();
		});

		it("returns Ok when insertDeltas returns Err", async () => {
			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
			const adapter = createMockAdapter({
				insertDeltas: vi
					.fn()
					.mockResolvedValue(Err({ code: "ADAPTER_ERROR", message: "write failed" })),
			});
			const buffer = new SharedBuffer(adapter, { consistencyMode: "eventual" });

			const result = await buffer.writeThroughPush([makeDelta()]);

			expect(result.ok).toBe(true);
			expect(warnSpy).toHaveBeenCalledWith(
				expect.stringContaining("Shared buffer write failed (eventual mode)"),
			);
			warnSpy.mockRestore();
		});

		it("returns Ok when insertDeltas throws", async () => {
			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
			const adapter = createMockAdapter({
				insertDeltas: vi.fn().mockRejectedValue(new Error("connection lost")),
			});
			const buffer = new SharedBuffer(adapter, { consistencyMode: "eventual" });

			const result = await buffer.writeThroughPush([makeDelta()]);

			expect(result.ok).toBe(true);
			expect(warnSpy).toHaveBeenCalledWith(
				expect.stringContaining("Shared buffer write error (eventual mode)"),
			);
			warnSpy.mockRestore();
		});
	});

	describe("strong mode", () => {
		it("returns Ok when write succeeds", async () => {
			const adapter = createMockAdapter();
			const buffer = new SharedBuffer(adapter, { consistencyMode: "strong" });

			const result = await buffer.writeThroughPush([makeDelta()]);

			expect(result.ok).toBe(true);
			expect(adapter.insertDeltas).toHaveBeenCalledOnce();
		});

		it("returns Err with SharedBufferError when insertDeltas returns Err", async () => {
			const adapter = createMockAdapter({
				insertDeltas: vi
					.fn()
					.mockResolvedValue(Err({ code: "ADAPTER_ERROR", message: "write failed" })),
			});
			const buffer = new SharedBuffer(adapter, { consistencyMode: "strong" });

			const result = await buffer.writeThroughPush([makeDelta()]);

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SHARED_WRITE_FAILED");
				expect(result.error.message).toBe("write failed");
			}
		});

		it("returns Err with SharedBufferError when insertDeltas throws", async () => {
			const adapter = createMockAdapter({
				insertDeltas: vi.fn().mockRejectedValue(new Error("connection lost")),
			});
			const buffer = new SharedBuffer(adapter, { consistencyMode: "strong" });

			const result = await buffer.writeThroughPush([makeDelta()]);

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SHARED_WRITE_FAILED");
				expect(result.error.message).toBe("connection lost");
			}
		});
	});
});

import type { DatabaseAdapter, HLCTimestamp, RowDelta, SyncResponse } from "@lakesync/core";
import { Err, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { AdapterBasedLock } from "../cluster";
import { SharedBuffer } from "../shared-buffer";

// ---------------------------------------------------------------------------
// Mock DatabaseAdapter
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
// AdapterBasedLock
// ---------------------------------------------------------------------------

describe("AdapterBasedLock", () => {
	it("acquire returns true when insertDeltas succeeds", async () => {
		const adapter = createMockAdapter();
		const lock = new AdapterBasedLock(adapter, "instance-1");

		const acquired = await lock.acquire("flush:gw-1", 30_000);

		expect(acquired).toBe(true);
		expect(adapter.insertDeltas).toHaveBeenCalledOnce();
	});

	it("acquire returns false when insertDeltas fails", async () => {
		const adapter = createMockAdapter({
			insertDeltas: vi.fn().mockResolvedValue(Err({ code: "ADAPTER_ERROR", message: "conflict" })),
		});
		const lock = new AdapterBasedLock(adapter, "instance-1");

		const acquired = await lock.acquire("flush:gw-1", 30_000);

		expect(acquired).toBe(false);
	});

	it("acquire returns false when insertDeltas throws", async () => {
		const adapter = createMockAdapter({
			insertDeltas: vi.fn().mockRejectedValue(new Error("connection lost")),
		});
		const lock = new AdapterBasedLock(adapter, "instance-1");

		const acquired = await lock.acquire("flush:gw-1", 30_000);

		expect(acquired).toBe(false);
	});

	it("release calls insertDeltas with DELETE op", async () => {
		const adapter = createMockAdapter();
		const lock = new AdapterBasedLock(adapter, "instance-1");

		await lock.release("flush:gw-1");

		expect(adapter.insertDeltas).toHaveBeenCalledOnce();
		const deltas = (adapter.insertDeltas as ReturnType<typeof vi.fn>).mock
			.calls[0]![0] as RowDelta[];
		expect(deltas[0]!.op).toBe("DELETE");
		expect(deltas[0]!.table).toBe("__lakesync_locks");
	});

	it("release does not throw when insertDeltas fails", async () => {
		const adapter = createMockAdapter({
			insertDeltas: vi.fn().mockRejectedValue(new Error("connection lost")),
		});
		const lock = new AdapterBasedLock(adapter, "instance-1");

		// Should not throw
		await lock.release("flush:gw-1");
	});
});

// ---------------------------------------------------------------------------
// SharedBuffer
// ---------------------------------------------------------------------------

describe("SharedBuffer", () => {
	describe("writeThroughPush", () => {
		it("writes deltas to the shared adapter", async () => {
			const adapter = createMockAdapter();
			const buffer = new SharedBuffer(adapter);
			const deltas = [makeDelta(), makeDelta()];

			const result = await buffer.writeThroughPush(deltas);

			expect(result.ok).toBe(true);
			expect(adapter.insertDeltas).toHaveBeenCalledOnce();
			expect(adapter.insertDeltas).toHaveBeenCalledWith(deltas);
		});

		it("does not throw when adapter fails (eventual mode)", async () => {
			const adapter = createMockAdapter({
				insertDeltas: vi.fn().mockRejectedValue(new Error("db down")),
			});
			const buffer = new SharedBuffer(adapter);

			const result = await buffer.writeThroughPush([makeDelta()]);

			expect(result.ok).toBe(true);
		});
	});

	describe("mergePull", () => {
		it("returns local result when adapter returns no additional deltas", async () => {
			const adapter = createMockAdapter({
				queryDeltasSince: vi.fn().mockResolvedValue(Ok([])),
			});
			const buffer = new SharedBuffer(adapter);

			const localResult: SyncResponse = {
				deltas: [makeDelta({ deltaId: "d1" })],
				serverHlc: (1n << 16n) as HLCTimestamp,
				hasMore: false,
			};

			const merged = await buffer.mergePull(localResult, 0n as HLCTimestamp);

			expect(merged.deltas).toHaveLength(1);
			expect(merged.deltas[0]!.deltaId).toBe("d1");
		});

		it("deduplicates by deltaId", async () => {
			const hlc1 = (100n << 16n) as HLCTimestamp;
			const hlc2 = (200n << 16n) as HLCTimestamp;

			const sharedDelta = makeDelta({ deltaId: "shared", hlc: hlc1 });
			const remoteDelta = makeDelta({ deltaId: "remote-only", hlc: hlc2 });

			const adapter = createMockAdapter({
				queryDeltasSince: vi.fn().mockResolvedValue(Ok([sharedDelta, remoteDelta])),
			});
			const buffer = new SharedBuffer(adapter);

			const localResult: SyncResponse = {
				deltas: [makeDelta({ deltaId: "shared", hlc: hlc1 })],
				serverHlc: hlc2,
				hasMore: false,
			};

			const merged = await buffer.mergePull(localResult, 0n as HLCTimestamp);

			// Should have 2 deltas: "shared" from local + "remote-only" from adapter
			expect(merged.deltas).toHaveLength(2);
			const ids = merged.deltas.map((d) => d.deltaId);
			expect(ids).toContain("shared");
			expect(ids).toContain("remote-only");
		});

		it("sorts merged deltas by HLC", async () => {
			const hlc1 = (100n << 16n) as HLCTimestamp;
			const hlc2 = (200n << 16n) as HLCTimestamp;
			const hlc3 = (300n << 16n) as HLCTimestamp;

			const adapter = createMockAdapter({
				queryDeltasSince: vi.fn().mockResolvedValue(Ok([makeDelta({ deltaId: "d3", hlc: hlc3 })])),
			});
			const buffer = new SharedBuffer(adapter);

			const localResult: SyncResponse = {
				deltas: [makeDelta({ deltaId: "d1", hlc: hlc1 }), makeDelta({ deltaId: "d2", hlc: hlc2 })],
				serverHlc: hlc3,
				hasMore: false,
			};

			const merged = await buffer.mergePull(localResult, 0n as HLCTimestamp);

			expect(merged.deltas).toHaveLength(3);
			expect(merged.deltas[0]!.deltaId).toBe("d1");
			expect(merged.deltas[1]!.deltaId).toBe("d2");
			expect(merged.deltas[2]!.deltaId).toBe("d3");
		});

		it("returns local result when adapter query fails", async () => {
			const adapter = createMockAdapter({
				queryDeltasSince: vi
					.fn()
					.mockResolvedValue(Err({ code: "ADAPTER_ERROR", message: "fail" })),
			});
			const buffer = new SharedBuffer(adapter);

			const localResult: SyncResponse = {
				deltas: [makeDelta({ deltaId: "d1" })],
				serverHlc: (1n << 16n) as HLCTimestamp,
				hasMore: false,
			};

			const merged = await buffer.mergePull(localResult, 0n as HLCTimestamp);

			expect(merged.deltas).toHaveLength(1);
			expect(merged.deltas[0]!.deltaId).toBe("d1");
		});

		it("returns local result when adapter throws", async () => {
			const adapter = createMockAdapter({
				queryDeltasSince: vi.fn().mockRejectedValue(new Error("connection lost")),
			});
			const buffer = new SharedBuffer(adapter);

			const localResult: SyncResponse = {
				deltas: [makeDelta({ deltaId: "d1" })],
				serverHlc: (1n << 16n) as HLCTimestamp,
				hasMore: false,
			};

			const merged = await buffer.mergePull(localResult, 0n as HLCTimestamp);

			expect(merged.deltas).toHaveLength(1);
		});
	});
});

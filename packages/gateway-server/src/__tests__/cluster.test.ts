import type { DatabaseAdapter, HLCTimestamp, RowDelta, SyncResponse } from "@lakesync/core";
import { Err, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import {
	AdapterBasedLock,
	type LockStore,
	PostgresAdvisoryLock,
	type PostgresConnection,
} from "../cluster";
import { SharedBuffer } from "../shared-buffer";

// ---------------------------------------------------------------------------
// Mock LockStore
// ---------------------------------------------------------------------------

function createMockLockStore(overrides: Partial<LockStore> = {}): LockStore {
	return {
		tryAcquire: vi.fn().mockResolvedValue(true),
		release: vi.fn().mockResolvedValue(undefined),
		...overrides,
	};
}

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
	it("acquire returns true when store.tryAcquire succeeds", async () => {
		const store = createMockLockStore();
		const lock = new AdapterBasedLock(store, "instance-1");

		const acquired = await lock.acquire("flush:gw-1", 30_000);

		expect(acquired).toBe(true);
		expect(store.tryAcquire).toHaveBeenCalledOnce();
		expect(store.tryAcquire).toHaveBeenCalledWith("flush:gw-1", 30_000, "instance-1");
	});

	it("acquire returns false when store.tryAcquire returns false", async () => {
		const store = createMockLockStore({
			tryAcquire: vi.fn().mockResolvedValue(false),
		});
		const lock = new AdapterBasedLock(store, "instance-1");

		const acquired = await lock.acquire("flush:gw-1", 30_000);

		expect(acquired).toBe(false);
	});

	it("acquire returns false when store.tryAcquire throws", async () => {
		const store = createMockLockStore({
			tryAcquire: vi.fn().mockRejectedValue(new Error("connection lost")),
		});
		const lock = new AdapterBasedLock(store, "instance-1");

		const acquired = await lock.acquire("flush:gw-1", 30_000);

		expect(acquired).toBe(false);
	});

	it("release delegates to store.release with holderId", async () => {
		const store = createMockLockStore();
		const lock = new AdapterBasedLock(store, "instance-1");

		await lock.release("flush:gw-1");

		expect(store.release).toHaveBeenCalledOnce();
		expect(store.release).toHaveBeenCalledWith("flush:gw-1", "instance-1");
	});

	it("release does not throw when store.release fails", async () => {
		const store = createMockLockStore({
			release: vi.fn().mockRejectedValue(new Error("connection lost")),
		});
		const lock = new AdapterBasedLock(store, "instance-1");

		// Should not throw
		await lock.release("flush:gw-1");
	});
});

// ---------------------------------------------------------------------------
// Mock PostgresConnection
// ---------------------------------------------------------------------------

function createMockPgConn(overrides: Partial<PostgresConnection> = {}): PostgresConnection {
	return {
		query: vi.fn().mockResolvedValue({ rows: [{ acquired: true }] }),
		...overrides,
	};
}

// ---------------------------------------------------------------------------
// PostgresAdvisoryLock
// ---------------------------------------------------------------------------

describe("PostgresAdvisoryLock", () => {
	it("acquire returns true when pg_try_advisory_lock succeeds", async () => {
		const conn = createMockPgConn();
		const lock = new PostgresAdvisoryLock(conn);

		const acquired = await lock.acquire("flush:gw-1", 30_000);

		expect(acquired).toBe(true);
		expect(conn.query).toHaveBeenCalledOnce();
		const [sql, params] = (conn.query as ReturnType<typeof vi.fn>).mock.calls[0]!;
		expect(sql).toContain("pg_try_advisory_lock");
		expect(params).toHaveLength(2);
		expect(typeof params[0]).toBe("number");
		expect(typeof params[1]).toBe("number");
	});

	it("acquire returns false when pg_try_advisory_lock returns false", async () => {
		const conn = createMockPgConn({
			query: vi.fn().mockResolvedValue({ rows: [{ acquired: false }] }),
		});
		const lock = new PostgresAdvisoryLock(conn);

		const acquired = await lock.acquire("flush:gw-1", 30_000);

		expect(acquired).toBe(false);
	});

	it("acquire returns false when query throws", async () => {
		const conn = createMockPgConn({
			query: vi.fn().mockRejectedValue(new Error("connection lost")),
		});
		const lock = new PostgresAdvisoryLock(conn);

		const acquired = await lock.acquire("flush:gw-1", 30_000);

		expect(acquired).toBe(false);
	});

	it("release calls pg_advisory_unlock", async () => {
		const conn = createMockPgConn();
		const lock = new PostgresAdvisoryLock(conn);

		await lock.acquire("flush:gw-1", 30_000);
		await lock.release("flush:gw-1");

		expect(conn.query).toHaveBeenCalledTimes(2);
		const [sql] = (conn.query as ReturnType<typeof vi.fn>).mock.calls[1]!;
		expect(sql).toContain("pg_advisory_unlock");
	});

	it("release does not throw when query fails", async () => {
		const queryFn = vi
			.fn()
			.mockResolvedValueOnce({ rows: [{ acquired: true }] })
			.mockRejectedValueOnce(new Error("connection lost"));
		const conn = createMockPgConn({ query: queryFn });
		const lock = new PostgresAdvisoryLock(conn);

		await lock.acquire("flush:gw-1", 30_000);
		// Should not throw
		await lock.release("flush:gw-1");
	});

	it("produces deterministic hash keys for the same input", async () => {
		const conn = createMockPgConn();
		const lock = new PostgresAdvisoryLock(conn);

		await lock.acquire("test-key", 1000);
		await lock.acquire("test-key", 1000);

		const calls = (conn.query as ReturnType<typeof vi.fn>).mock.calls;
		expect(calls[0]![1]).toEqual(calls[1]![1]);
	});

	it("produces different hash keys for different inputs", async () => {
		const conn = createMockPgConn();
		const lock = new PostgresAdvisoryLock(conn);

		await lock.acquire("key-a", 1000);
		await lock.acquire("key-b", 1000);

		const calls = (conn.query as ReturnType<typeof vi.fn>).mock.calls;
		const [k1a, k2a] = calls[0]![1] as [number, number];
		const [k1b, k2b] = calls[1]![1] as [number, number];
		// At least one of the pair should differ
		expect(k1a === k1b && k2a === k2b).toBe(false);
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

import type { RowDelta, SyncPull } from "@lakesync/core";
import { Err, HLC, LakeSyncError as LakeSyncErrorClass, Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { LocalDB } from "../../db/local-db";
import type { SyncQueue } from "../../queue/types";
import { SyncCoordinator } from "../coordinator";
import type { SyncTransport } from "../transport";
import { HttpTransport } from "../transport-http";
import type { LocalGateway } from "../transport-local";
import { LocalTransport } from "../transport-local";

// ────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────

function makeDelta(overrides?: Partial<RowDelta>): RowDelta {
	return {
		op: "INSERT",
		table: "todos",
		rowId: "row-1",
		clientId: "test-client",
		columns: [{ column: "title", value: "Buy milk" }],
		hlc: HLC.encode(1_000_000, 0),
		deltaId: "delta-1",
		...overrides,
	};
}

function mockQueue(): SyncQueue {
	return {
		push: vi.fn().mockResolvedValue(Ok(undefined)),
		peek: vi.fn().mockResolvedValue(Ok([])),
		markSending: vi.fn().mockResolvedValue(Ok(undefined)),
		ack: vi.fn().mockResolvedValue(Ok(undefined)),
		nack: vi.fn().mockResolvedValue(Ok(undefined)),
		depth: vi.fn().mockResolvedValue(Ok(0)),
		clear: vi.fn().mockResolvedValue(Ok(undefined)),
	} as unknown as SyncQueue;
}

function mockTransport(): SyncTransport {
	return {
		push: vi
			.fn<SyncTransport["push"]>()
			.mockResolvedValue(Ok({ serverHlc: HLC.encode(2_000_000, 0), accepted: 1 })),
		pull: vi
			.fn<SyncTransport["pull"]>()
			.mockResolvedValue(Ok({ deltas: [], serverHlc: HLC.encode(2_000_000, 0), hasMore: false })),
	};
}

function mockLocalDB(): LocalDB {
	return {
		exec: vi.fn().mockResolvedValue(Ok(undefined)),
		query: vi.fn().mockResolvedValue(Ok([])),
		name: "test-source",
		backend: "memory",
		close: vi.fn().mockResolvedValue(undefined),
		save: vi.fn().mockResolvedValue(Ok(undefined)),
		transaction: vi.fn().mockResolvedValue(Ok(undefined)),
	} as unknown as LocalDB;
}

const CLIENT_ID = "source-test-client";
const SERVER_HLC = HLC.encode(5_000_000, 0);

// ────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────

describe("SyncCoordinator source support", () => {
	let db: LocalDB;
	let transport: SyncTransport;
	let queue: SyncQueue;
	let coordinator: SyncCoordinator;

	beforeEach(() => {
		db = mockLocalDB();
		transport = mockTransport();
		queue = mockQueue();

		coordinator = new SyncCoordinator(db, transport, {
			queue,
			hlc: new HLC(() => 1_000_000),
			clientId: CLIENT_ID,
		});
	});

	describe("pullFrom", () => {
		it("constructs SyncPull with source field set", async () => {
			await coordinator.pullFrom("bigquery");

			expect(transport.pull).toHaveBeenCalledOnce();
			const pullArg = (transport.pull as ReturnType<typeof vi.fn>).mock.calls[0]?.[0] as SyncPull;
			expect(pullArg.source).toBe("bigquery");
			expect(pullArg.clientId).toBe(CLIENT_ID);
			expect(pullArg.maxDeltas).toBe(1000);
		});

		it("applies returned deltas from adapter source", async () => {
			const remoteDelta = makeDelta({
				clientId: "remote",
				hlc: HLC.encode(4_000_000, 0),
				deltaId: "bq-delta-1",
			});

			(transport.pull as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok({ deltas: [remoteDelta], serverHlc: SERVER_HLC, hasMore: false }),
			);

			const applied = await coordinator.pullFrom("bigquery");
			expect(applied).toBe(1);
		});

		it("propagates adapter errors", async () => {
			const adapterErr = new LakeSyncErrorClass("BigQuery auth failed", "ADAPTER_ERROR");
			(transport.pull as ReturnType<typeof vi.fn>).mockResolvedValue(Err(adapterErr));

			const applied = await coordinator.pullFrom("bigquery");
			expect(applied).toBe(0);
		});
	});

	describe("pullFromGateway backwards compatibility", () => {
		it("source is undefined when called without arguments", async () => {
			await coordinator.pullFromGateway();

			expect(transport.pull).toHaveBeenCalledOnce();
			const pullArg = (transport.pull as ReturnType<typeof vi.fn>).mock.calls[0]?.[0] as SyncPull;
			expect(pullArg.source).toBeUndefined();
		});

		it("source is set when called with explicit argument", async () => {
			await coordinator.pullFromGateway("postgres");

			const pullArg = (transport.pull as ReturnType<typeof vi.fn>).mock.calls[0]?.[0] as SyncPull;
			expect(pullArg.source).toBe("postgres");
		});
	});
});

describe("HttpTransport source query param", () => {
	function createMockFetch(response: { status: number; body: unknown }) {
		return vi.fn().mockResolvedValue({
			ok: response.status >= 200 && response.status < 300,
			status: response.status,
			text: () =>
				Promise.resolve(
					JSON.stringify(response.body, (_k, v) => (typeof v === "bigint" ? v.toString() : v)),
				),
		});
	}

	it("includes source as query param when set", async () => {
		const mockFetch = createMockFetch({
			status: 200,
			body: { deltas: [], serverHlc: SERVER_HLC, hasMore: false },
		});

		const transport = new HttpTransport({
			baseUrl: "https://gateway.example.com",
			gatewayId: "gw-1",
			token: "test-token",
			fetch: mockFetch,
		});

		await transport.pull({
			clientId: "client-1",
			sinceHlc: HLC.encode(1_000_000, 0),
			maxDeltas: 100,
			source: "bigquery",
		});

		const url = new URL(mockFetch.mock.calls[0]![0] as string);
		expect(url.searchParams.get("source")).toBe("bigquery");
	});

	it("omits source query param when not set", async () => {
		const mockFetch = createMockFetch({
			status: 200,
			body: { deltas: [], serverHlc: SERVER_HLC, hasMore: false },
		});

		const transport = new HttpTransport({
			baseUrl: "https://gateway.example.com",
			gatewayId: "gw-1",
			token: "test-token",
			fetch: mockFetch,
		});

		await transport.pull({
			clientId: "client-1",
			sinceHlc: HLC.encode(1_000_000, 0),
			maxDeltas: 100,
		});

		const url = new URL(mockFetch.mock.calls[0]![0] as string);
		expect(url.searchParams.has("source")).toBe(false);
	});
});

describe("LocalTransport pullFromBuffer passthrough", () => {
	it("passes pull message through to gateway pullFromBuffer", async () => {
		const gateway: LocalGateway = {
			handlePush: vi.fn().mockReturnValue(Ok({ serverHlc: HLC.encode(2_000_000, 0), accepted: 0 })),
			pullFromBuffer: vi
				.fn()
				.mockReturnValue(Ok({ deltas: [], serverHlc: HLC.encode(2_000_000, 0), hasMore: false })),
		};

		const transport = new LocalTransport(gateway);

		const pullMsg: SyncPull = {
			clientId: "client-1",
			sinceHlc: HLC.encode(1_000_000, 0),
			maxDeltas: 100,
		};

		await transport.pull(pullMsg);

		expect(gateway.pullFromBuffer).toHaveBeenCalledWith(pullMsg);
	});

	it("pullFromBuffer can return LakeSyncError", async () => {
		const adapterErr = new LakeSyncErrorClass("Connection refused", "ADAPTER_ERROR");
		const gateway: LocalGateway = {
			handlePush: vi.fn().mockReturnValue(Ok({ serverHlc: HLC.encode(2_000_000, 0), accepted: 0 })),
			pullFromBuffer: vi.fn().mockReturnValue(Err(adapterErr)),
		};

		const transport = new LocalTransport(gateway);

		const result = await transport.pull({
			clientId: "client-1",
			sinceHlc: HLC.encode(1_000_000, 0),
			maxDeltas: 100,
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("ADAPTER_ERROR");
		}
	});
});

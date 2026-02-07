import type { SyncPull, SyncPush, SyncResponse } from "@lakesync/core";
import { Err, HLC, LakeSyncError, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import type { LocalGateway } from "../transport-local";
import { LocalTransport } from "../transport-local";

const TEST_HLC = HLC.encode(1_700_000_000_000, 1);
const SERVER_HLC = HLC.encode(1_700_000_000_000, 5);

function createPushMsg(): SyncPush {
	return {
		clientId: "client-1",
		deltas: [],
		lastSeenHlc: TEST_HLC,
	};
}

function createPullMsg(): SyncPull {
	return {
		clientId: "client-1",
		sinceHlc: TEST_HLC,
		maxDeltas: 100,
	};
}

describe("LocalTransport", () => {
	describe("push", () => {
		it("delegates to gateway.handlePush", async () => {
			const expected = Ok({ serverHlc: SERVER_HLC, accepted: 3 });
			const gateway: LocalGateway = {
				handlePush: vi.fn().mockReturnValue(expected),
				handlePull: vi.fn(),
			};
			const transport = new LocalTransport(gateway);
			const msg = createPushMsg();

			const result = await transport.push(msg);

			expect(gateway.handlePush).toHaveBeenCalledTimes(1);
			expect(gateway.handlePush).toHaveBeenCalledWith(msg);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.serverHlc).toBe(SERVER_HLC);
			expect(result.value.accepted).toBe(3);
		});

		it("propagates gateway error", async () => {
			const error = new LakeSyncError("Push failed", "GATEWAY_ERROR");
			const expected = Err(error);
			const gateway: LocalGateway = {
				handlePush: vi.fn().mockReturnValue(expected),
				handlePull: vi.fn(),
			};
			const transport = new LocalTransport(gateway);

			const result = await transport.push(createPushMsg());

			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error).toBe(error);
			expect(result.error.code).toBe("GATEWAY_ERROR");
			expect(result.error.message).toBe("Push failed");
		});
	});

	describe("pull", () => {
		it("delegates to gateway.handlePull", async () => {
			const deltaHlc = HLC.encode(1_700_000_000_000, 2);
			const response: SyncResponse = {
				deltas: [
					{
						op: "INSERT",
						table: "todos",
						rowId: "row-1",
						clientId: "client-2",
						columns: [{ column: "title", value: "Hello" }],
						hlc: deltaHlc,
						deltaId: "delta-abc",
					},
				],
				serverHlc: SERVER_HLC,
				hasMore: true,
			};
			const expected = Ok(response);
			const gateway: LocalGateway = {
				handlePush: vi.fn(),
				handlePull: vi.fn().mockReturnValue(expected),
			};
			const transport = new LocalTransport(gateway);
			const msg = createPullMsg();

			const result = await transport.pull(msg);

			expect(gateway.handlePull).toHaveBeenCalledTimes(1);
			expect(gateway.handlePull).toHaveBeenCalledWith(msg);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.serverHlc).toBe(SERVER_HLC);
			expect(result.value.hasMore).toBe(true);
			expect(result.value.deltas).toHaveLength(1);
			expect(result.value.deltas[0]?.op).toBe("INSERT");
			expect(result.value.deltas[0]?.table).toBe("todos");
			expect(result.value.deltas[0]?.hlc).toBe(deltaHlc);
		});

		it("returns gateway result unchanged", async () => {
			const response: SyncResponse = {
				deltas: [],
				serverHlc: SERVER_HLC,
				hasMore: false,
			};
			const gatewayResult = Ok(response);
			const gateway: LocalGateway = {
				handlePush: vi.fn(),
				handlePull: vi.fn().mockReturnValue(gatewayResult),
			};
			const transport = new LocalTransport(gateway);

			const result = await transport.pull(createPullMsg());

			// The promise wrapping should not alter the result shape
			expect(result).toEqual(gatewayResult);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value).toBe(response);
			expect(result.value.deltas).toEqual([]);
			expect(result.value.serverHlc).toBe(SERVER_HLC);
			expect(result.value.hasMore).toBe(false);
		});
	});
});

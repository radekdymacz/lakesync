import type { SyncPull, SyncPush } from "@lakesync/core";
import { HLC } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { HttpTransport } from "../transport-http";

/** Create a mock fetch that returns a fixed status and body */
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

/** Create an HttpTransport wired to a mock fetch */
function createTransport(mockFetch: ReturnType<typeof createMockFetch>) {
	return new HttpTransport({
		baseUrl: "https://gateway.example.com",
		gatewayId: "gw-1",
		token: "test-token-abc",
		fetch: mockFetch,
	});
}

const TEST_HLC = HLC.encode(1_700_000_000_000, 1);
const SERVER_HLC = HLC.encode(1_700_000_000_000, 5);

describe("HttpTransport", () => {
	describe("push", () => {
		it("returns Ok with parsed serverHlc on 200", async () => {
			const mockFetch = createMockFetch({
				status: 200,
				body: { serverHlc: SERVER_HLC, accepted: 3 },
			});
			const transport = createTransport(mockFetch);

			const pushMsg: SyncPush = {
				clientId: "client-1",
				deltas: [],
				lastSeenHlc: TEST_HLC,
			};

			const result = await transport.push(pushMsg);

			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.serverHlc).toBe(SERVER_HLC);
			expect(typeof result.value.serverHlc).toBe("bigint");
			expect(result.value.accepted).toBe(3);
		});

		it("sends Bearer token in Authorization header", async () => {
			const mockFetch = createMockFetch({
				status: 200,
				body: { serverHlc: SERVER_HLC, accepted: 0 },
			});
			const transport = createTransport(mockFetch);

			const pushMsg: SyncPush = {
				clientId: "client-1",
				deltas: [],
				lastSeenHlc: TEST_HLC,
			};

			await transport.push(pushMsg);

			expect(mockFetch).toHaveBeenCalledTimes(1);
			const callArgs = mockFetch.mock.calls[0]!;
			const url = callArgs[0] as string;
			const options = callArgs[1] as RequestInit;

			expect(url).toBe("https://gateway.example.com/sync/gw-1/push");
			expect(options.method).toBe("POST");
			expect(options.headers).toEqual(
				expect.objectContaining({
					Authorization: "Bearer test-token-abc",
					"Content-Type": "application/json",
				}),
			);
		});

		it("returns Err with TRANSPORT_ERROR on 500", async () => {
			const mockFetch = createMockFetch({
				status: 500,
				body: "Internal server error",
			});
			const transport = createTransport(mockFetch);

			const pushMsg: SyncPush = {
				clientId: "client-1",
				deltas: [],
				lastSeenHlc: TEST_HLC,
			};

			const result = await transport.push(pushMsg);

			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("TRANSPORT_ERROR");
			expect(result.error.message).toContain("500");
		});
	});

	describe("pull", () => {
		it("returns Ok with deltas, serverHlc, and hasMore on 200", async () => {
			const deltaHlc = HLC.encode(1_700_000_000_000, 2);
			const mockFetch = createMockFetch({
				status: 200,
				body: {
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
				},
			});
			const transport = createTransport(mockFetch);

			const pullMsg: SyncPull = {
				clientId: "client-1",
				sinceHlc: TEST_HLC,
				maxDeltas: 100,
			};

			const result = await transport.pull(pullMsg);

			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.serverHlc).toBe(SERVER_HLC);
			expect(typeof result.value.serverHlc).toBe("bigint");
			expect(result.value.hasMore).toBe(true);
			expect(result.value.deltas).toHaveLength(1);
			expect(result.value.deltas[0]?.op).toBe("INSERT");
			expect(result.value.deltas[0]?.table).toBe("todos");
			expect(result.value.deltas[0]?.hlc).toBe(deltaHlc);
			expect(typeof result.value.deltas[0]?.hlc).toBe("bigint");
		});

		it("returns Err with TRANSPORT_ERROR on 401", async () => {
			const mockFetch = createMockFetch({
				status: 401,
				body: "Unauthorized",
			});
			const transport = createTransport(mockFetch);

			const pullMsg: SyncPull = {
				clientId: "client-1",
				sinceHlc: TEST_HLC,
				maxDeltas: 50,
			};

			const result = await transport.pull(pullMsg);

			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("TRANSPORT_ERROR");
			expect(result.error.message).toContain("401");
		});

		it("sends correct query parameters", async () => {
			const mockFetch = createMockFetch({
				status: 200,
				body: { deltas: [], serverHlc: SERVER_HLC, hasMore: false },
			});
			const transport = createTransport(mockFetch);

			const sinceHlc = HLC.encode(1_700_000_000_000, 10);
			const pullMsg: SyncPull = {
				clientId: "client-1",
				sinceHlc,
				maxDeltas: 50,
			};

			await transport.pull(pullMsg);

			expect(mockFetch).toHaveBeenCalledTimes(1);
			const callArgs = mockFetch.mock.calls[0]!;
			const url = new URL(callArgs[0] as string);

			expect(url.pathname).toBe("/sync/gw-1/pull");
			expect(url.searchParams.get("since")).toBe(sinceHlc.toString());
			expect(url.searchParams.get("clientId")).toBe("client-1");
			expect(url.searchParams.get("limit")).toBe("50");
		});
	});

	describe("network error", () => {
		it("returns Err with TRANSPORT_ERROR when fetch throws", async () => {
			const mockFetch = vi.fn().mockRejectedValue(new TypeError("Failed to fetch"));
			const transport = createTransport(mockFetch);

			const pushMsg: SyncPush = {
				clientId: "client-1",
				deltas: [],
				lastSeenHlc: TEST_HLC,
			};

			const result = await transport.push(pushMsg);

			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("TRANSPORT_ERROR");
			expect(result.error.message).toContain("Failed to fetch");
		});

		it("returns Err with TRANSPORT_ERROR when pull fetch throws", async () => {
			const mockFetch = vi.fn().mockRejectedValue(new Error("Network unreachable"));
			const transport = createTransport(mockFetch);

			const pullMsg: SyncPull = {
				clientId: "client-1",
				sinceHlc: TEST_HLC,
				maxDeltas: 100,
			};

			const result = await transport.pull(pullMsg);

			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("TRANSPORT_ERROR");
			expect(result.error.message).toContain("Network unreachable");
		});
	});

	describe("BigInt serialisation", () => {
		it("serialises bigint HLC values as strings in push request body", async () => {
			const mockFetch = createMockFetch({
				status: 200,
				body: { serverHlc: SERVER_HLC, accepted: 1 },
			});
			const transport = createTransport(mockFetch);

			const deltaHlc = HLC.encode(1_700_000_000_000, 3);
			const pushMsg: SyncPush = {
				clientId: "client-1",
				deltas: [
					{
						op: "INSERT",
						table: "todos",
						rowId: "row-1",
						clientId: "client-1",
						columns: [{ column: "title", value: "Test" }],
						hlc: deltaHlc,
						deltaId: "delta-123",
					},
				],
				lastSeenHlc: TEST_HLC,
			};

			await transport.push(pushMsg);

			expect(mockFetch).toHaveBeenCalledTimes(1);
			const callArgs = mockFetch.mock.calls[0]!;
			const body = JSON.parse(callArgs[1].body as string);

			// BigInt values should have been serialised as strings in the JSON body
			expect(typeof body.lastSeenHlc).toBe("string");
			expect(body.lastSeenHlc).toBe(TEST_HLC.toString());
			expect(typeof body.deltas[0].hlc).toBe("string");
			expect(body.deltas[0].hlc).toBe(deltaHlc.toString());
		});

		it("revives HLC string values back to bigint in push response", async () => {
			// The mock body uses bigint which gets serialised to string via createMockFetch
			const mockFetch = createMockFetch({
				status: 200,
				body: { serverHlc: SERVER_HLC, accepted: 2 },
			});
			const transport = createTransport(mockFetch);

			const pushMsg: SyncPush = {
				clientId: "client-1",
				deltas: [],
				lastSeenHlc: TEST_HLC,
			};

			const result = await transport.push(pushMsg);

			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// serverHlc should be revived from string to bigint
			expect(typeof result.value.serverHlc).toBe("bigint");
			expect(result.value.serverHlc).toBe(SERVER_HLC);
		});

		it("revives HLC string values back to bigint in pull response", async () => {
			const deltaHlc = HLC.encode(1_700_000_000_000, 7);
			const mockFetch = createMockFetch({
				status: 200,
				body: {
					deltas: [
						{
							op: "UPDATE",
							table: "todos",
							rowId: "row-1",
							clientId: "client-2",
							columns: [{ column: "completed", value: 1 }],
							hlc: deltaHlc,
							deltaId: "delta-789",
						},
					],
					serverHlc: SERVER_HLC,
					hasMore: false,
				},
			});
			const transport = createTransport(mockFetch);

			const pullMsg: SyncPull = {
				clientId: "client-1",
				sinceHlc: TEST_HLC,
				maxDeltas: 100,
			};

			const result = await transport.pull(pullMsg);

			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// All HLC fields should be revived as bigint
			expect(typeof result.value.serverHlc).toBe("bigint");
			expect(result.value.serverHlc).toBe(SERVER_HLC);
			expect(typeof result.value.deltas[0]?.hlc).toBe("bigint");
			expect(result.value.deltas[0]?.hlc).toBe(deltaHlc);
		});
	});
});

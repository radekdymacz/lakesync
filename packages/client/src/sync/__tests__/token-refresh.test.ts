import type { SyncPull, SyncPush } from "@lakesync/core";
import { HLC } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { HttpTransport } from "../transport-http";

const TEST_HLC = HLC.encode(1_700_000_000_000, 1);
const SERVER_HLC = HLC.encode(1_700_000_000_000, 5);

/** Create a mock fetch returning a fixed status and body */
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

/** Build a minimal SyncPush message */
function makePush(): SyncPush {
	return { clientId: "client-1", deltas: [], lastSeenHlc: TEST_HLC };
}

/** Build a minimal SyncPull message */
function makePull(): SyncPull {
	return { clientId: "client-1", sinceHlc: TEST_HLC, maxDeltas: 100 };
}

describe("HttpTransport token refresh", () => {
	describe("getToken callback", () => {
		it("calls getToken before each push request", async () => {
			const getToken = vi.fn().mockResolvedValue("fresh-token");
			const mockFetch = createMockFetch({
				status: 200,
				body: { serverHlc: SERVER_HLC, accepted: 0 },
			});
			const transport = new HttpTransport({
				baseUrl: "https://gw.example.com",
				gatewayId: "gw-1",
				token: "stale-token",
				getToken,
				fetch: mockFetch,
			});

			await transport.push(makePush());

			expect(getToken).toHaveBeenCalledOnce();
			const headers = mockFetch.mock.calls[0]![1].headers as Record<string, string>;
			expect(headers.Authorization).toBe("Bearer fresh-token");
		});

		it("calls getToken before each pull request", async () => {
			const getToken = vi.fn().mockResolvedValue("fresh-token");
			const mockFetch = createMockFetch({
				status: 200,
				body: { deltas: [], serverHlc: SERVER_HLC, hasMore: false },
			});
			const transport = new HttpTransport({
				baseUrl: "https://gw.example.com",
				gatewayId: "gw-1",
				token: "stale-token",
				getToken,
				fetch: mockFetch,
			});

			await transport.pull(makePull());

			expect(getToken).toHaveBeenCalledOnce();
			const headers = mockFetch.mock.calls[0]![1].headers as Record<string, string>;
			expect(headers.Authorization).toBe("Bearer fresh-token");
		});

		it("supports synchronous getToken", async () => {
			const getToken = vi.fn().mockReturnValue("sync-token");
			const mockFetch = createMockFetch({
				status: 200,
				body: { serverHlc: SERVER_HLC, accepted: 0 },
			});
			const transport = new HttpTransport({
				baseUrl: "https://gw.example.com",
				gatewayId: "gw-1",
				token: "stale-token",
				getToken,
				fetch: mockFetch,
			});

			const result = await transport.push(makePush());

			expect(result.ok).toBe(true);
			const headers = mockFetch.mock.calls[0]![1].headers as Record<string, string>;
			expect(headers.Authorization).toBe("Bearer sync-token");
		});
	});

	describe("401 retry", () => {
		it("retries push once with fresh token on 401", async () => {
			const getToken = vi
				.fn()
				.mockResolvedValueOnce("expired-token")
				.mockResolvedValueOnce("refreshed-token");

			const mockFetch = vi
				.fn()
				.mockResolvedValueOnce({
					ok: false,
					status: 401,
					text: () => Promise.resolve("Unauthorized"),
				})
				.mockResolvedValueOnce({
					ok: true,
					status: 200,
					text: () =>
						Promise.resolve(
							JSON.stringify({ serverHlc: SERVER_HLC, accepted: 1 }, (_k, v) =>
								typeof v === "bigint" ? v.toString() : v,
							),
						),
				});

			const transport = new HttpTransport({
				baseUrl: "https://gw.example.com",
				gatewayId: "gw-1",
				token: "unused",
				getToken,
				fetch: mockFetch,
			});

			const result = await transport.push(makePush());

			expect(result.ok).toBe(true);
			// getToken called twice: initial + retry
			expect(getToken).toHaveBeenCalledTimes(2);
			expect(mockFetch).toHaveBeenCalledTimes(2);
			// Second call should use the refreshed token
			const retryHeaders = mockFetch.mock.calls[1]![1].headers as Record<string, string>;
			expect(retryHeaders.Authorization).toBe("Bearer refreshed-token");
		});

		it("retries pull once with fresh token on 401", async () => {
			const getToken = vi
				.fn()
				.mockResolvedValueOnce("expired-token")
				.mockResolvedValueOnce("refreshed-token");

			const mockFetch = vi
				.fn()
				.mockResolvedValueOnce({
					ok: false,
					status: 401,
					text: () => Promise.resolve("Unauthorized"),
				})
				.mockResolvedValueOnce({
					ok: true,
					status: 200,
					text: () =>
						Promise.resolve(
							JSON.stringify({ deltas: [], serverHlc: SERVER_HLC, hasMore: false }, (_k, v) =>
								typeof v === "bigint" ? v.toString() : v,
							),
						),
				});

			const transport = new HttpTransport({
				baseUrl: "https://gw.example.com",
				gatewayId: "gw-1",
				token: "unused",
				getToken,
				fetch: mockFetch,
			});

			const result = await transport.pull(makePull());

			expect(result.ok).toBe(true);
			expect(getToken).toHaveBeenCalledTimes(2);
			expect(mockFetch).toHaveBeenCalledTimes(2);
		});

		it("does not retry on 401 when getToken is not configured", async () => {
			const mockFetch = vi.fn().mockResolvedValue({
				ok: false,
				status: 401,
				text: () => Promise.resolve("Unauthorized"),
			});

			const transport = new HttpTransport({
				baseUrl: "https://gw.example.com",
				gatewayId: "gw-1",
				token: "static-token",
				fetch: mockFetch,
			});

			const result = await transport.push(makePush());

			expect(result.ok).toBe(false);
			// Only called once — no retry
			expect(mockFetch).toHaveBeenCalledOnce();
		});

		it("returns error if retry also fails with 401", async () => {
			const getToken = vi.fn().mockResolvedValue("still-expired");

			const mockFetch = vi.fn().mockResolvedValue({
				ok: false,
				status: 401,
				text: () => Promise.resolve("Unauthorized"),
			});

			const transport = new HttpTransport({
				baseUrl: "https://gw.example.com",
				gatewayId: "gw-1",
				token: "unused",
				getToken,
				fetch: mockFetch,
			});

			const result = await transport.push(makePush());

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.message).toContain("401");
			}
			// Called twice: initial + one retry
			expect(mockFetch).toHaveBeenCalledTimes(2);
		});
	});

	describe("backward compatibility", () => {
		it("uses static token when getToken is not provided", async () => {
			const mockFetch = createMockFetch({
				status: 200,
				body: { serverHlc: SERVER_HLC, accepted: 0 },
			});

			const transport = new HttpTransport({
				baseUrl: "https://gw.example.com",
				gatewayId: "gw-1",
				token: "static-token",
				fetch: mockFetch,
			});

			await transport.push(makePush());

			const headers = mockFetch.mock.calls[0]![1].headers as Record<string, string>;
			expect(headers.Authorization).toBe("Bearer static-token");
		});
	});
});

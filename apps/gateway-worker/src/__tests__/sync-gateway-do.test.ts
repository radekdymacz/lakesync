import { describe, expect, it, vi } from "vitest";

// ── Mock cloudflare:workers module ────────────────────────────────────
// SyncGatewayDO extends DurableObject from "cloudflare:workers" which is
// not available outside the Workers runtime. We provide a minimal stub.
vi.mock("cloudflare:workers", () => {
	class DurableObject {
		protected ctx: unknown;
		protected env: unknown;
		constructor(ctx: unknown, env: unknown) {
			this.ctx = ctx;
			this.env = env;
		}
	}
	return { DurableObject };
});

// ── Mock @lakesync/gateway ────────────────────────────────────────────
const mockHandlePush = vi.fn();
const mockHandlePull = vi.fn();
const mockFlush = vi.fn();
const mockShouldFlush = vi.fn();
const mockBufferStats = { logSize: 0, byteSize: 0, indexSize: 0 };

vi.mock("@lakesync/gateway", () => {
	class MockSyncGateway {
		handlePush = mockHandlePush;
		handlePull = mockHandlePull;
		flush = mockFlush;
		shouldFlush = mockShouldFlush;
		get bufferStats() {
			return mockBufferStats;
		}
	}
	return { SyncGateway: MockSyncGateway };
});

// ── Mock @lakesync/proto ──────────────────────────────────────────────
vi.mock("@lakesync/proto", () => ({
	decodeSyncPush: vi.fn(),
	decodeSyncPull: vi.fn(),
	encodeSyncResponse: vi.fn().mockReturnValue({
		ok: true,
		value: new Uint8Array([0x00]),
	}),
}));

// ── Mock R2Adapter ────────────────────────────────────────────────────
vi.mock("../r2-adapter", () => {
	class MockR2Adapter {}
	return { R2Adapter: MockR2Adapter };
});

// Import after all mocks are set up
import { SyncGatewayDO } from "../sync-gateway-do";

/**
 * Create a mock DurableObject context with storage alarm support.
 */
function createMockCtx(): {
	id: { toString: () => string };
	storage: {
		setAlarm: ReturnType<typeof vi.fn>;
		get: ReturnType<typeof vi.fn>;
		put: ReturnType<typeof vi.fn>;
	};
	acceptWebSocket: ReturnType<typeof vi.fn>;
} {
	return {
		id: { toString: () => "do-test-id" },
		storage: {
			setAlarm: vi.fn(),
			get: vi.fn().mockResolvedValue(undefined),
			put: vi.fn().mockResolvedValue(undefined),
		},
		acceptWebSocket: vi.fn(),
	};
}

/**
 * Create a mock Env for the Durable Object.
 */
function createMockEnv(): {
	SYNC_GATEWAY: unknown;
	LAKE_BUCKET: Record<string, unknown>;
	NESSIE_URI: string;
	JWT_SECRET: string;
} {
	return {
		SYNC_GATEWAY: {},
		LAKE_BUCKET: {},
		NESSIE_URI: "http://localhost:19120",
		JWT_SECRET: "test-secret",
	};
}

/**
 * Create a SyncGatewayDO instance with mock context and env.
 */
function createDO(): SyncGatewayDO {
	const ctx = createMockCtx();
	const env = createMockEnv();
	return new SyncGatewayDO(ctx as unknown as DurableObjectState, env as unknown as never);
}

/**
 * Reset all mock gateway method implementations before each test.
 */
function resetGatewayMocks(): void {
	mockHandlePush.mockReset();
	mockHandlePull.mockReset();
	mockFlush.mockReset();
	mockShouldFlush.mockReset();
	mockShouldFlush.mockReturnValue(false);
}

describe("SyncGatewayDO", () => {
	// ── bigint serialisation (tested indirectly via HTTP handlers) ─────
	// The bigintReplacer/bigintReviver are private to the module.
	// We test them indirectly through the push/pull HTTP responses which
	// use JSON.stringify with bigintReplacer.

	describe("handlePush (POST /push)", () => {
		it("returns 200 with valid push body", async () => {
			resetGatewayMocks();
			mockHandlePush.mockReturnValue({
				ok: true,
				value: { accepted: 1, serverHlc: BigInt(1234567890) },
			});

			const DO = createDO();
			const request = new Request("https://do.example.com/push", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					clientId: "client-1",
					deltas: [
						{
							op: "UPDATE",
							table: "todos",
							rowId: "row-1",
							clientId: "client-1",
							columns: [{ column: "title", value: "Test" }],
							hlc: "1234567890",
							deltaId: "delta-1",
						},
					],
					lastSeenHlc: "0",
				}),
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(200);

			const body = (await response.json()) as Record<string, unknown>;
			expect(body.accepted).toBe(1);
		});

		it("returns 400 for invalid JSON body", async () => {
			resetGatewayMocks();

			const DO = createDO();
			const request = new Request("https://do.example.com/push", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: "not valid json{{{",
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(400);

			const body = (await response.json()) as { error: string };
			expect(body.error).toContain("Invalid JSON");
		});

		it("returns 400 when required fields are missing", async () => {
			resetGatewayMocks();

			const DO = createDO();
			const request = new Request("https://do.example.com/push", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ someField: "value" }),
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(400);

			const body = (await response.json()) as { error: string };
			expect(body.error).toContain("Missing required fields");
		});

		it("returns 405 for non-POST requests to /push", async () => {
			resetGatewayMocks();

			const DO = createDO();
			const request = new Request("https://do.example.com/push", {
				method: "GET",
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(405);
		});

		it("returns 409 on CLOCK_DRIFT error", async () => {
			resetGatewayMocks();
			mockHandlePush.mockReturnValue({
				ok: false,
				error: { code: "CLOCK_DRIFT", message: "Clock drift detected" },
			});

			const DO = createDO();
			const request = new Request("https://do.example.com/push", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					clientId: "client-1",
					deltas: [
						{
							op: "UPDATE",
							table: "t",
							rowId: "r",
							clientId: "c",
							columns: [],
							hlc: "9999999999999999",
							deltaId: "d1",
						},
					],
					lastSeenHlc: "0",
				}),
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(409);
		});

		it("returns 422 on SCHEMA_MISMATCH error", async () => {
			resetGatewayMocks();
			mockHandlePush.mockReturnValue({
				ok: false,
				error: { code: "SCHEMA_MISMATCH", message: "Schema mismatch" },
			});

			const DO = createDO();
			const request = new Request("https://do.example.com/push", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					clientId: "client-1",
					deltas: [
						{
							op: "UPDATE",
							table: "t",
							rowId: "r",
							clientId: "c",
							columns: [],
							hlc: "1",
							deltaId: "d1",
						},
					],
					lastSeenHlc: "0",
				}),
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(422);
		});
	});

	describe("handlePull (GET /pull)", () => {
		it("returns 200 with valid query params", async () => {
			resetGatewayMocks();
			mockHandlePull.mockReturnValue({
				ok: true,
				value: {
					deltas: [],
					serverHlc: BigInt(100),
					hasMore: false,
				},
			});

			const DO = createDO();
			const request = new Request(
				"https://do.example.com/pull?since=0&clientId=client-1&limit=50",
				{ method: "GET" },
			);

			const response = await DO.fetch(request);
			expect(response.status).toBe(200);

			const body = (await response.json()) as { deltas: unknown[]; hasMore: boolean };
			expect(body.deltas).toEqual([]);
			expect(body.hasMore).toBe(false);
		});

		it("returns 400 when since param is missing", async () => {
			resetGatewayMocks();

			const DO = createDO();
			const request = new Request("https://do.example.com/pull?clientId=client-1", {
				method: "GET",
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(400);

			const body = (await response.json()) as { error: string };
			expect(body.error).toContain("since");
		});

		it("returns 400 when clientId param is missing", async () => {
			resetGatewayMocks();

			const DO = createDO();
			const request = new Request("https://do.example.com/pull?since=0", { method: "GET" });

			const response = await DO.fetch(request);
			expect(response.status).toBe(400);

			const body = (await response.json()) as { error: string };
			expect(body.error).toContain("clientId");
		});

		it("returns 400 for invalid since parameter", async () => {
			resetGatewayMocks();

			const DO = createDO();
			const request = new Request(
				"https://do.example.com/pull?since=not-a-number&clientId=client-1",
				{ method: "GET" },
			);

			const response = await DO.fetch(request);
			expect(response.status).toBe(400);

			const body = (await response.json()) as { error: string };
			expect(body.error).toContain("since");
		});

		it("returns 400 for invalid limit parameter", async () => {
			resetGatewayMocks();

			const DO = createDO();
			const request = new Request(
				"https://do.example.com/pull?since=0&clientId=client-1&limit=-1",
				{ method: "GET" },
			);

			const response = await DO.fetch(request);
			expect(response.status).toBe(400);

			const body = (await response.json()) as { error: string };
			expect(body.error).toContain("limit");
		});
	});

	describe("handleFlush (POST /flush)", () => {
		it("returns 200 on successful flush", async () => {
			resetGatewayMocks();
			mockFlush.mockResolvedValue({ ok: true, value: undefined });

			const DO = createDO();
			const request = new Request("https://do.example.com/flush", {
				method: "POST",
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(200);

			const body = (await response.json()) as { flushed: boolean };
			expect(body.flushed).toBe(true);
		});

		it("returns 500 when flush fails", async () => {
			resetGatewayMocks();
			mockFlush.mockResolvedValue({
				ok: false,
				error: { message: "Flush failed: adapter error" },
			});

			const DO = createDO();
			const request = new Request("https://do.example.com/flush", {
				method: "POST",
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(500);
		});
	});

	describe("routing", () => {
		it("returns 404 for unknown paths", async () => {
			resetGatewayMocks();

			const DO = createDO();
			const request = new Request("https://do.example.com/unknown", {
				method: "GET",
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(404);

			const body = (await response.json()) as { error: string };
			expect(body.error).toContain("Not found");
		});
	});

	describe("WebSocket upgrade", () => {
		it("returns 101 on WebSocket upgrade request", async () => {
			resetGatewayMocks();

			// Mock WebSocketPair as a class constructor (CF Workers global)
			const mockClient = { close: vi.fn() };
			const mockServer = { close: vi.fn() };
			const g = globalThis as Record<string, unknown>;
			const originalWebSocketPair = g.WebSocketPair;

			g.WebSocketPair = function WebSocketPair(this: Record<number, unknown>) {
				this[0] = mockClient;
				this[1] = mockServer;
			};

			// The standard Response constructor rejects status 101,
			// but CF Workers allows it. We mock Response to allow 101.
			const OriginalResponse = globalThis.Response;
			const MockResponse = class extends OriginalResponse {
				_status: number;
				_webSocket: unknown;
				constructor(body: BodyInit | null, init?: ResponseInit & { webSocket?: unknown }) {
					// Use status 200 for the parent constructor, override status getter
					super(body, { ...init, status: 200 });
					this._status = init?.status ?? 200;
					this._webSocket = init?.webSocket;
				}
				override get status(): number {
					return this._status;
				}
			};
			globalThis.Response = MockResponse as unknown as typeof Response;

			try {
				const DO = createDO();
				const request = new Request("https://do.example.com/ws", {
					method: "GET",
					headers: { Upgrade: "websocket" },
				});

				const response = await DO.fetch(request);
				expect(response.status).toBe(101);
			} finally {
				globalThis.Response = OriginalResponse;
				if (originalWebSocketPair) {
					g.WebSocketPair = originalWebSocketPair;
				} else {
					delete g.WebSocketPair;
				}
			}
		});
	});

	describe("bigint JSON serialisation", () => {
		it("serialises BigInt values in push response via bigintReplacer", async () => {
			resetGatewayMocks();
			const bigHlc = BigInt("281474976710656"); // a realistic HLC value
			mockHandlePush.mockReturnValue({
				ok: true,
				value: { accepted: 1, serverHlc: bigHlc },
			});

			const DO = createDO();
			const request = new Request("https://do.example.com/push", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					clientId: "client-1",
					deltas: [
						{
							op: "UPDATE",
							table: "t",
							rowId: "r",
							clientId: "c",
							columns: [],
							hlc: "1",
							deltaId: "d1",
						},
					],
					lastSeenHlc: "0",
				}),
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(200);

			// The response body should contain the BigInt as a string
			const text = await response.text();
			expect(text).toContain("281474976710656");
			// It should be valid JSON (no BigInt syntax error)
			expect(() => JSON.parse(text)).not.toThrow();
		});

		it("revives hlc fields from string to BigInt in push body", async () => {
			resetGatewayMocks();
			// When the handler parses JSON with bigintReviver, fields ending
			// in "hlc" (case-insensitive) should be converted to BigInt.
			// We verify by checking what handlePush receives.
			mockHandlePush.mockImplementation((msg: { lastSeenHlc: unknown }) => {
				// lastSeenHlc should have been revived to BigInt
				expect(typeof msg.lastSeenHlc).toBe("bigint");
				return {
					ok: true,
					value: { accepted: 0, serverHlc: BigInt(0) },
				};
			});

			const DO = createDO();
			const request = new Request("https://do.example.com/push", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					clientId: "client-1",
					deltas: [],
					lastSeenHlc: "12345",
				}),
			});

			await DO.fetch(request);
			// The assertion happens inside mockHandlePush
			// but also verify it was called
			expect(mockHandlePush).toHaveBeenCalled();
		});
	});
});

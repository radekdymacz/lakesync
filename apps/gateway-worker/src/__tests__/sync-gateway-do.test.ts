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
const mockGetTablesExceedingBudget = vi.fn().mockReturnValue([]);
const mockFlushTable = vi.fn();
const mockBufferStats = { logSize: 0, byteSize: 0, indexSize: 0 };
let lastGatewayConfig: Record<string, unknown> | null = null;

vi.mock("@lakesync/gateway", async (importOriginal) => {
	const actual = await importOriginal<typeof import("@lakesync/gateway")>();
	class MockSyncGateway {
		handlePush = mockHandlePush;
		handlePull = mockHandlePull;
		flush = mockFlush;
		shouldFlush = mockShouldFlush;
		getTablesExceedingBudget = mockGetTablesExceedingBudget;
		flushTable = mockFlushTable;
		constructor(config: Record<string, unknown>) {
			lastGatewayConfig = config;
		}
		get bufferStats() {
			return mockBufferStats;
		}
	}
	return {
		...actual,
		SyncGateway: MockSyncGateway,
	};
});

// ── Mock @lakesync/proto ──────────────────────────────────────────────
vi.mock("@lakesync/proto", () => ({
	decodeSyncPush: vi.fn(),
	decodeSyncPull: vi.fn(),
	decodeSyncResponse: vi.fn(),
	encodeSyncResponse: vi.fn().mockReturnValue({
		ok: true,
		value: new Uint8Array([0x00]),
	}),
	encodeBroadcastFrame: vi.fn().mockReturnValue({
		ok: true,
		value: new Uint8Array([0x00]),
	}),
}));

// ── Mock R2Adapter ────────────────────────────────────────────────────
/**
 * In-memory store for R2 objects. Tests populate this to control what
 * the R2Adapter returns from getObject.
 */
const r2Store = new Map<string, Uint8Array>();

vi.mock("../r2-adapter", () => {
	class MockR2Adapter {
		async getObject(key: string) {
			const data = r2Store.get(key);
			if (!data) {
				return { ok: false, error: { message: `Object not found: ${key}` } };
			}
			return { ok: true, value: data };
		}
	}
	return { R2Adapter: MockR2Adapter };
});

import {
	decodeSyncPull,
	decodeSyncPush,
	decodeSyncResponse,
	encodeSyncResponse,
} from "@lakesync/proto";
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
	getWebSockets: ReturnType<typeof vi.fn>;
} {
	return {
		id: { toString: () => "do-test-id" },
		storage: {
			setAlarm: vi.fn(),
			get: vi.fn().mockResolvedValue(undefined),
			put: vi.fn().mockResolvedValue(undefined),
		},
		acceptWebSocket: vi.fn(),
		getWebSockets: vi.fn().mockReturnValue([]),
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
	lastGatewayConfig = null;
	r2Store.clear();
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
				value: { accepted: 1, serverHlc: BigInt(1234567890), deltas: [] },
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
			const mockServer = {
				close: vi.fn(),
				serializeAttachment: vi.fn(),
			};
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
					headers: {
						Upgrade: "websocket",
						"X-Auth-Claims": JSON.stringify({ sub: "client-1" }),
						"X-Client-Id": "client-1",
					},
				});

				const response = await DO.fetch(request);
				expect(response.status).toBe(101);
				// Verify claims were stored on the server WebSocket
				expect(mockServer.serializeAttachment).toHaveBeenCalledWith({
					claims: { sub: "client-1" },
					clientId: "client-1",
				});
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

	describe("webSocketMessage", () => {
		/** Create a mock WebSocket with deserializeAttachment support. */
		function createMockWs(
			clientId = "client-1",
			claims: Record<string, unknown> = { sub: "client-1" },
		): WebSocket {
			return {
				close: vi.fn(),
				send: vi.fn(),
				deserializeAttachment: vi.fn().mockReturnValue({ claims, clientId }),
			} as unknown as WebSocket;
		}

		it("text frame closes with 1003", async () => {
			resetGatewayMocks();
			const DO = createDO();
			const mockWs = createMockWs();

			await DO.webSocketMessage(mockWs, "hello text");

			expect(mockWs.close).toHaveBeenCalledWith(1003, "Binary frames only");
		});

		it("short message closes with 1002", async () => {
			resetGatewayMocks();
			const DO = createDO();
			const mockWs = createMockWs();

			await DO.webSocketMessage(mockWs, new Uint8Array([0x01]).buffer);

			expect(mockWs.close).toHaveBeenCalledWith(1002, "Message too short");
		});

		it("unknown tag closes with 1002", async () => {
			resetGatewayMocks();
			const DO = createDO();
			const mockWs = createMockWs();

			await DO.webSocketMessage(mockWs, new Uint8Array([0xff, 0x00]).buffer);

			expect(mockWs.close).toHaveBeenCalledWith(
				1002,
				expect.stringContaining("Unknown message tag"),
			);
		});

		it("tag 0x01 delegates to decodeSyncPush and handlePush", async () => {
			resetGatewayMocks();
			const DO = createDO();
			const mockWs = createMockWs();

			const mockedDecodeSyncPush = vi.mocked(decodeSyncPush);
			mockedDecodeSyncPush.mockReturnValue({
				ok: true,
				value: {
					clientId: "client-1",
					deltas: [],
					lastSeenHlc: BigInt(0) as never,
				},
			});

			mockHandlePush.mockReturnValue({
				ok: true,
				value: { accepted: 0, serverHlc: BigInt(100), deltas: [] },
			});

			const mockedEncodeSyncResponse = vi.mocked(encodeSyncResponse);
			mockedEncodeSyncResponse.mockReturnValue({
				ok: true,
				value: new Uint8Array([0xaa, 0xbb]),
			});

			const payload = new Uint8Array([0x01, 0x10, 0x20]);
			await DO.webSocketMessage(mockWs, payload.buffer);

			expect(mockedDecodeSyncPush).toHaveBeenCalled();
			expect(mockHandlePush).toHaveBeenCalled();
			expect(mockWs.send).toHaveBeenCalled();
		});

		it("tag 0x01 closes when push clientId mismatches authenticated identity", async () => {
			resetGatewayMocks();
			const DO = createDO();
			const mockWs = createMockWs("client-1");

			const mockedDecodeSyncPush = vi.mocked(decodeSyncPush);
			mockedDecodeSyncPush.mockReturnValue({
				ok: true,
				value: {
					clientId: "impersonator",
					deltas: [],
					lastSeenHlc: BigInt(0) as never,
				},
			});

			const payload = new Uint8Array([0x01, 0x10, 0x20]);
			await DO.webSocketMessage(mockWs, payload.buffer);

			expect(mockWs.close).toHaveBeenCalledWith(
				1008,
				"Client ID mismatch: push clientId does not match authenticated identity",
			);
			expect(mockHandlePush).not.toHaveBeenCalled();
		});

		it("tag 0x02 delegates to decodeSyncPull and handlePull", async () => {
			resetGatewayMocks();
			const DO = createDO();
			const mockWs = createMockWs();

			const mockedDecodeSyncPull = vi.mocked(decodeSyncPull);
			mockedDecodeSyncPull.mockReturnValue({
				ok: true,
				value: {
					clientId: "client-1",
					sinceHlc: BigInt(0) as never,
					maxDeltas: 100,
				},
			});

			mockHandlePull.mockReturnValue({
				ok: true,
				value: {
					deltas: [],
					serverHlc: BigInt(100),
					hasMore: false,
				},
			});

			const mockedEncodeSyncResponse = vi.mocked(encodeSyncResponse);
			mockedEncodeSyncResponse.mockReturnValue({
				ok: true,
				value: new Uint8Array([0xcc, 0xdd]),
			});

			const payload = new Uint8Array([0x02, 0x10, 0x20]);
			await DO.webSocketMessage(mockWs, payload.buffer);

			expect(mockedDecodeSyncPull).toHaveBeenCalled();
			expect(mockHandlePull).toHaveBeenCalled();
			expect(mockWs.send).toHaveBeenCalled();
		});
	});

	describe("alarm flush lifecycle", () => {
		/**
		 * Helper: create a DO with an explicit mock context so we can
		 * inspect setAlarm calls in alarm tests.
		 */
		function createDOWithCtx(): {
			DO: SyncGatewayDO;
			ctx: ReturnType<typeof createMockCtx>;
		} {
			const ctx = createMockCtx();
			const env = createMockEnv();
			const DO = new SyncGatewayDO(ctx as unknown as DurableObjectState, env as unknown as never);
			return { DO, ctx };
		}

		it("alarm skips when buffer is empty", async () => {
			resetGatewayMocks();
			mockBufferStats.logSize = 0;

			const { DO, ctx } = createDOWithCtx();
			await DO.alarm();

			expect(mockFlush).not.toHaveBeenCalled();
			expect(ctx.storage.setAlarm).not.toHaveBeenCalled();
		});

		it("alarm flushes when buffer has data", async () => {
			resetGatewayMocks();
			mockBufferStats.logSize = 5;

			mockFlush.mockImplementation(async () => {
				mockBufferStats.logSize = 0;
				return { ok: true, value: undefined };
			});

			const { DO, ctx } = createDOWithCtx();
			await DO.alarm();

			expect(mockFlush).toHaveBeenCalledOnce();
			expect(ctx.storage.setAlarm).not.toHaveBeenCalled();
		});

		it("alarm reschedules with backoff on flush failure", async () => {
			resetGatewayMocks();
			mockBufferStats.logSize = 5;

			mockFlush.mockResolvedValue({
				ok: false,
				error: { message: "Adapter error" },
			});

			const { DO, ctx } = createDOWithCtx();
			const now = Date.now();
			await DO.alarm();

			expect(ctx.storage.setAlarm).toHaveBeenCalledOnce();
			const alarmTime = ctx.storage.setAlarm.mock.calls[0]![0] as number;
			// First failure: backoff = BASE_RETRY_BACKOFF_MS * 2^0 = 1000ms
			const delta = alarmTime - now;
			expect(delta).toBeGreaterThanOrEqual(1000);
			expect(delta).toBeLessThanOrEqual(1100);
		});

		it("alarm backoff increases exponentially on repeated failures", async () => {
			resetGatewayMocks();
			mockBufferStats.logSize = 5;

			mockFlush.mockResolvedValue({
				ok: false,
				error: { message: "Adapter error" },
			});

			const { DO, ctx } = createDOWithCtx();

			// Call alarm 3 times to accumulate backoff
			const timestamps: number[] = [];
			for (let i = 0; i < 3; i++) {
				const now = Date.now();
				await DO.alarm();
				const alarmTime = ctx.storage.setAlarm.mock.calls[i]![0] as number;
				timestamps.push(alarmTime - now);
			}

			// Expected backoffs: ~1000ms, ~2000ms, ~4000ms
			expect(timestamps[0]).toBeGreaterThanOrEqual(1000);
			expect(timestamps[0]).toBeLessThanOrEqual(1100);
			expect(timestamps[1]).toBeGreaterThanOrEqual(2000);
			expect(timestamps[1]).toBeLessThanOrEqual(2100);
			expect(timestamps[2]).toBeGreaterThanOrEqual(4000);
			expect(timestamps[2]).toBeLessThanOrEqual(4100);
		});

		it("alarm resets backoff after success", async () => {
			resetGatewayMocks();
			mockBufferStats.logSize = 5;

			mockFlush.mockResolvedValue({
				ok: false,
				error: { message: "Adapter error" },
			});

			const { DO, ctx } = createDOWithCtx();

			// Fail twice to reach 2s backoff
			await DO.alarm();
			await DO.alarm();

			// Succeed — this should reset the backoff counter
			mockFlush.mockImplementation(async () => {
				mockBufferStats.logSize = 0;
				return { ok: true, value: undefined };
			});
			await DO.alarm();

			// Fail again — backoff should be back to 1s, not 4s
			mockBufferStats.logSize = 5;
			mockFlush.mockResolvedValue({
				ok: false,
				error: { message: "Adapter error" },
			});
			const now = Date.now();
			await DO.alarm();

			// The last setAlarm call should show ~1000ms backoff (reset)
			const lastCall = ctx.storage.setAlarm.mock.calls.at(-1)!;
			const alarmTime = lastCall[0] as number;
			const delta = alarmTime - now;
			expect(delta).toBeGreaterThanOrEqual(1000);
			expect(delta).toBeLessThanOrEqual(1100);
		});

		it("alarm reschedules immediately when buffer still has data after flush", async () => {
			resetGatewayMocks();
			mockBufferStats.logSize = 5;

			mockFlush.mockImplementation(async () => {
				// Simulate more data arriving during flush
				mockBufferStats.logSize = 3;
				return { ok: true, value: undefined };
			});

			const { DO, ctx } = createDOWithCtx();
			const now = Date.now();
			await DO.alarm();

			expect(mockFlush).toHaveBeenCalledOnce();
			expect(ctx.storage.setAlarm).toHaveBeenCalledOnce();

			// Should reschedule with Date.now() (immediate)
			const alarmTime = ctx.storage.setAlarm.mock.calls[0]![0] as number;
			const delta = alarmTime - now;
			// Date.now() is essentially immediate — allow up to 50ms tolerance
			expect(delta).toBeGreaterThanOrEqual(0);
			expect(delta).toBeLessThanOrEqual(50);
		});
	});

	describe("MAX_BUFFER_BYTES env var", () => {
		it("uses MAX_BUFFER_BYTES env var when set", async () => {
			resetGatewayMocks();
			mockFlush.mockResolvedValue({ ok: true, value: undefined });

			const ctx = createMockCtx();
			const env = {
				...createMockEnv(),
				MAX_BUFFER_BYTES: "8388608", // 8 MiB
			};
			const DO = new SyncGatewayDO(ctx as unknown as DurableObjectState, env as unknown as never);

			// Trigger gateway creation via /flush
			const request = new Request("https://do.example.com/flush", {
				method: "POST",
			});
			await DO.fetch(request);

			expect(lastGatewayConfig).not.toBeNull();
			expect(lastGatewayConfig!.maxBufferBytes).toBe(8388608);
		});

		it("uses default maxBufferBytes when MAX_BUFFER_BYTES is not set", async () => {
			resetGatewayMocks();
			mockFlush.mockResolvedValue({ ok: true, value: undefined });

			const DO = createDO();

			const request = new Request("https://do.example.com/flush", {
				method: "POST",
			});
			await DO.fetch(request);

			expect(lastGatewayConfig).not.toBeNull();
			// Default is 4 MiB = 4 * 1024 * 1024 = 4194304
			expect(lastGatewayConfig!.maxBufferBytes).toBe(4 * 1024 * 1024);
		});
	});

	describe("handleCheckpoint (GET /checkpoint)", () => {
		/** Seed R2 store with a manifest and chunk for the test DO. */
		function seedCheckpointData(snapshotHlc: string, chunkBytes: Uint8Array): void {
			const gatewayId = "do-test-id";
			const manifest = JSON.stringify({
				snapshotHlc,
				chunks: ["chunk-0.bin"],
				chunkCount: 1,
			});
			r2Store.set(`checkpoints/${gatewayId}/manifest.json`, new TextEncoder().encode(manifest));
			r2Store.set(`checkpoints/${gatewayId}/chunk-0.bin`, chunkBytes);
		}

		it("returns streaming checkpoint response", async () => {
			resetGatewayMocks();

			const snapshotHlc = "281474976710656";
			const chunkProtoBytes = new Uint8Array([0x0a, 0x0b, 0x0c]);
			seedCheckpointData(snapshotHlc, chunkProtoBytes);

			// Mock decodeSyncResponse to return a decoded chunk with deltas
			const mockDelta = {
				op: "UPDATE" as const,
				table: "todos",
				rowId: "row-1",
				clientId: "client-1",
				columns: [{ column: "title", value: "Hello" }],
				hlc: BigInt(100) as never,
				deltaId: "d1",
			};
			vi.mocked(decodeSyncResponse).mockReturnValue({
				ok: true,
				value: { deltas: [mockDelta], serverHlc: BigInt(snapshotHlc) as never, hasMore: false },
			});

			// Mock encodeSyncResponse to return deterministic frame bytes
			const frameBytes = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
			vi.mocked(encodeSyncResponse).mockReturnValue({
				ok: true,
				value: frameBytes,
			});

			const DO = createDO();
			const request = new Request("https://do.example.com/checkpoint", {
				method: "GET",
				headers: {
					Accept: "application/x-lakesync-checkpoint-stream",
				},
			});

			const response = await DO.fetch(request);

			expect(response.status).toBe(200);
			expect(response.headers.get("Content-Type")).toBe("application/x-lakesync-checkpoint-stream");
			expect(response.headers.get("X-Checkpoint-Hlc")).toBe(snapshotHlc);

			// Read the streamed body — expect 4-byte BE length prefix + frame bytes
			const body = new Uint8Array(await response.arrayBuffer());
			// Length prefix: frameBytes.length = 4 → 0x00000004 big-endian
			const expectedPrefix = new Uint8Array([0x00, 0x00, 0x00, 0x04]);
			expect(body.slice(0, 4)).toEqual(expectedPrefix);
			expect(body.slice(4)).toEqual(frameBytes);
		});
	});

	describe("handleMetrics (GET /admin/metrics)", () => {
		it("peakBufferBytes tracks high-water mark across pushes", async () => {
			resetGatewayMocks();

			// First push: buffer grows to 512 bytes
			mockBufferStats.byteSize = 512;
			mockBufferStats.logSize = 3;
			mockHandlePush.mockReturnValue({
				ok: true,
				value: { accepted: 1, serverHlc: BigInt(100), deltas: [] },
			});

			const DO = createDO();

			const pushRequest = () =>
				new Request("https://do.example.com/push", {
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

			await DO.fetch(pushRequest());

			// Check metrics — peakBufferBytes should be 512
			let metricsRes = await DO.fetch(
				new Request("https://do.example.com/admin/metrics", { method: "GET" }),
			);
			expect(metricsRes.status).toBe(200);
			let metrics = (await metricsRes.json()) as { peakBufferBytes: number };
			expect(metrics.peakBufferBytes).toBe(512);

			// Second push: buffer grows to 2048 bytes
			mockBufferStats.byteSize = 2048;
			mockHandlePush.mockReturnValue({
				ok: true,
				value: { accepted: 1, serverHlc: BigInt(200), deltas: [] },
			});
			await DO.fetch(pushRequest());

			metricsRes = await DO.fetch(
				new Request("https://do.example.com/admin/metrics", { method: "GET" }),
			);
			metrics = (await metricsRes.json()) as { peakBufferBytes: number };
			expect(metrics.peakBufferBytes).toBe(2048);

			// Third push: buffer shrinks to 256 bytes (e.g. after partial flush)
			// peakBufferBytes should still report 2048 (high-water mark)
			mockBufferStats.byteSize = 256;
			mockHandlePush.mockReturnValue({
				ok: true,
				value: { accepted: 1, serverHlc: BigInt(300), deltas: [] },
			});
			await DO.fetch(pushRequest());

			metricsRes = await DO.fetch(
				new Request("https://do.example.com/admin/metrics", { method: "GET" }),
			);
			metrics = (await metricsRes.json()) as { peakBufferBytes: number };
			expect(metrics.peakBufferBytes).toBe(2048);
		});
	});

	describe("bigint JSON serialisation", () => {
		it("serialises BigInt values in push response via bigintReplacer", async () => {
			resetGatewayMocks();
			const bigHlc = BigInt("281474976710656"); // a realistic HLC value
			mockHandlePush.mockReturnValue({
				ok: true,
				value: { accepted: 1, serverHlc: bigHlc, deltas: [] },
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
					value: { accepted: 0, serverHlc: BigInt(0), deltas: [] },
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

	describe("handleInternalBroadcast (POST /internal/broadcast)", () => {
		it("broadcasts deltas to connected WebSocket clients", async () => {
			resetGatewayMocks();

			const ctx = createMockCtx();
			const env = createMockEnv();

			const mockWs = {
				close: vi.fn(),
				send: vi.fn(),
				deserializeAttachment: vi.fn().mockReturnValue({
					claims: {},
					clientId: "other-client",
				}),
			};
			ctx.getWebSockets.mockReturnValue([mockWs]);

			const DO = new SyncGatewayDO(ctx as unknown as DurableObjectState, env as unknown as never);

			const { bigintReplacer } = await import("@lakesync/core");
			const body = JSON.stringify(
				{
					deltas: [
						{
							op: "INSERT",
							table: "todos",
							rowId: "row-1",
							clientId: "sender-client",
							columns: [{ column: "title", value: "Test" }],
							hlc: BigInt(100),
							deltaId: "d1",
						},
					],
					serverHlc: BigInt(200),
					excludeClientId: "sender-client",
				},
				bigintReplacer,
			);

			const request = new Request("https://do.example.com/internal/broadcast", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body,
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(200);

			const result = (await response.json()) as { broadcast: boolean };
			expect(result.broadcast).toBe(true);

			// The WebSocket client should have received the broadcast frame
			expect(mockWs.send).toHaveBeenCalled();
		});

		it("returns 405 for non-POST requests", async () => {
			resetGatewayMocks();
			const DO = createDO();

			const request = new Request("https://do.example.com/internal/broadcast", {
				method: "GET",
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(405);
		});

		it("skips broadcast for empty deltas", async () => {
			resetGatewayMocks();
			const DO = createDO();

			const { bigintReplacer } = await import("@lakesync/core");
			const body = JSON.stringify(
				{
					deltas: [],
					serverHlc: BigInt(100),
					excludeClientId: "client-1",
				},
				bigintReplacer,
			);

			const request = new Request("https://do.example.com/internal/broadcast", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body,
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(200);

			const result = (await response.json()) as { broadcast: boolean; clients: number };
			expect(result.broadcast).toBe(true);
			expect(result.clients).toBe(0);
		});

		it("excludes sender client from broadcast", async () => {
			resetGatewayMocks();

			const ctx = createMockCtx();
			const env = createMockEnv();

			const senderWs = {
				close: vi.fn(),
				send: vi.fn(),
				deserializeAttachment: vi.fn().mockReturnValue({
					claims: {},
					clientId: "sender-client",
				}),
			};
			const otherWs = {
				close: vi.fn(),
				send: vi.fn(),
				deserializeAttachment: vi.fn().mockReturnValue({
					claims: {},
					clientId: "other-client",
				}),
			};
			ctx.getWebSockets.mockReturnValue([senderWs, otherWs]);

			const DO = new SyncGatewayDO(ctx as unknown as DurableObjectState, env as unknown as never);

			const { bigintReplacer } = await import("@lakesync/core");
			const body = JSON.stringify(
				{
					deltas: [
						{
							op: "INSERT",
							table: "todos",
							rowId: "row-1",
							clientId: "sender-client",
							columns: [{ column: "title", value: "Test" }],
							hlc: BigInt(100),
							deltaId: "d1",
						},
					],
					serverHlc: BigInt(200),
					excludeClientId: "sender-client",
				},
				bigintReplacer,
			);

			const request = new Request("https://do.example.com/internal/broadcast", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body,
			});

			const response = await DO.fetch(request);
			expect(response.status).toBe(200);

			// Sender should NOT receive the broadcast
			expect(senderWs.send).not.toHaveBeenCalled();
			// Other client should receive it
			expect(otherWs.send).toHaveBeenCalled();
		});
	});
});

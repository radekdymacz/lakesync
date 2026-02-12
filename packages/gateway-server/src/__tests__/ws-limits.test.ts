import { HLC, type HLCTimestamp, type RowDelta } from "@lakesync/core";
import { encodeSyncPush, TAG_SYNC_PUSH } from "@lakesync/proto";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { GatewayServer, type GatewayServerConfig } from "../server";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeDelta(opts: Partial<RowDelta> & { hlc: HLCTimestamp }): RowDelta {
	return {
		op: opts.op ?? "INSERT",
		table: opts.table ?? "todos",
		rowId: opts.rowId ?? `row-${Math.random().toString(36).slice(2, 8)}`,
		clientId: opts.clientId ?? "client-a",
		columns: opts.columns ?? [{ column: "title", value: "Test" }],
		hlc: opts.hlc,
		deltaId: opts.deltaId ?? `delta-${Math.random().toString(36).slice(2, 8)}`,
	};
}

function waitForOpen(ws: WebSocket): Promise<void> {
	return new Promise((resolve, reject) => {
		if (ws.readyState === WebSocket.OPEN) {
			resolve();
			return;
		}
		ws.on("open", resolve);
		ws.on("error", reject);
	});
}

function waitForClose(ws: WebSocket, timeoutMs = 3000): Promise<{ code: number; reason: string }> {
	return new Promise((resolve, reject) => {
		const timer = setTimeout(() => reject(new Error("Timed out waiting for close")), timeoutMs);
		ws.on("close", (code: number, reason: Buffer) => {
			clearTimeout(timer);
			resolve({ code, reason: reason.toString() });
		});
	});
}

function waitForMessage(ws: WebSocket, timeoutMs = 2000): Promise<Buffer> {
	return new Promise((resolve, reject) => {
		const timer = setTimeout(() => reject(new Error("Timed out waiting for message")), timeoutMs);
		ws.once("message", (data: Buffer) => {
			clearTimeout(timer);
			resolve(data);
		});
	});
}

/** Try to open a WS connection and see if it connects or gets rejected. */
function tryConnect(
	url: string,
	timeoutMs = 2000,
): Promise<{ connected: boolean; ws?: WebSocket; error?: string }> {
	return new Promise((resolve) => {
		const timer = setTimeout(() => resolve({ connected: false, error: "timeout" }), timeoutMs);
		const ws = new WebSocket(url);
		ws.on("open", () => {
			clearTimeout(timer);
			resolve({ connected: true, ws });
		});
		ws.on("error", (err) => {
			clearTimeout(timer);
			resolve({ connected: false, error: err.message });
		});
		ws.on("unexpected-response", (_req, res) => {
			clearTimeout(timer);
			resolve({ connected: false, error: `HTTP ${res.statusCode}` });
		});
	});
}

// ---------------------------------------------------------------------------
// WebSocket connection limits
// ---------------------------------------------------------------------------

describe("WebSocket connection limits", () => {
	let server: GatewayServer;
	const config: GatewayServerConfig = {
		port: 0,
		gatewayId: "gw-ws-limits",
		persistence: "memory",
		wsLimits: {
			maxConnections: 2,
			maxMessagesPerSecond: 50,
		},
	};

	beforeEach(async () => {
		server = new GatewayServer(config);
		await server.start();
	});

	afterEach(async () => {
		await server.stop();
	});

	it("allows connections up to the limit", async () => {
		const ws1 = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-limits/ws`);
		const ws2 = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-limits/ws`);
		await Promise.all([waitForOpen(ws1), waitForOpen(ws2)]);

		expect(ws1.readyState).toBe(WebSocket.OPEN);
		expect(ws2.readyState).toBe(WebSocket.OPEN);

		ws1.close();
		ws2.close();
	});

	it("rejects connections exceeding the limit", async () => {
		const ws1 = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-limits/ws`);
		const ws2 = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-limits/ws`);
		await Promise.all([waitForOpen(ws1), waitForOpen(ws2)]);

		// Third connection should be rejected
		const result = await tryConnect(`ws://localhost:${server.port}/sync/gw-ws-limits/ws`);
		expect(result.connected).toBe(false);

		ws1.close();
		ws2.close();
	});

	it("allows new connections after one disconnects", async () => {
		const ws1 = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-limits/ws`);
		const ws2 = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-limits/ws`);
		await Promise.all([waitForOpen(ws1), waitForOpen(ws2)]);

		// Close one
		ws1.close();
		await new Promise((resolve) => setTimeout(resolve, 100));

		// Now a new connection should be allowed
		const ws3 = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-limits/ws`);
		await waitForOpen(ws3);
		expect(ws3.readyState).toBe(WebSocket.OPEN);

		ws2.close();
		ws3.close();
	});
});

// ---------------------------------------------------------------------------
// WebSocket message rate limits
// ---------------------------------------------------------------------------

describe("WebSocket message rate limits", () => {
	let server: GatewayServer;
	const config: GatewayServerConfig = {
		port: 0,
		gatewayId: "gw-ws-rate",
		persistence: "memory",
		wsLimits: {
			maxConnections: 100,
			maxMessagesPerSecond: 3,
		},
	};

	beforeEach(async () => {
		server = new GatewayServer(config);
		await server.start();
	});

	afterEach(async () => {
		await server.stop();
	});

	it("allows messages within the rate limit", async () => {
		const ws = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-rate/ws`);
		await waitForOpen(ws);

		const hlc = HLC.encode(1_000_000, 0);
		const push = encodeSyncPush({
			clientId: "client-rate",
			deltas: [makeDelta({ hlc, clientId: "client-rate" })],
			lastSeenHlc: hlc,
		});
		if (!push.ok) return;

		const frame = new Uint8Array(1 + push.value.length);
		frame[0] = TAG_SYNC_PUSH;
		frame.set(push.value, 1);

		// Send 3 messages (within limit)
		for (let i = 0; i < 3; i++) {
			const msgPromise = waitForMessage(ws);
			ws.send(frame);
			await msgPromise;
		}

		// Still connected
		expect(ws.readyState).toBe(WebSocket.OPEN);

		ws.close();
	});

	it("closes connection with 1008 when message rate exceeded", async () => {
		const ws = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-rate/ws`);
		await waitForOpen(ws);

		const hlc = HLC.encode(1_000_000, 0);
		const push = encodeSyncPush({
			clientId: "client-flood",
			deltas: [makeDelta({ hlc, clientId: "client-flood" })],
			lastSeenHlc: hlc,
		});
		if (!push.ok) return;

		const frame = new Uint8Array(1 + push.value.length);
		frame[0] = TAG_SYNC_PUSH;
		frame.set(push.value, 1);

		const closePromise = waitForClose(ws);

		// Send more messages than allowed in rapid succession
		// Rate limit is 3/sec — send 5 quickly
		for (let i = 0; i < 5; i++) {
			if (ws.readyState === WebSocket.OPEN) {
				ws.send(frame);
			}
		}

		const { code, reason } = await closePromise;
		expect(code).toBe(1008);
		expect(reason).toBe("Rate limit exceeded");
	});
});

import { bigintReplacer, HLC, type HLCTimestamp, type RowDelta } from "@lakesync/core";
import {
	decodeBroadcastFrame,
	decodeSyncResponse,
	encodeSyncPull,
	encodeSyncPush,
	TAG_SYNC_PULL,
	TAG_SYNC_PUSH,
} from "@lakesync/proto";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { GatewayServer, type GatewayServerConfig } from "../server";

/** Helper to build a RowDelta with sensible defaults. */
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

/** Wait for a WebSocket to open. */
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

/** Wait for the next binary message on a WebSocket. */
function waitForMessage(ws: WebSocket, timeoutMs = 2000): Promise<Buffer> {
	return new Promise((resolve, reject) => {
		const timer = setTimeout(() => reject(new Error("Timed out waiting for message")), timeoutMs);
		ws.once("message", (data: Buffer) => {
			clearTimeout(timer);
			resolve(data);
		});
	});
}

describe("GatewayServer WebSocket", () => {
	let server: GatewayServer;
	const config: GatewayServerConfig = {
		port: 0, // random port
		gatewayId: "gw-ws-test",
		persistence: "memory",
	};

	beforeEach(async () => {
		server = new GatewayServer(config);
		await server.start();
	});

	afterEach(async () => {
		await server.stop();
	});

	it("accepts WebSocket connections without JWT when no secret configured", async () => {
		const ws = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-test/ws`);
		await waitForOpen(ws);
		expect(ws.readyState).toBe(WebSocket.OPEN);
		ws.close();
	});

	it("push via WebSocket returns SyncResponse", async () => {
		const ws = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-test/ws`);
		await waitForOpen(ws);

		const hlc = HLC.encode(1_000_000, 0);
		const push = encodeSyncPush({
			clientId: "client-ws",
			deltas: [makeDelta({ hlc, clientId: "client-ws" })],
			lastSeenHlc: hlc,
		});
		expect(push.ok).toBe(true);
		if (!push.ok) return;

		const frame = new Uint8Array(1 + push.value.length);
		frame[0] = TAG_SYNC_PUSH;
		frame.set(push.value, 1);

		const msgPromise = waitForMessage(ws);
		ws.send(frame);

		const responseData = await msgPromise;
		const decoded = decodeSyncResponse(new Uint8Array(responseData));
		expect(decoded.ok).toBe(true);
		if (decoded.ok) {
			// Push response has empty deltas and a serverHlc
			expect(decoded.value.deltas).toHaveLength(0);
			expect(decoded.value.serverHlc).toBeGreaterThan(0n);
		}

		ws.close();
	});

	it("pull via WebSocket returns deltas", async () => {
		// First push a delta via HTTP
		const hlc = HLC.encode(1_000_000, 0);
		const delta = makeDelta({ hlc, clientId: "client-http", deltaId: "delta-pull-test" });

		const pushRes = await fetch(`http://localhost:${server.port}/sync/gw-ws-test/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(
				{
					clientId: "client-http",
					deltas: [delta],
					lastSeenHlc: "0",
				},
				bigintReplacer,
			),
		});
		expect(pushRes.ok).toBe(true);

		// Now pull via WebSocket
		const ws = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-test/ws`);
		await waitForOpen(ws);

		const pull = encodeSyncPull({
			clientId: "client-ws-pull",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});
		expect(pull.ok).toBe(true);
		if (!pull.ok) return;

		const frame = new Uint8Array(1 + pull.value.length);
		frame[0] = TAG_SYNC_PULL;
		frame.set(pull.value, 1);

		const msgPromise = waitForMessage(ws);
		ws.send(frame);

		const responseData = await msgPromise;
		const decoded = decodeSyncResponse(new Uint8Array(responseData));
		expect(decoded.ok).toBe(true);
		if (decoded.ok) {
			expect(decoded.value.deltas.length).toBeGreaterThanOrEqual(1);
		}

		ws.close();
	});

	it("HTTP push broadcasts to connected WebSocket clients", async () => {
		// Connect two WS clients
		const wsA = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-test/ws`);
		const wsB = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-test/ws`);
		await Promise.all([waitForOpen(wsA), waitForOpen(wsB)]);

		// Listen for broadcast on both
		const msgA = waitForMessage(wsA);
		const msgB = waitForMessage(wsB);

		// Push via HTTP (different client)
		const hlc = HLC.encode(2_000_000, 0);
		const delta = makeDelta({
			hlc,
			clientId: "client-http-push",
			deltaId: "delta-broadcast-test",
		});

		const pushRes = await fetch(`http://localhost:${server.port}/sync/gw-ws-test/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(
				{
					clientId: "client-http-push",
					deltas: [delta],
					lastSeenHlc: "0",
				},
				bigintReplacer,
			),
		});
		expect(pushRes.ok).toBe(true);

		// Both WS clients should receive the broadcast
		const [dataA, dataB] = await Promise.all([msgA, msgB]);

		const decodedA = decodeBroadcastFrame(new Uint8Array(dataA));
		expect(decodedA.ok).toBe(true);
		if (decodedA.ok) {
			expect(decodedA.value.deltas).toHaveLength(1);
			expect(decodedA.value.deltas[0]?.deltaId).toBe("delta-broadcast-test");
		}

		const decodedB = decodeBroadcastFrame(new Uint8Array(dataB));
		expect(decodedB.ok).toBe(true);
		if (decodedB.ok) {
			expect(decodedB.value.deltas).toHaveLength(1);
		}

		wsA.close();
		wsB.close();
	});

	it("WS push broadcasts to other WS clients but not sender", async () => {
		// Connect two WS clients
		const wsSender = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-test/ws`);
		const wsReceiver = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-test/ws`);
		await Promise.all([waitForOpen(wsSender), waitForOpen(wsReceiver)]);

		// Listen for messages
		const receiverMsg = waitForMessage(wsReceiver);
		const senderMsg = waitForMessage(wsSender, 500); // Short timeout — sender should NOT get broadcast

		// Push from sender via WS
		const hlc = HLC.encode(3_000_000, 0);
		const push = encodeSyncPush({
			clientId: "client-ws-sender",
			deltas: [makeDelta({ hlc, clientId: "client-ws-sender", deltaId: "delta-ws-push" })],
			lastSeenHlc: hlc,
		});
		expect(push.ok).toBe(true);
		if (!push.ok) return;

		const frame = new Uint8Array(1 + push.value.length);
		frame[0] = TAG_SYNC_PUSH;
		frame.set(push.value, 1);

		wsSender.send(frame);

		// Sender gets a push response (not a broadcast)
		const senderData = await senderMsg;
		// The sender should get a SyncResponse (no tag byte), not a broadcast frame
		const senderDecoded = decodeSyncResponse(new Uint8Array(senderData));
		expect(senderDecoded.ok).toBe(true);

		// Receiver should get a broadcast
		const receiverData = await receiverMsg;
		const receiverDecoded = decodeBroadcastFrame(new Uint8Array(receiverData));
		expect(receiverDecoded.ok).toBe(true);
		if (receiverDecoded.ok) {
			expect(receiverDecoded.value.deltas).toHaveLength(1);
			expect(receiverDecoded.value.deltas[0]?.deltaId).toBe("delta-ws-push");
		}

		wsSender.close();
		wsReceiver.close();
	});

	it("disconnected clients are removed from broadcast list", async () => {
		const ws = new WebSocket(`ws://localhost:${server.port}/sync/gw-ws-test/ws`);
		await waitForOpen(ws);

		// Close the connection
		ws.close();
		await new Promise((resolve) => setTimeout(resolve, 100));

		// Push via HTTP — should not throw even with closed clients
		const hlc = HLC.encode(4_000_000, 0);
		const pushRes = await fetch(`http://localhost:${server.port}/sync/gw-ws-test/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(
				{
					clientId: "client-after-close",
					deltas: [makeDelta({ hlc, clientId: "client-after-close" })],
					lastSeenHlc: "0",
				},
				bigintReplacer,
			),
		});
		expect(pushRes.ok).toBe(true);
	});
});

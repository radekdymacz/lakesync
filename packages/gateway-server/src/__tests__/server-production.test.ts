import { request as httpRequest, type IncomingMessage } from "node:http";
import {
	bigintReplacer,
	type DatabaseAdapter,
	type HLCTimestamp,
	type RowDelta,
} from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { GatewayServer } from "../server";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a minimal RowDelta for testing. */
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

/** Make an HTTP request and return { status, headers, body }. */
function req(
	url: string,
	options: {
		method?: string;
		headers?: Record<string, string>;
		body?: string;
		timeout?: number;
	} = {},
): Promise<{ status: number; headers: Record<string, string>; body: string }> {
	return new Promise((resolve, reject) => {
		const parsed = new URL(url);
		const r = httpRequest(
			{
				hostname: parsed.hostname,
				port: parsed.port,
				path: parsed.pathname + parsed.search,
				method: options.method ?? "GET",
				headers: options.headers,
				timeout: options.timeout,
			},
			(res: IncomingMessage) => {
				const chunks: Buffer[] = [];
				res.on("data", (chunk: Buffer) => chunks.push(chunk));
				res.on("end", () => {
					const body = Buffer.concat(chunks).toString("utf-8");
					const headers: Record<string, string> = {};
					for (const [key, value] of Object.entries(res.headers)) {
						if (typeof value === "string") {
							headers[key] = value;
						}
					}
					resolve({ status: res.statusCode ?? 0, headers, body });
				});
			},
		);
		r.on("error", reject);
		if (options.body) {
			r.write(options.body);
		}
		r.end();
	});
}

/** Build a JSON push body. */
function pushBody(deltas: RowDelta[], clientId = "client-1"): string {
	return JSON.stringify({ clientId, deltas, lastSeenHlc: "0" }, bigintReplacer);
}

// ---------------------------------------------------------------------------
// B1: Graceful Shutdown
// ---------------------------------------------------------------------------

describe("GatewayServer graceful shutdown", () => {
	const gatewayId = "shutdown-gw";

	it("returns 503 during drain for push/pull requests", async () => {
		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		// Verify server is healthy before drain
		const healthRes = await req(`${baseUrl}/health`);
		expect(healthRes.status).toBe(200);

		// Manually trigger draining state
		(server as unknown as { draining: boolean }).draining = true;

		// Push should get 503
		const pushRes = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([makeDelta()]),
		});
		expect(pushRes.status).toBe(503);
		const pushBody2 = JSON.parse(pushRes.body);
		expect(pushBody2.error).toBe("Service is shutting down");

		// Pull should get 503
		const pullRes = await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=c1`);
		expect(pullRes.status).toBe(503);

		// Health should still work during drain
		const healthRes2 = await req(`${baseUrl}/health`);
		expect(healthRes2.status).toBe(200);

		// Clean up
		(server as unknown as { draining: boolean }).draining = false;
		await server.stop();
	});

	it("setupSignalHandlers registers and cleans up SIGTERM/SIGINT", async () => {
		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();

		// Signal handlers should be registered (signalCleanup should be set)
		const cleanup = (server as unknown as { signalCleanup: (() => void) | null }).signalCleanup;
		expect(cleanup).not.toBeNull();

		// Stop should clean up signal handlers
		await server.stop();
		const cleanupAfter = (server as unknown as { signalCleanup: (() => void) | null })
			.signalCleanup;
		expect(cleanupAfter).toBeNull();
	});

	it("isDraining getter reflects draining state", async () => {
		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();

		expect(server.isDraining).toBe(false);
		(server as unknown as { draining: boolean }).draining = true;
		expect(server.isDraining).toBe(true);

		(server as unknown as { draining: boolean }).draining = false;
		await server.stop();
	});

	it("performs final flush during graceful shutdown", async () => {
		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
			drainTimeoutMs: 500,
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		// Push a delta
		const pushRes = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([makeDelta()]),
		});
		expect(pushRes.status).toBe(200);

		// Verify buffer has data
		expect(server.gatewayInstance.bufferStats.logSize).toBe(1);

		// Spy on the gateway flush
		const flushSpy = vi.spyOn(server.gatewayInstance, "flush");

		// Trigger graceful shutdown without process.exit
		const exitSpy = vi.spyOn(process, "exit").mockImplementation((() => {}) as never);
		const shutdownFn = (
			server as unknown as { gracefulShutdown: () => Promise<void> }
		).gracefulShutdown.bind(server);
		await shutdownFn();

		expect(flushSpy).toHaveBeenCalled();
		exitSpy.mockRestore();
	});
});

// ---------------------------------------------------------------------------
// B2: Health & Readiness Probes
// ---------------------------------------------------------------------------

describe("GatewayServer readiness probe", () => {
	const gatewayId = "ready-gw";

	it("GET /ready returns 200 when not draining and no adapter", async () => {
		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		const res = await req(`${baseUrl}/ready`);
		expect(res.status).toBe(200);
		const body = JSON.parse(res.body);
		expect(body).toEqual({ status: "ready" });

		await server.stop();
	});

	it("GET /ready returns 503 during drain", async () => {
		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		// Trigger drain
		(server as unknown as { draining: boolean }).draining = true;

		const res = await req(`${baseUrl}/ready`);
		expect(res.status).toBe(503);
		const body = JSON.parse(res.body);
		expect(body.status).toBe("not_ready");
		expect(body.reason).toBe("draining");

		(server as unknown as { draining: boolean }).draining = false;
		await server.stop();
	});

	it("GET /ready returns 503 when DatabaseAdapter is unreachable", async () => {
		// Create a mock DatabaseAdapter that fails health checks
		const failingAdapter: DatabaseAdapter = {
			insertDeltas: vi.fn().mockResolvedValue({ ok: true }),
			queryDeltasSince: vi.fn().mockRejectedValue(new Error("Connection refused")),
			getLatestState: vi.fn().mockResolvedValue({ ok: true, value: null }),
			ensureSchema: vi.fn().mockResolvedValue({ ok: true }),
			close: vi.fn().mockResolvedValue(undefined),
		};

		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
			adapter: failingAdapter,
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		const res = await req(`${baseUrl}/ready`);
		expect(res.status).toBe(503);
		const body = JSON.parse(res.body);
		expect(body.status).toBe("not_ready");
		expect(body.reason).toBe("adapter unreachable");

		await server.stop();
	});

	it("GET /ready returns 200 when DatabaseAdapter is healthy", async () => {
		const healthyAdapter: DatabaseAdapter = {
			insertDeltas: vi.fn().mockResolvedValue({ ok: true }),
			queryDeltasSince: vi.fn().mockResolvedValue({ ok: true, value: [] }),
			getLatestState: vi.fn().mockResolvedValue({ ok: true, value: null }),
			ensureSchema: vi.fn().mockResolvedValue({ ok: true }),
			close: vi.fn().mockResolvedValue(undefined),
		};

		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
			adapter: healthyAdapter,
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		const res = await req(`${baseUrl}/ready`);
		expect(res.status).toBe(200);
		const body = JSON.parse(res.body);
		expect(body.status).toBe("ready");

		await server.stop();
	});

	it("GET /ready is available even during drain", async () => {
		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		(server as unknown as { draining: boolean }).draining = true;

		// /ready should still respond (with 503 for draining)
		const res = await req(`${baseUrl}/ready`);
		expect(res.status).toBe(503);
		expect(JSON.parse(res.body).reason).toBe("draining");

		(server as unknown as { draining: boolean }).draining = false;
		await server.stop();
	});
});

// ---------------------------------------------------------------------------
// B3: Request Timeouts
// ---------------------------------------------------------------------------

describe("GatewayServer request timeouts", () => {
	const gatewayId = "timeout-gw";

	it("sets request timeout via config", async () => {
		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
			requestTimeoutMs: 5_000,
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		// Verify normal requests still work with the timeout set
		const res = await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=c1`);
		expect(res.status).toBe(200);

		await server.stop();
	});

	it("flush timeout config is applied to periodic flush", async () => {
		// Create a server with a very short flush timeout
		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
			flushTimeoutMs: 50, // Very short timeout
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		// Push a delta
		const pushRes = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([makeDelta()]),
		});
		expect(pushRes.status).toBe(200);

		// Server should still be healthy after periodic flush (even if timeout fires)
		const healthRes = await req(`${baseUrl}/health`);
		expect(healthRes.status).toBe(200);

		await server.stop();
	});

	it("periodic flush with timeout wraps flush in Promise.race", async () => {
		// Create a mock adapter that takes forever to flush
		const slowAdapter: DatabaseAdapter = {
			insertDeltas: vi.fn().mockImplementation(
				() => new Promise(() => {}), // Never resolves
			),
			queryDeltasSince: vi.fn().mockResolvedValue({ ok: true, value: [] }),
			getLatestState: vi.fn().mockResolvedValue({ ok: true, value: null }),
			ensureSchema: vi.fn().mockResolvedValue({ ok: true }),
			close: vi.fn().mockResolvedValue(undefined),
		};

		const server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
			flushTimeoutMs: 100,
			adapter: slowAdapter,
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		// Push a delta to put something in the buffer
		await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([makeDelta()]),
		});

		// Manually trigger periodic flush — should not hang
		const periodicFlush = (
			server as unknown as { periodicFlush: () => Promise<void> }
		).periodicFlush.bind(server);

		// The flush should complete within the timeout period
		const flushPromise = periodicFlush();
		const timeoutPromise = new Promise<"timeout">((resolve) =>
			setTimeout(() => resolve("timeout"), 2_000),
		);
		const result = await Promise.race([flushPromise.then(() => "done" as const), timeoutPromise]);
		expect(result).toBe("done");

		await server.stop();
	});
});

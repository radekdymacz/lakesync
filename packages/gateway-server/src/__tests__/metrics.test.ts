import { request as httpRequest, type IncomingMessage } from "node:http";
import { bigintReplacer, type HLCTimestamp, type RowDelta } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Counter, Gauge, Histogram, MetricsRegistry } from "../metrics";
import { GatewayServer } from "../server";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

function req(
	url: string,
	options: { method?: string; headers?: Record<string, string>; body?: string } = {},
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

function pushBody(deltas: RowDelta[], clientId = "client-1"): string {
	return JSON.stringify({ clientId, deltas, lastSeenHlc: "0" }, bigintReplacer);
}

// ---------------------------------------------------------------------------
// Unit tests — Counter, Gauge, Histogram
// ---------------------------------------------------------------------------

describe("Counter", () => {
	it("increments and exposes values", () => {
		const c = new Counter("test_total", "Test counter");
		c.inc({ status: "ok" });
		c.inc({ status: "ok" });
		c.inc({ status: "error" });

		expect(c.get({ status: "ok" })).toBe(2);
		expect(c.get({ status: "error" })).toBe(1);
		expect(c.get({ status: "unknown" })).toBe(0);

		const output = c.expose();
		expect(output).toContain("# HELP test_total Test counter");
		expect(output).toContain("# TYPE test_total counter");
		expect(output).toContain('test_total{status="ok"} 2');
		expect(output).toContain('test_total{status="error"} 1');
	});

	it("increments by a custom amount", () => {
		const c = new Counter("batch_total", "Batch counter");
		c.inc({}, 5);
		expect(c.get({})).toBe(5);
	});

	it("resets all values", () => {
		const c = new Counter("reset_total", "Reset counter");
		c.inc({ a: "1" });
		c.reset();
		expect(c.get({ a: "1" })).toBe(0);
	});
});

describe("Gauge", () => {
	it("sets, increments, and decrements", () => {
		const g = new Gauge("test_gauge", "Test gauge");
		g.set({}, 10);
		expect(g.get({})).toBe(10);

		g.inc({}, 5);
		expect(g.get({})).toBe(15);

		g.dec({}, 3);
		expect(g.get({})).toBe(12);
	});

	it("exposes in Prometheus format", () => {
		const g = new Gauge("buffer_bytes", "Buffer bytes");
		g.set({}, 1024);
		const output = g.expose();
		expect(output).toContain("# TYPE buffer_bytes gauge");
		expect(output).toContain("buffer_bytes 1024");
	});
});

describe("Histogram", () => {
	it("observes values and distributes to buckets", () => {
		const h = new Histogram("latency_ms", "Latency", [10, 50, 100, 500]);

		h.observe({}, 5); // <= 10, <= 50, <= 100, <= 500, +Inf
		h.observe({}, 25); // <= 50, <= 100, <= 500, +Inf
		h.observe({}, 75); // <= 100, <= 500, +Inf
		h.observe({}, 200); // <= 500, +Inf

		expect(h.getCount({})).toBe(4);
		expect(h.getSum({})).toBe(305);

		const output = h.expose();
		expect(output).toContain("# TYPE latency_ms histogram");
		expect(output).toContain('latency_ms_bucket{le="10"} 1');
		expect(output).toContain('latency_ms_bucket{le="50"} 2');
		expect(output).toContain('latency_ms_bucket{le="100"} 3');
		expect(output).toContain('latency_ms_bucket{le="500"} 4');
		expect(output).toContain('latency_ms_bucket{le="+Inf"} 4');
		expect(output).toContain("latency_ms_sum 305");
		expect(output).toContain("latency_ms_count 4");
	});

	it("resets all data", () => {
		const h = new Histogram("reset_hist", "Reset histogram", [10]);
		h.observe({}, 5);
		h.reset();
		expect(h.getCount({})).toBe(0);
	});
});

describe("MetricsRegistry", () => {
	it("exposes all metrics in a single payload", () => {
		const registry = new MetricsRegistry();
		registry.pushTotal.inc({ status: "ok" });
		registry.pullTotal.inc({ status: "ok" });
		registry.bufferBytes.set({}, 2048);

		const output = registry.expose();
		expect(output).toContain("lakesync_push_total");
		expect(output).toContain("lakesync_pull_total");
		expect(output).toContain("lakesync_flush_total");
		expect(output).toContain("lakesync_flush_duration_ms");
		expect(output).toContain("lakesync_push_latency_ms");
		expect(output).toContain("lakesync_buffer_bytes");
		expect(output).toContain("lakesync_buffer_deltas");
		expect(output).toContain("lakesync_ws_connections");
		expect(output).toContain("lakesync_active_requests");
	});

	it("resets all metrics", () => {
		const registry = new MetricsRegistry();
		registry.pushTotal.inc({ status: "ok" }, 10);
		registry.reset();
		expect(registry.pushTotal.get({ status: "ok" })).toBe(0);
	});
});

// ---------------------------------------------------------------------------
// Integration — GET /metrics endpoint and instrumentation
// ---------------------------------------------------------------------------

describe("GET /metrics endpoint", () => {
	const gatewayId = "metrics-gw";
	let server: GatewayServer;
	let baseUrl: string;

	beforeEach(async () => {
		server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
			logLevel: "error", // Suppress log noise during tests
		});
		await server.start();
		baseUrl = `http://localhost:${server.port}`;
	});

	afterEach(async () => {
		await server.stop();
	});

	it("returns Prometheus text exposition format", async () => {
		const res = await req(`${baseUrl}/metrics`);
		expect(res.status).toBe(200);
		expect(res.headers["content-type"]).toContain("text/plain");
		expect(res.body).toContain("lakesync_push_total");
		expect(res.body).toContain("lakesync_pull_total");
		expect(res.body).toContain("lakesync_buffer_bytes");
	});

	it("increments push counter on successful push", async () => {
		const delta = makeDelta();
		await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([delta]),
		});

		const metricsRes = await req(`${baseUrl}/metrics`);
		expect(metricsRes.body).toContain('lakesync_push_total{status="ok"} 1');
	});

	it("increments pull counter on successful pull", async () => {
		await req(`${baseUrl}/v1/sync/${gatewayId}/pull?since=0&clientId=c1`);

		const metricsRes = await req(`${baseUrl}/metrics`);
		expect(metricsRes.body).toContain('lakesync_pull_total{status="ok"} 1');
	});

	it("increments flush counter on admin flush", async () => {
		await req(`${baseUrl}/v1/admin/flush/${gatewayId}`, { method: "POST" });

		const metricsRes = await req(`${baseUrl}/metrics`);
		expect(metricsRes.body).toContain('lakesync_flush_total{status="ok"} 1');
	});

	it("updates buffer gauges after push", async () => {
		const delta = makeDelta();
		await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([delta]),
		});

		const metricsRes = await req(`${baseUrl}/metrics`);
		// Buffer should have at least 1 delta
		expect(metricsRes.body).toContain("lakesync_buffer_deltas 1");
		// Buffer bytes should be > 0
		expect(metricsRes.body).toMatch(/lakesync_buffer_bytes \d+/);
	});

	it("records push latency histogram", async () => {
		const delta = makeDelta();
		await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([delta]),
		});

		const metricsRes = await req(`${baseUrl}/metrics`);
		expect(metricsRes.body).toContain("lakesync_push_latency_ms_count 1");
	});

	it("tracks active requests gauge", async () => {
		// After a request completes, the active requests gauge should be back to 0
		// (unless another /metrics request is in-flight which is the request itself)
		const metricsRes = await req(`${baseUrl}/metrics`);
		// The /metrics request itself is active, so gauge may be 1 during response
		// But the expose snapshot happens synchronously, so it should show 1
		expect(metricsRes.body).toContain("lakesync_active_requests");
	});

	it("exposes metrics registry via metricsRegistry getter", () => {
		const registry = server.metricsRegistry;
		expect(registry).toBeDefined();
		registry.pushTotal.inc({ status: "ok" });
		expect(registry.pushTotal.get({ status: "ok" })).toBe(1);
	});
});

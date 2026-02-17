import { request as httpRequest, type IncomingMessage } from "node:http";
import { bigintReplacer, bigintReviver, type HLCTimestamp, type RowDelta } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MemoryPersistence, SqlitePersistence } from "../persistence";
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
// Tests
// ---------------------------------------------------------------------------

describe("GatewayServer", () => {
	const gatewayId = "test-gw";
	let server: GatewayServer;
	let baseUrl: string;

	beforeEach(async () => {
		server = new GatewayServer({
			gatewayId,
			port: 0, // Let OS pick a free port
			flushIntervalMs: 60_000,
		});
		await server.start();
		baseUrl = `http://localhost:${server.port}`;
	});

	afterEach(async () => {
		await server.stop();
	});

	// -----------------------------------------------------------------------
	// Health check
	// -----------------------------------------------------------------------

	it("responds to health check", async () => {
		const res = await req(`${baseUrl}/health`);
		expect(res.status).toBe(200);
		const body = JSON.parse(res.body);
		expect(body).toEqual({ status: "ok" });
	});

	// -----------------------------------------------------------------------
	// Push / Pull roundtrip
	// -----------------------------------------------------------------------

	it("push/pull roundtrip returns pushed deltas", async () => {
		const delta = makeDelta();

		// Push
		const pushRes = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([delta]),
		});
		expect(pushRes.status).toBe(200);
		const pushData = JSON.parse(pushRes.body) as { accepted: number; serverHlc: string };
		expect(pushData.accepted).toBe(1);
		expect(pushData.serverHlc).toBeDefined();

		// Pull
		const pullRes = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?since=0&clientId=client-1`);
		expect(pullRes.status).toBe(200);
		const pullData = JSON.parse(pullRes.body, bigintReviver) as {
			deltas: RowDelta[];
			serverHlc: HLCTimestamp;
			hasMore: boolean;
		};
		expect(pullData.deltas.length).toBe(1);
		expect(pullData.deltas[0]!.deltaId).toBe(delta.deltaId);
		expect(pullData.deltas[0]!.table).toBe("tasks");
	});

	// -----------------------------------------------------------------------
	// Route validation
	// -----------------------------------------------------------------------

	it("returns 404 for unknown routes", async () => {
		const res = await req(`${baseUrl}/unknown`);
		expect(res.status).toBe(404);
	});

	it("returns 404 for wrong gateway ID", async () => {
		const res = await req(`${baseUrl}/v1/sync/wrong-gw/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(404);
	});

	// -----------------------------------------------------------------------
	// Push validation
	// -----------------------------------------------------------------------

	it("rejects push with invalid JSON", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: "not json",
		});
		expect(res.status).toBe(400);
	});

	it("rejects push with missing fields", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ clientId: "c1" }),
		});
		expect(res.status).toBe(400);
	});

	// -----------------------------------------------------------------------
	// Pull validation
	// -----------------------------------------------------------------------

	it("rejects pull with missing since param", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?clientId=c1`);
		expect(res.status).toBe(400);
	});

	// -----------------------------------------------------------------------
	// Admin: schema
	// -----------------------------------------------------------------------

	it("saves and validates schema", async () => {
		const schema = {
			table: "tasks",
			columns: [
				{ name: "title", type: "string" },
				{ name: "done", type: "boolean" },
			],
		};

		const res = await req(`${baseUrl}/v1/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(schema),
		});
		expect(res.status).toBe(200);
		const body = JSON.parse(res.body);
		expect(body).toEqual({ saved: true });
	});

	it("rejects invalid schema", async () => {
		const res = await req(`${baseUrl}/v1/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ table: "t" }),
		});
		expect(res.status).toBe(400);
	});

	// -----------------------------------------------------------------------
	// Admin: sync rules
	// -----------------------------------------------------------------------

	it("saves valid sync rules", async () => {
		const rules = {
			version: 1,
			buckets: [
				{
					name: "user-data",
					tables: ["tasks"],
					filters: [{ column: "userId", op: "eq", value: "jwt:sub" }],
				},
			],
		};

		const res = await req(`${baseUrl}/v1/admin/sync-rules/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(rules),
		});
		expect(res.status).toBe(200);
		const body = JSON.parse(res.body);
		expect(body).toEqual({ saved: true });
	});

	// -----------------------------------------------------------------------
	// Admin: flush
	// -----------------------------------------------------------------------

	it("flush returns success when buffer is empty", async () => {
		const res = await req(`${baseUrl}/v1/admin/flush/${gatewayId}`, {
			method: "POST",
		});
		expect(res.status).toBe(200);
		const body = JSON.parse(res.body);
		expect(body).toEqual({ flushed: true });
	});

	// -----------------------------------------------------------------------
	// Admin: metrics
	// -----------------------------------------------------------------------

	it("GET /admin/metrics returns buffer stats", async () => {
		const res = await req(`${baseUrl}/v1/admin/metrics/${gatewayId}`);
		expect(res.status).toBe(200);
		const body = JSON.parse(res.body);
		expect(typeof body.logSize).toBe("number");
		expect(typeof body.indexSize).toBe("number");
		expect(typeof body.byteSize).toBe("number");
		expect(body.process).toBeDefined();
		expect(typeof body.process.heapUsed).toBe("number");
	});

	// -----------------------------------------------------------------------
	// CORS
	// -----------------------------------------------------------------------

	it("responds to CORS preflight", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "OPTIONS",
			headers: { Origin: "http://localhost:5173" },
		});
		expect(res.status).toBe(204);
		expect(res.headers["access-control-allow-origin"]).toBe("http://localhost:5173");
	});

	it("sets CORS headers on responses", async () => {
		const res = await req(`${baseUrl}/health`, {
			headers: { Origin: "http://localhost:5173" },
		});
		expect(res.headers["access-control-allow-origin"]).toBe("http://localhost:5173");
	});

	// -----------------------------------------------------------------------
	// Security headers (A4)
	// -----------------------------------------------------------------------

	it("health response includes security headers but not Cache-Control: no-store", async () => {
		const res = await req(`${baseUrl}/health`);
		expect(res.status).toBe(200);
		expect(res.headers["x-content-type-options"]).toBe("nosniff");
		expect(res.headers["x-frame-options"]).toBe("DENY");
		expect(res.headers["strict-transport-security"]).toBe(
			"max-age=31536000; includeSubDomains",
		);
		// Health endpoint should NOT have Cache-Control: no-store
		expect(res.headers["cache-control"]).toBeUndefined();
	});

	it("sync route response includes Cache-Control: no-store", async () => {
		const delta = makeDelta();
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([delta]),
		});
		expect(res.status).toBe(200);
		expect(res.headers["x-content-type-options"]).toBe("nosniff");
		expect(res.headers["x-frame-options"]).toBe("DENY");
		expect(res.headers["strict-transport-security"]).toBe(
			"max-age=31536000; includeSubDomains",
		);
		expect(res.headers["cache-control"]).toBe("no-store");
	});

	it("admin route response includes Cache-Control: no-store", async () => {
		const res = await req(`${baseUrl}/v1/admin/flush/${gatewayId}`, {
			method: "POST",
		});
		expect(res.status).toBe(200);
		expect(res.headers["cache-control"]).toBe("no-store");
	});
});

// ---------------------------------------------------------------------------
// Periodic flush
// ---------------------------------------------------------------------------

describe("GatewayServer periodic flush", () => {
	it("fires periodic flush after interval", async () => {
		const server = new GatewayServer({
			gatewayId: "flush-gw",
			port: 0,
			flushIntervalMs: 50, // Very short for testing
		});
		await server.start();
		const baseUrl = `http://localhost:${server.port}`;

		// Push a delta
		const delta = makeDelta();
		await req(`${baseUrl}/v1/sync/flush-gw/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([delta]),
		});

		// Wait for periodic flush to fire
		await new Promise((resolve) => setTimeout(resolve, 150));

		// Verify server is still healthy after periodic flush
		const healthRes = await req(`${baseUrl}/health`);
		expect(healthRes.status).toBe(200);

		await server.stop();
	});
});

// ---------------------------------------------------------------------------
// MemoryPersistence
// ---------------------------------------------------------------------------

describe("MemoryPersistence", () => {
	it("stores and retrieves deltas", () => {
		const p = new MemoryPersistence();
		const delta = makeDelta();

		p.appendBatch([delta]);
		expect(p.loadAll()).toHaveLength(1);
		expect(p.loadAll()[0]!.deltaId).toBe(delta.deltaId);

		p.clear();
		expect(p.loadAll()).toHaveLength(0);

		p.close();
	});
});

// ---------------------------------------------------------------------------
// SqlitePersistence
// ---------------------------------------------------------------------------

describe("SqlitePersistence", () => {
	it("persists and reloads deltas across clear cycles", () => {
		// Use a temp file path — better-sqlite3 does not support :memory: in the same way
		const dbPath = `/tmp/lakesync-test-${Date.now()}.sqlite`;
		const p = new SqlitePersistence(dbPath);

		const d1 = makeDelta({ clientId: "c1" });
		const d2 = makeDelta({ clientId: "c2" });

		p.appendBatch([d1]);
		p.appendBatch([d2]);

		const loaded = p.loadAll();
		expect(loaded).toHaveLength(2);
		expect(loaded[0]!.clientId).toBe("c1");
		expect(loaded[1]!.clientId).toBe("c2");

		// Verify bigint HLC survived serialisation round-trip
		expect(typeof loaded[0]!.hlc).toBe("bigint");

		p.clear();
		expect(p.loadAll()).toHaveLength(0);

		p.close();

		// Clean up
		try {
			require("node:fs").unlinkSync(dbPath);
		} catch {
			// Ignore cleanup errors
		}
	});
});

// ---------------------------------------------------------------------------
// JWT authentication
// ---------------------------------------------------------------------------

describe("GatewayServer with JWT auth", () => {
	const gatewayId = "auth-gw";
	const jwtSecret = "test-secret-key-for-hmac-256-tests";
	let server: GatewayServer;
	let baseUrl: string;

	beforeEach(async () => {
		server = new GatewayServer({
			gatewayId,
			port: 0,
			jwtSecret,
			flushIntervalMs: 60_000,
		});
		await server.start();
		baseUrl = `http://localhost:${server.port}`;
	});

	afterEach(async () => {
		await server.stop();
	});

	/** Sign a JWT with HMAC-SHA256 using Web Crypto. */
	async function signJwt(payload: Record<string, unknown>, secret: string): Promise<string> {
		const encoder = new TextEncoder();
		const header = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" }))
			.replace(/\+/g, "-")
			.replace(/\//g, "_")
			.replace(/=+$/, "");
		const payloadB64 = btoa(JSON.stringify(payload))
			.replace(/\+/g, "-")
			.replace(/\//g, "_")
			.replace(/=+$/, "");

		const key = await crypto.subtle.importKey(
			"raw",
			encoder.encode(secret),
			{ name: "HMAC", hash: "SHA-256" },
			false,
			["sign"],
		);

		const signature = await crypto.subtle.sign(
			"HMAC",
			key,
			encoder.encode(`${header}.${payloadB64}`),
		);

		const sigB64 = btoa(String.fromCharCode(...new Uint8Array(signature)))
			.replace(/\+/g, "-")
			.replace(/\//g, "_")
			.replace(/=+$/, "");

		return `${header}.${payloadB64}.${sigB64}`;
	}

	it("rejects requests without a token", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(401);
	});

	it("accepts requests with a valid token", async () => {
		const token = await signJwt(
			{
				sub: "client-1",
				gw: gatewayId,
				role: "client",
				exp: Math.floor(Date.now() / 1000) + 3600,
			},
			jwtSecret,
		);

		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(200);
	});

	it("rejects admin routes for non-admin role", async () => {
		const token = await signJwt(
			{
				sub: "client-1",
				gw: gatewayId,
				role: "client",
				exp: Math.floor(Date.now() / 1000) + 3600,
			},
			jwtSecret,
		);

		const res = await req(`${baseUrl}/v1/admin/flush/${gatewayId}`, {
			method: "POST",
			headers: { Authorization: `Bearer ${token}` },
		});
		expect(res.status).toBe(403);
	});

	it("allows admin routes for admin role", async () => {
		const token = await signJwt(
			{
				sub: "admin-1",
				gw: gatewayId,
				role: "admin",
				exp: Math.floor(Date.now() / 1000) + 3600,
			},
			jwtSecret,
		);

		const res = await req(`${baseUrl}/v1/admin/flush/${gatewayId}`, {
			method: "POST",
			headers: { Authorization: `Bearer ${token}` },
		});
		expect(res.status).toBe(200);
	});

	it("rejects token with wrong gateway ID", async () => {
		const token = await signJwt(
			{
				sub: "client-1",
				gw: "other-gw",
				role: "client",
				exp: Math.floor(Date.now() / 1000) + 3600,
			},
			jwtSecret,
		);

		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(403);
	});
});

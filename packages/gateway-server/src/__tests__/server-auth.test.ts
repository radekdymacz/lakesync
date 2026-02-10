import { request as httpRequest, type IncomingMessage } from "node:http";
import { bigintReplacer, type HLCTimestamp, type RowDelta } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
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

/** Sign a JWT with HMAC-SHA256 using Web Crypto. */
async function createTestJWT(payload: Record<string, unknown>, secret: string): Promise<string> {
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

/** Create a valid client JWT for the test gateway. */
function validClientToken(
	secret: string,
	gatewayId: string,
	clientId = "client-1",
): Promise<string> {
	return createTestJWT(
		{
			sub: clientId,
			gw: gatewayId,
			role: "client",
			exp: Math.floor(Date.now() / 1000) + 3600,
		},
		secret,
	);
}

/** Create a valid admin JWT for the test gateway. */
function validAdminToken(secret: string, gatewayId: string): Promise<string> {
	return createTestJWT(
		{
			sub: "admin-1",
			gw: gatewayId,
			role: "admin",
			exp: Math.floor(Date.now() / 1000) + 3600,
		},
		secret,
	);
}

// ---------------------------------------------------------------------------
// Tests — Auth-protected server
// ---------------------------------------------------------------------------

describe("GatewayServer JWT auth enforcement", () => {
	const gatewayId = "auth-gw";
	const jwtSecret = "test-secret-key-for-hmac-256-auth-tests";
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

	// -----------------------------------------------------------------------
	// Routes require token when jwtSecret is configured
	// -----------------------------------------------------------------------

	it("push without token returns 401", async () => {
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(401);
		expect(JSON.parse(res.body).error).toContain("Bearer token");
	});

	it("pull without token returns 401", async () => {
		const res = await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=client-1`);
		expect(res.status).toBe(401);
	});

	it("admin flush without token returns 401", async () => {
		const res = await req(`${baseUrl}/admin/flush/${gatewayId}`, {
			method: "POST",
		});
		expect(res.status).toBe(401);
	});

	it("admin schema without token returns 401", async () => {
		const res = await req(`${baseUrl}/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ table: "t", columns: [{ name: "a", type: "string" }] }),
		});
		expect(res.status).toBe(401);
	});

	it("admin sync-rules without token returns 401", async () => {
		const res = await req(`${baseUrl}/admin/sync-rules/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ version: 1, buckets: [] }),
		});
		expect(res.status).toBe(401);
	});

	// -----------------------------------------------------------------------
	// Valid tokens succeed
	// -----------------------------------------------------------------------

	it("push with valid client token returns 200", async () => {
		const token = await validClientToken(jwtSecret, gatewayId);
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(200);
	});

	it("pull with valid client token returns 200", async () => {
		const token = await validClientToken(jwtSecret, gatewayId);
		const res = await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=client-1`, {
			headers: { Authorization: `Bearer ${token}` },
		});
		expect(res.status).toBe(200);
	});

	// -----------------------------------------------------------------------
	// Token validation edge cases
	// -----------------------------------------------------------------------

	it("expired token returns 401", async () => {
		const token = await createTestJWT(
			{
				sub: "client-1",
				gw: gatewayId,
				role: "client",
				exp: Math.floor(Date.now() / 1000) - 60, // Expired 60 seconds ago
			},
			jwtSecret,
		);
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(401);
	});

	it("token signed with wrong secret returns 401", async () => {
		const token = await validClientToken("wrong-secret-key-for-testing", gatewayId);
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(401);
	});

	it("malformed token (not JWT format) returns 401", async () => {
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: "Bearer not-a-valid-jwt-token",
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(401);
	});

	it("token missing sub claim returns 401", async () => {
		const token = await createTestJWT(
			{
				gw: gatewayId,
				role: "client",
				exp: Math.floor(Date.now() / 1000) + 3600,
			},
			jwtSecret,
		);
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(401);
	});

	it("token missing gw claim returns 401", async () => {
		const token = await createTestJWT(
			{
				sub: "client-1",
				role: "client",
				exp: Math.floor(Date.now() / 1000) + 3600,
			},
			jwtSecret,
		);
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(401);
	});

	it("token missing exp claim returns 401", async () => {
		const token = await createTestJWT(
			{
				sub: "client-1",
				gw: gatewayId,
				role: "client",
			},
			jwtSecret,
		);
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(401);
	});

	// -----------------------------------------------------------------------
	// Authorization header format
	// -----------------------------------------------------------------------

	it("missing 'Bearer ' prefix in Authorization header returns 401", async () => {
		const token = await validClientToken(jwtSecret, gatewayId);
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: token, // Missing "Bearer " prefix
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(401);
	});

	// -----------------------------------------------------------------------
	// Gateway ID mismatch in JWT
	// -----------------------------------------------------------------------

	it("token with wrong gateway ID in gw claim returns 403", async () => {
		const token = await createTestJWT(
			{
				sub: "client-1",
				gw: "other-gateway",
				role: "client",
				exp: Math.floor(Date.now() / 1000) + 3600,
			},
			jwtSecret,
		);
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(403);
		expect(JSON.parse(res.body).error).toContain("Gateway ID mismatch");
	});

	// -----------------------------------------------------------------------
	// Admin role enforcement
	// -----------------------------------------------------------------------

	it("admin flush with client role returns 403", async () => {
		const token = await validClientToken(jwtSecret, gatewayId);
		const res = await req(`${baseUrl}/admin/flush/${gatewayId}`, {
			method: "POST",
			headers: { Authorization: `Bearer ${token}` },
		});
		expect(res.status).toBe(403);
		expect(JSON.parse(res.body).error).toContain("Admin role required");
	});

	it("admin flush with admin role returns 200", async () => {
		const token = await validAdminToken(jwtSecret, gatewayId);
		const res = await req(`${baseUrl}/admin/flush/${gatewayId}`, {
			method: "POST",
			headers: { Authorization: `Bearer ${token}` },
		});
		expect(res.status).toBe(200);
	});

	it("admin schema with admin role returns 200", async () => {
		const token = await validAdminToken(jwtSecret, gatewayId);
		const res = await req(`${baseUrl}/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: JSON.stringify({
				table: "tasks",
				columns: [{ name: "title", type: "string" }],
			}),
		});
		expect(res.status).toBe(200);
	});

	// -----------------------------------------------------------------------
	// Client ID mismatch between JWT and push body
	// -----------------------------------------------------------------------

	it("push with mismatched clientId returns 403", async () => {
		const token = await validClientToken(jwtSecret, gatewayId, "client-1");
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Authorization: `Bearer ${token}`,
			},
			body: pushBody([makeDelta()], "different-client"),
		});
		expect(res.status).toBe(403);
		expect(JSON.parse(res.body).error).toContain("Client ID mismatch");
	});
});

// ---------------------------------------------------------------------------
// Tests — No auth mode (jwtSecret omitted)
// ---------------------------------------------------------------------------

describe("GatewayServer without JWT auth", () => {
	const gatewayId = "noauth-gw";
	let server: GatewayServer;
	let baseUrl: string;

	beforeEach(async () => {
		server = new GatewayServer({
			gatewayId,
			port: 0,
			// No jwtSecret — auth is disabled
			flushIntervalMs: 60_000,
		});
		await server.start();
		baseUrl = `http://localhost:${server.port}`;
	});

	afterEach(async () => {
		await server.stop();
	});

	it("push without token succeeds when auth is disabled", async () => {
		const res = await req(`${baseUrl}/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: pushBody([makeDelta()]),
		});
		expect(res.status).toBe(200);
	});

	it("pull without token succeeds when auth is disabled", async () => {
		const res = await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=client-1`);
		expect(res.status).toBe(200);
	});

	it("admin flush without token succeeds when auth is disabled", async () => {
		const res = await req(`${baseUrl}/admin/flush/${gatewayId}`, {
			method: "POST",
		});
		expect(res.status).toBe(200);
	});

	it("admin schema without token succeeds when auth is disabled", async () => {
		const res = await req(`${baseUrl}/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				table: "tasks",
				columns: [{ name: "title", type: "string" }],
			}),
		});
		expect(res.status).toBe(200);
	});

	it("health check still works without auth", async () => {
		const res = await req(`${baseUrl}/health`);
		expect(res.status).toBe(200);
		expect(JSON.parse(res.body)).toEqual({ status: "ok" });
	});
});

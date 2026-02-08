import { describe, expect, it, vi } from "vitest";
import type { Env } from "../env";

// ── Mock cloudflare:workers (imported transitively via sync-gateway-do) ──
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

// ── Mock verifyToken before importing the handler ─────────────────────
vi.mock("../auth", () => ({
	verifyToken: vi.fn(),
}));

// Import after mock is set up
import { verifyToken } from "../auth";
import handler from "../index";

const mockedVerifyToken = vi.mocked(verifyToken);

/** Stub response returned by the Durable Object stub.fetch() */
const STUB_RESPONSE = new Response(JSON.stringify({ forwarded: true }), {
	status: 200,
	headers: { "Content-Type": "application/json" },
});

/**
 * Create a mock Env with a mock DurableObjectNamespace.
 * The DO stub's fetch() returns a fixed response so we can verify forwarding.
 */
function createMockEnv(): Env {
	const stubFetch = vi
		.fn<(input: RequestInfo | URL, init?: RequestInit) => Promise<Response>>()
		.mockResolvedValue(STUB_RESPONSE);

	const mockStub = { fetch: stubFetch };

	const mockNamespace = {
		idFromName: vi.fn().mockReturnValue("mock-do-id"),
		get: vi.fn().mockReturnValue(mockStub),
	};

	return {
		SYNC_GATEWAY: mockNamespace as unknown as DurableObjectNamespace,
		LAKE_BUCKET: {} as unknown as R2Bucket,
		NESSIE_URI: "http://localhost:19120",
		JWT_SECRET: "test-secret",
	};
}

/**
 * Helper to configure the mock verifyToken to return Ok.
 */
function mockAuthSuccess(clientId = "client-1", gatewayId = "gw-1"): void {
	mockedVerifyToken.mockResolvedValue({
		ok: true,
		value: { clientId, gatewayId, customClaims: { sub: clientId } },
	});
}

/**
 * Helper to configure the mock verifyToken to return Err.
 */
function mockAuthFailure(message = "Invalid token"): void {
	mockedVerifyToken.mockResolvedValue({
		ok: false,
		error: new Error(message) as ReturnType<typeof Error> & { name: string },
	});
}

describe("Worker fetch handler", () => {
	// ── Health check ──────────────────────────────────────────────────

	it("GET /health returns 200 without authentication", async () => {
		const env = createMockEnv();
		const request = new Request("https://api.example.com/health", {
			method: "GET",
		});

		const response = await handler.fetch(request, env);

		expect(response.status).toBe(200);
		const body = (await response.json()) as { status: string };
		expect(body.status).toBe("ok");
		// verifyToken should NOT have been called
		expect(mockedVerifyToken).not.toHaveBeenCalled();
	});

	// ── Authentication ────────────────────────────────────────────────

	it("returns 401 when Authorization header is missing", async () => {
		const env = createMockEnv();
		const request = new Request("https://api.example.com/sync/gw1/push", {
			method: "POST",
		});

		const response = await handler.fetch(request, env);

		expect(response.status).toBe(401);
		const body = (await response.json()) as { error: string };
		expect(body.error).toContain("Missing Bearer token");
	});

	it("returns 401 when Authorization header is not Bearer", async () => {
		const env = createMockEnv();
		const request = new Request("https://api.example.com/sync/gw1/push", {
			method: "POST",
			headers: { Authorization: "Basic dXNlcjpwYXNz" },
		});

		const response = await handler.fetch(request, env);

		expect(response.status).toBe(401);
		const body = (await response.json()) as { error: string };
		expect(body.error).toContain("Missing Bearer token");
	});

	it("returns 401 when token verification fails", async () => {
		const env = createMockEnv();
		mockAuthFailure("JWT has expired");

		const request = new Request("https://api.example.com/sync/gw1/push", {
			method: "POST",
			headers: { Authorization: "Bearer expired-token" },
		});

		const response = await handler.fetch(request, env);

		expect(response.status).toBe(401);
	});

	// ── Routing: /sync/:id/push ───────────────────────────────────────

	it("POST /sync/:id/push forwards request to DO stub", async () => {
		const env = createMockEnv();
		mockAuthSuccess("client-1", "gw1");

		// Send without body to avoid `duplex` requirement in non-CF environments.
		// The handler forwards request.body to the DO stub regardless;
		// we are testing route matching and DO namespace lookup here.
		const request = new Request("https://api.example.com/sync/gw1/push", {
			method: "POST",
			headers: {
				Authorization: "Bearer valid-token",
			},
		});

		const response = await handler.fetch(request, env);

		expect(response.status).toBe(200);
		// Verify the DO namespace was called correctly
		const ns = env.SYNC_GATEWAY as unknown as {
			idFromName: ReturnType<typeof vi.fn>;
			get: ReturnType<typeof vi.fn>;
		};
		expect(ns.idFromName).toHaveBeenCalledWith("gw1");
		expect(ns.get).toHaveBeenCalledWith("mock-do-id");
	});

	// ── Routing: /sync/:id/pull ───────────────────────────────────────

	it("GET /sync/:id/pull forwards request to DO stub", async () => {
		const env = createMockEnv();
		mockAuthSuccess("client-1", "gw1");

		const request = new Request("https://api.example.com/sync/gw1/pull?since=0&clientId=client-1", {
			method: "GET",
			headers: { Authorization: "Bearer valid-token" },
		});

		const response = await handler.fetch(request, env);

		expect(response.status).toBe(200);
		const ns = env.SYNC_GATEWAY as unknown as {
			idFromName: ReturnType<typeof vi.fn>;
		};
		expect(ns.idFromName).toHaveBeenCalledWith("gw1");
	});

	// ── Routing: /admin/flush/:id ─────────────────────────────────────

	it("POST /admin/flush/:id forwards request to DO stub", async () => {
		const env = createMockEnv();
		mockAuthSuccess("client-1", "gw1");

		const request = new Request("https://api.example.com/admin/flush/gw1", {
			method: "POST",
			headers: { Authorization: "Bearer valid-token" },
		});

		const response = await handler.fetch(request, env);

		expect(response.status).toBe(200);
		const ns = env.SYNC_GATEWAY as unknown as {
			idFromName: ReturnType<typeof vi.fn>;
		};
		expect(ns.idFromName).toHaveBeenCalledWith("gw1");
	});

	// ── 404 ───────────────────────────────────────────────────────────

	it("returns 404 for unknown paths", async () => {
		const env = createMockEnv();
		mockAuthSuccess();

		const request = new Request("https://api.example.com/unknown/path", {
			method: "GET",
			headers: { Authorization: "Bearer valid-token" },
		});

		const response = await handler.fetch(request, env);

		expect(response.status).toBe(404);
	});

	it("returns 404 for /sync without action segment", async () => {
		const env = createMockEnv();
		mockAuthSuccess();

		const request = new Request("https://api.example.com/sync/gw1", {
			method: "GET",
			headers: { Authorization: "Bearer valid-token" },
		});

		const response = await handler.fetch(request, env);

		expect(response.status).toBe(404);
	});
});

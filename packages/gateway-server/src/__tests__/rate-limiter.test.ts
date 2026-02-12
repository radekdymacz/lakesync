import { request as httpRequest, type IncomingMessage } from "node:http";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RateLimiter } from "../rate-limiter";
import { GatewayServer } from "../server";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Unit tests — RateLimiter class
// ---------------------------------------------------------------------------

describe("RateLimiter", () => {
	it("allows requests within the limit", () => {
		const limiter = new RateLimiter({ maxRequests: 3, windowMs: 60_000 });

		expect(limiter.tryConsume("client-a")).toBe(true);
		expect(limiter.tryConsume("client-a")).toBe(true);
		expect(limiter.tryConsume("client-a")).toBe(true);

		limiter.dispose();
	});

	it("rejects requests exceeding the limit", () => {
		const limiter = new RateLimiter({ maxRequests: 2, windowMs: 60_000 });

		expect(limiter.tryConsume("client-a")).toBe(true);
		expect(limiter.tryConsume("client-a")).toBe(true);
		expect(limiter.tryConsume("client-a")).toBe(false);

		limiter.dispose();
	});

	it("tracks clients independently", () => {
		const limiter = new RateLimiter({ maxRequests: 1, windowMs: 60_000 });

		expect(limiter.tryConsume("client-a")).toBe(true);
		expect(limiter.tryConsume("client-a")).toBe(false);
		expect(limiter.tryConsume("client-b")).toBe(true);

		limiter.dispose();
	});

	it("resets the window after windowMs expires", () => {
		vi.useFakeTimers();

		const limiter = new RateLimiter({ maxRequests: 1, windowMs: 1000 });

		expect(limiter.tryConsume("client-a")).toBe(true);
		expect(limiter.tryConsume("client-a")).toBe(false);

		vi.advanceTimersByTime(1001);

		expect(limiter.tryConsume("client-a")).toBe(true);

		limiter.dispose();
		vi.useRealTimers();
	});

	it("returns correct retryAfterSeconds", () => {
		vi.useFakeTimers();

		const limiter = new RateLimiter({ maxRequests: 1, windowMs: 10_000 });

		limiter.tryConsume("client-a");
		limiter.tryConsume("client-a"); // rejected

		const seconds = limiter.retryAfterSeconds("client-a");
		expect(seconds).toBe(10);

		vi.advanceTimersByTime(5000);
		const secondsAfter = limiter.retryAfterSeconds("client-a");
		expect(secondsAfter).toBe(5);

		limiter.dispose();
		vi.useRealTimers();
	});

	it("returns 0 retryAfterSeconds for unknown client", () => {
		const limiter = new RateLimiter({ maxRequests: 10, windowMs: 60_000 });
		expect(limiter.retryAfterSeconds("unknown")).toBe(0);
		limiter.dispose();
	});

	it("reset() clears all tracked clients", () => {
		const limiter = new RateLimiter({ maxRequests: 1, windowMs: 60_000 });

		limiter.tryConsume("client-a");
		expect(limiter.tryConsume("client-a")).toBe(false);

		limiter.reset();
		expect(limiter.tryConsume("client-a")).toBe(true);

		limiter.dispose();
	});

	it("uses default config values when none provided", () => {
		const limiter = new RateLimiter();

		// Default maxRequests is 100 — should allow at least 100
		for (let i = 0; i < 100; i++) {
			expect(limiter.tryConsume("client-default")).toBe(true);
		}
		expect(limiter.tryConsume("client-default")).toBe(false);

		limiter.dispose();
	});
});

// ---------------------------------------------------------------------------
// Integration tests — rate limiting in GatewayServer
// ---------------------------------------------------------------------------

describe("GatewayServer with rate limiting", () => {
	const gatewayId = "rate-limit-gw";
	let server: GatewayServer;
	let baseUrl: string;

	beforeEach(async () => {
		server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
			rateLimiter: {
				maxRequests: 3,
				windowMs: 60_000,
			},
		});
		await server.start();
		baseUrl = `http://localhost:${server.port}`;
	});

	afterEach(async () => {
		await server.stop();
	});

	it("allows requests within the rate limit", async () => {
		for (let i = 0; i < 3; i++) {
			const res = await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=client-1`);
			expect(res.status).toBe(200);
		}
	});

	it("returns 429 when rate limit exceeded", async () => {
		// Use up the limit
		for (let i = 0; i < 3; i++) {
			await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=client-1`);
		}

		// Next request should be rejected
		const res = await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=client-1`);
		expect(res.status).toBe(429);
		const body = JSON.parse(res.body);
		expect(body.error).toBe("Too many requests");
	});

	it("includes Retry-After header on 429 response", async () => {
		// Use up the limit
		for (let i = 0; i < 3; i++) {
			await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=client-1`);
		}

		const res = await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=client-1`);
		expect(res.status).toBe(429);
		expect(res.headers["retry-after"]).toBeDefined();
		expect(Number(res.headers["retry-after"])).toBeGreaterThan(0);
	});

	it("does not rate limit health check", async () => {
		// Exhaust rate limit
		for (let i = 0; i < 3; i++) {
			await req(`${baseUrl}/sync/${gatewayId}/pull?since=0&clientId=client-1`);
		}

		// Health check should still work (it returns before rate limiting)
		const res = await req(`${baseUrl}/health`);
		expect(res.status).toBe(200);
	});
});

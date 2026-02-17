import { EventEmitter } from "node:events";
import type { IncomingMessage, ServerResponse } from "node:http";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { RequestContext } from "../middleware";
import { runPipeline } from "../middleware";
import {
	type OrgIdResolver,
	type QuotaEnforcer,
	type QuotaEnforcerResult,
	quotaMiddleware,
} from "../quota-middleware";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createMockContext(overrides: Partial<RequestContext> = {}): RequestContext {
	const res = new EventEmitter() as unknown as ServerResponse;
	const headers: Record<string, string | number> = {};
	const resObj = res as unknown as Record<string, unknown>;
	resObj.writeHead = vi.fn();
	resObj.end = vi.fn();
	resObj.setHeader = vi.fn((key: string, value: string | number) => {
		headers[key] = value;
	});
	resObj.writableEnded = false;
	resObj.__headers = headers;

	return {
		req: {} as IncomingMessage,
		res,
		method: "POST",
		url: new URL("http://localhost:3000/v1/sync/gw-1/push"),
		pathname: "/v1/sync/gw-1/push",
		requestId: "req-123",
		logger: {
			info: vi.fn(),
			warn: vi.fn(),
			error: vi.fn(),
			debug: vi.fn(),
			child: vi.fn(),
		} as never,
		corsHeaders: {},
		route: { gatewayId: "gw-1", action: "push" },
		...overrides,
	};
}

function createMockEnforcer(
	overrides: {
		pushResult?: QuotaEnforcerResult;
		connectionResult?: QuotaEnforcerResult;
		pushError?: boolean;
		connectionError?: boolean;
	} = {},
): QuotaEnforcer {
	return {
		checkPush: vi.fn().mockImplementation(async () => {
			if (overrides.pushError) throw new Error("quota service down");
			return overrides.pushResult ?? { allowed: true, remaining: 9999 };
		}),
		checkConnection: vi.fn().mockImplementation(async () => {
			if (overrides.connectionError) throw new Error("quota service down");
			return overrides.connectionResult ?? { allowed: true, remaining: 4 };
		}),
	};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("quotaMiddleware", () => {
	let resolveOrgId: OrgIdResolver;

	beforeEach(() => {
		resolveOrgId = vi.fn().mockResolvedValue("org-1");
	});

	describe("push action", () => {
		it("allows push when quota check passes", async () => {
			const enforcer = createMockEnforcer();
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext();
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(next).toHaveBeenCalledOnce();
			expect(enforcer.checkPush).toHaveBeenCalledWith("org-1", 0);
		});

		it("rejects push with 429 when over quota", async () => {
			const resetAt = new Date(Date.now() + 86_400_000);
			const enforcer = createMockEnforcer({
				pushResult: {
					allowed: false,
					reason: "Monthly delta quota exceeded",
					resetAt,
				},
			});
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext();
			const next = vi.fn();

			await middleware(ctx, next);

			expect(next).not.toHaveBeenCalled();
			expect(ctx.res.writeHead).toHaveBeenCalled();
			const [status, headers] = vi.mocked(ctx.res.writeHead).mock.calls[0]!;
			expect(status).toBe(429);
			const headerObj = headers as Record<string, string>;
			expect(headerObj["X-Quota-Remaining"]).toBe("0");
			expect(headerObj["Retry-After"]).toBeDefined();
			expect(Number(headerObj["Retry-After"])).toBeGreaterThan(0);
		});

		it("sets X-Quota-Remaining header when allowed", async () => {
			const enforcer = createMockEnforcer({
				pushResult: { allowed: true, remaining: 5000 },
			});
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext();
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(ctx.res.setHeader).toHaveBeenCalledWith("X-Quota-Remaining", "5000");
			expect(next).toHaveBeenCalled();
		});
	});

	describe("ws action", () => {
		it("checks connection quota for WebSocket action", async () => {
			const enforcer = createMockEnforcer();
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext({
				route: { gatewayId: "gw-1", action: "ws" },
			});
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(enforcer.checkConnection).toHaveBeenCalledWith("org-1", "gw-1");
			expect(next).toHaveBeenCalled();
		});

		it("rejects WebSocket when connection limit reached", async () => {
			const enforcer = createMockEnforcer({
				connectionResult: {
					allowed: false,
					reason: "Connection limit reached",
				},
			});
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext({
				route: { gatewayId: "gw-1", action: "ws" },
			});
			const next = vi.fn();

			await middleware(ctx, next);

			expect(next).not.toHaveBeenCalled();
			const [status] = vi.mocked(ctx.res.writeHead).mock.calls[0]!;
			expect(status).toBe(429);
		});
	});

	describe("non-quota actions", () => {
		it("skips quota check for pull action", async () => {
			const enforcer = createMockEnforcer();
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext({
				route: { gatewayId: "gw-1", action: "pull" },
			});
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(enforcer.checkPush).not.toHaveBeenCalled();
			expect(enforcer.checkConnection).not.toHaveBeenCalled();
			expect(next).toHaveBeenCalled();
		});

		it("skips quota check for flush action", async () => {
			const enforcer = createMockEnforcer();
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext({
				route: { gatewayId: "gw-1", action: "flush" },
			});
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(enforcer.checkPush).not.toHaveBeenCalled();
			expect(next).toHaveBeenCalled();
		});

		it("skips quota check when no route is matched", async () => {
			const enforcer = createMockEnforcer();
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext({ route: undefined });
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(enforcer.checkPush).not.toHaveBeenCalled();
			expect(next).toHaveBeenCalled();
		});
	});

	describe("fail-open behaviour", () => {
		it("allows request when orgId resolver returns null", async () => {
			const enforcer = createMockEnforcer();
			const nullResolver: OrgIdResolver = vi.fn().mockResolvedValue(null);
			const middleware = quotaMiddleware(enforcer, nullResolver);
			const ctx = createMockContext();
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(enforcer.checkPush).not.toHaveBeenCalled();
			expect(next).toHaveBeenCalled();
		});

		it("allows request when orgId resolver throws", async () => {
			const enforcer = createMockEnforcer();
			const failingResolver: OrgIdResolver = vi.fn().mockRejectedValue(new Error("db down"));
			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
			const middleware = quotaMiddleware(enforcer, failingResolver);
			const ctx = createMockContext();
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(next).toHaveBeenCalled();
			expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("fail-open"));
			warnSpy.mockRestore();
		});

		it("allows request when quota check throws", async () => {
			const enforcer = createMockEnforcer({ pushError: true });
			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext();
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(next).toHaveBeenCalled();
			expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("fail-open"));
			warnSpy.mockRestore();
		});

		it("allows request when connection quota check throws", async () => {
			const enforcer = createMockEnforcer({ connectionError: true });
			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext({
				route: { gatewayId: "gw-1", action: "ws" },
			});
			const next = vi.fn().mockResolvedValue(undefined);

			await middleware(ctx, next);

			expect(next).toHaveBeenCalled();
			warnSpy.mockRestore();
		});
	});

	describe("pipeline integration", () => {
		it("can be composed in a middleware pipeline", async () => {
			const enforcer = createMockEnforcer({
				pushResult: { allowed: true, remaining: 9500 },
			});
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext();

			let reachedHandler = false;
			const handlerMiddleware = async (_ctx: RequestContext, _next: () => Promise<void>) => {
				reachedHandler = true;
			};

			await runPipeline([middleware, handlerMiddleware], ctx);

			expect(reachedHandler).toBe(true);
		});

		it("short-circuits pipeline when quota exceeded", async () => {
			const enforcer = createMockEnforcer({
				pushResult: {
					allowed: false,
					reason: "Quota exceeded",
					resetAt: new Date(Date.now() + 3600_000),
				},
			});
			const middleware = quotaMiddleware(enforcer, resolveOrgId);
			const ctx = createMockContext();

			let reachedHandler = false;
			const handlerMiddleware = async (_ctx: RequestContext, _next: () => Promise<void>) => {
				reachedHandler = true;
			};

			await runPipeline([middleware, handlerMiddleware], ctx);

			expect(reachedHandler).toBe(false);
		});
	});
});

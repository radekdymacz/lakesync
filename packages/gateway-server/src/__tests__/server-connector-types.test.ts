import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { GatewayServer } from "../server";

describe("GET /v1/connectors/types", () => {
	let server: GatewayServer;

	beforeAll(async () => {
		server = new GatewayServer({
			gatewayId: "test-gw",
			port: 0,
			jwtSecret: "test-secret-that-should-not-block",
		});
		await server.start();
	});

	afterAll(async () => {
		await server.stop();
	});

	it("returns 200 with connector descriptors (no auth required)", async () => {
		const res = await fetch(`http://localhost:${server.port}/v1/connectors/types`);
		expect(res.status).toBe(200);

		const body = (await res.json()) as Array<Record<string, unknown>>;
		expect(Array.isArray(body)).toBe(true);
		expect(body.length).toBeGreaterThanOrEqual(5);

		// Verify first entry has expected shape
		const first = body[0];
		expect(first).toHaveProperty("type");
		expect(first).toHaveProperty("displayName");
		expect(first).toHaveProperty("configSchema");
	});

	it("returns 200 even with jwtSecret configured", async () => {
		// jwtSecret is set in beforeAll — this verifies the route is unauthenticated
		const res = await fetch(`http://localhost:${server.port}/v1/connectors/types`);
		expect(res.status).toBe(200);
	});
});

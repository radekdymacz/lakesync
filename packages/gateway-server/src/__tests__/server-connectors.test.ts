import { request as httpRequest, type IncomingMessage } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { GatewayServer } from "../server";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("GatewayServer connector management", () => {
	const gatewayId = "test-gw";
	let server: GatewayServer;
	let baseUrl: string;

	afterEach(async () => {
		if (server) await server.stop();
	});

	const createServer = async () => {
		server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();
		baseUrl = `http://localhost:${server.port}`;
	};

	it("POST /admin/connectors/:gw registers a connector (no auth)", async () => {
		await createServer();
		const res = await req(`${baseUrl}/v1/admin/connectors/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				name: "my-pg",
				type: "postgres",
				postgres: { connectionString: "postgres://localhost:5432/test" },
			}),
		});
		expect(res.status).toBe(200);
		const data = JSON.parse(res.body);
		expect(data.registered).toBe(true);
		expect(data.name).toBe("my-pg");
	});

	it("POST with invalid body returns 400", async () => {
		await createServer();
		const res = await req(`${baseUrl}/v1/admin/connectors/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ invalid: true }),
		});
		expect(res.status).toBe(400);
	});

	it("POST with duplicate name returns 409", async () => {
		await createServer();
		const config = {
			name: "dup",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/test" },
		};
		await req(`${baseUrl}/v1/admin/connectors/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(config),
		});
		const res = await req(`${baseUrl}/v1/admin/connectors/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(config),
		});
		expect(res.status).toBe(409);
	});

	it("GET /admin/connectors/:gw lists connectors", async () => {
		await createServer();
		await req(`${baseUrl}/v1/admin/connectors/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				name: "src-1",
				type: "postgres",
				postgres: { connectionString: "postgres://localhost/test" },
			}),
		});
		const res = await req(`${baseUrl}/v1/admin/connectors/${gatewayId}`);
		expect(res.status).toBe(200);
		const list = JSON.parse(res.body);
		expect(list).toHaveLength(1);
		expect(list[0].name).toBe("src-1");
		expect(list[0].type).toBe("postgres");
		expect(list[0].hasIngest).toBe(false);
		// Verify no connection string leaks in response
		expect(list[0].connectionString).toBeUndefined();
		expect(list[0].postgres).toBeUndefined();
	});

	it("DELETE /admin/connectors/:gw/:name unregisters", async () => {
		await createServer();
		await req(`${baseUrl}/v1/admin/connectors/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				name: "to-delete",
				type: "postgres",
				postgres: { connectionString: "postgres://localhost/test" },
			}),
		});
		const res = await req(`${baseUrl}/v1/admin/connectors/${gatewayId}/to-delete`, {
			method: "DELETE",
		});
		expect(res.status).toBe(200);
		const data = JSON.parse(res.body);
		expect(data.unregistered).toBe(true);

		// Verify it's gone
		const listRes = await req(`${baseUrl}/v1/admin/connectors/${gatewayId}`);
		const list = JSON.parse(listRes.body);
		expect(list).toHaveLength(0);
	});

	it("DELETE non-existent connector returns 404", async () => {
		await createServer();
		const res = await req(`${baseUrl}/v1/admin/connectors/${gatewayId}/missing`, {
			method: "DELETE",
		});
		expect(res.status).toBe(404);
	});

	it("wrong gateway ID returns 404", async () => {
		await createServer();
		const res = await req(`${baseUrl}/v1/admin/connectors/wrong-gw`);
		expect(res.status).toBe(404);
	});
});

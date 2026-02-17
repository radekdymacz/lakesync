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

// ---------------------------------------------------------------------------
// Tests — Push validation
// ---------------------------------------------------------------------------

describe("GatewayServer push validation", () => {
	const gatewayId = "val-gw";
	let server: GatewayServer;
	let baseUrl: string;

	beforeEach(async () => {
		server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();
		baseUrl = `http://localhost:${server.port}`;
	});

	afterEach(async () => {
		await server.stop();
	});

	it("rejects payload exceeding 1 MiB via Content-Length header", async () => {
		const body = pushBody([makeDelta()]);
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				// Claim a Content-Length larger than 1 MiB
				"Content-Length": String(1_048_577),
			},
			body,
		});
		expect(res.status).toBe(413);
		expect(JSON.parse(res.body).error).toContain("Payload too large");
	});

	it("rejects push with more than 10,000 deltas", async () => {
		// Build an array with 10,001 minimal deltas
		const deltas: RowDelta[] = [];
		for (let i = 0; i < 10_001; i++) {
			deltas.push(makeDelta({ deltaId: `d-${i}` }));
		}
		const body = pushBody(deltas);

		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
			},
			body,
		});
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("Too many deltas");
	});

	it("accepts push with exactly 10,000 deltas", { timeout: 30_000 }, async () => {
		const deltas: RowDelta[] = [];
		for (let i = 0; i < 10_000; i++) {
			deltas.push(makeDelta({ deltaId: `d-${i}` }));
		}
		const body = pushBody(deltas);

		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
			},
			body,
		});
		expect(res.status).toBe(200);
	});

	it("accepts push with empty deltas array", async () => {
		const body = pushBody([]);
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body,
		});
		expect(res.status).toBe(200);
	});

	it("rejects push with missing clientId", async () => {
		const body = JSON.stringify({ deltas: [makeDelta()], lastSeenHlc: "0" }, bigintReplacer);
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body,
		});
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("Missing required fields");
	});

	it("rejects push with missing deltas field", async () => {
		const body = JSON.stringify({ clientId: "client-1", lastSeenHlc: "0" });
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body,
		});
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("Missing required fields");
	});

	it("rejects push with non-JSON body", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/push`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: "this is not json {{{",
		});
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("Invalid JSON");
	});
});

// ---------------------------------------------------------------------------
// Tests — Pull validation
// ---------------------------------------------------------------------------

describe("GatewayServer pull validation", () => {
	const gatewayId = "val-pull-gw";
	let server: GatewayServer;
	let baseUrl: string;

	beforeEach(async () => {
		server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();
		baseUrl = `http://localhost:${server.port}`;
	});

	afterEach(async () => {
		await server.stop();
	});

	it("rejects pull with missing since param", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?clientId=c1`);
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("since");
	});

	it("rejects pull with missing clientId param", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?since=0`);
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("clientId");
	});

	it("rejects pull with invalid since param (non-numeric)", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?since=abc&clientId=c1`);
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("since");
	});

	it("rejects pull with invalid limit param (zero)", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?since=0&clientId=c1&limit=0`);
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("limit");
	});

	it("rejects pull with invalid limit param (negative)", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?since=0&clientId=c1&limit=-5`);
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("limit");
	});

	it("rejects pull with invalid limit param (non-numeric)", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?since=0&clientId=c1&limit=abc`);
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("limit");
	});

	it("accepts pull with valid limit param", async () => {
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?since=0&clientId=c1&limit=50`);
		expect(res.status).toBe(200);
	});

	it("clamps limit to max 10,000", async () => {
		// Should not error even with a very large limit — server clamps it
		const res = await req(`${baseUrl}/v1/sync/${gatewayId}/pull?since=0&clientId=c1&limit=99999`);
		expect(res.status).toBe(200);
	});
});

// ---------------------------------------------------------------------------
// Tests — Schema validation
// ---------------------------------------------------------------------------

describe("GatewayServer schema validation", () => {
	const gatewayId = "val-schema-gw";
	let server: GatewayServer;
	let baseUrl: string;

	beforeEach(async () => {
		server = new GatewayServer({
			gatewayId,
			port: 0,
			flushIntervalMs: 60_000,
		});
		await server.start();
		baseUrl = `http://localhost:${server.port}`;
	});

	afterEach(async () => {
		await server.stop();
	});

	it("rejects schema with missing table field", async () => {
		const res = await req(`${baseUrl}/v1/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ columns: [{ name: "a", type: "string" }] }),
		});
		expect(res.status).toBe(400);
	});

	it("rejects schema with missing columns field", async () => {
		const res = await req(`${baseUrl}/v1/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ table: "t" }),
		});
		expect(res.status).toBe(400);
	});

	it("rejects schema with invalid column type", async () => {
		const res = await req(`${baseUrl}/v1/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				table: "t",
				columns: [{ name: "a", type: "invalid_type" }],
			}),
		});
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("Invalid column type");
	});

	it("rejects schema with empty column name", async () => {
		const res = await req(`${baseUrl}/v1/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				table: "t",
				columns: [{ name: "", type: "string" }],
			}),
		});
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("non-empty");
	});

	it("rejects non-JSON body for schema", async () => {
		const res = await req(`${baseUrl}/v1/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: "not json",
		});
		expect(res.status).toBe(400);
		expect(JSON.parse(res.body).error).toContain("Invalid JSON");
	});

	it("accepts schema with all valid column types", async () => {
		const res = await req(`${baseUrl}/v1/admin/schema/${gatewayId}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				table: "t",
				columns: [
					{ name: "a", type: "string" },
					{ name: "b", type: "number" },
					{ name: "c", type: "boolean" },
					{ name: "d", type: "json" },
					{ name: "e", type: "null" },
				],
			}),
		});
		expect(res.status).toBe(200);
	});
});

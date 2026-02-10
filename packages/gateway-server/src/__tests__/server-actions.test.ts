import type { Action, ActionHandler, ActionResult, HLCTimestamp, Result } from "@lakesync/core";
import { type ActionExecutionError, Ok } from "@lakesync/core";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { GatewayServer } from "../server";

/** Create a mock ActionHandler. */
function createMockHandler(): ActionHandler {
	return {
		supportedActions: [{ actionType: "send_message", description: "Send a message" }],
		async executeAction(action: Action): Promise<Result<ActionResult, ActionExecutionError>> {
			return Ok({
				actionId: action.actionId,
				data: { sent: true, channel: (action.params as Record<string, unknown>).channel },
				serverHlc: 0n as HLCTimestamp,
			});
		},
	};
}

describe("GatewayServer action route", () => {
	let server: GatewayServer;
	let port: number;

	beforeAll(async () => {
		server = new GatewayServer({
			gatewayId: "action-test",
			port: 0,
		});
		// Register handler directly on the gateway
		server.gatewayInstance.registerActionHandler("slack", createMockHandler());
		await server.start();
		port = server.port;
	});

	afterAll(async () => {
		await server.stop();
	});

	it("POST /sync/:id/action executes actions", async () => {
		const body = {
			clientId: "client-1",
			actions: [
				{
					actionId: "a1",
					clientId: "client-1",
					hlc: "100",
					connector: "slack",
					actionType: "send_message",
					params: { channel: "#general", text: "hello" },
				},
			],
		};

		const response = await fetch(`http://localhost:${port}/sync/action-test/action`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(body),
		});

		expect(response.status).toBe(200);
		const data = (await response.json()) as { results: Array<{ actionId: string }> };
		expect(data.results).toHaveLength(1);
		expect(data.results[0]!.actionId).toBe("a1");
	});

	it("returns error for unknown connector", async () => {
		const body = {
			clientId: "client-1",
			actions: [
				{
					actionId: "a2",
					clientId: "client-1",
					hlc: "200",
					connector: "unknown",
					actionType: "do_something",
					params: {},
				},
			],
		};

		const response = await fetch(`http://localhost:${port}/sync/action-test/action`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(body),
		});

		expect(response.status).toBe(200);
		const data = (await response.json()) as { results: Array<{ code: string }> };
		expect(data.results[0]!.code).toBe("ACTION_NOT_SUPPORTED");
	});

	it("rejects missing required fields", async () => {
		const response = await fetch(`http://localhost:${port}/sync/action-test/action`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ clientId: "client-1" }),
		});

		expect(response.status).toBe(400);
	});

	it("rejects invalid JSON", async () => {
		const response = await fetch(`http://localhost:${port}/sync/action-test/action`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: "not-json",
		});

		expect(response.status).toBe(400);
	});
});

describe("GatewayServer action discovery route", () => {
	let server: GatewayServer;
	let port: number;

	beforeAll(async () => {
		server = new GatewayServer({
			gatewayId: "discover-test",
			port: 0,
		});
		server.gatewayInstance.registerActionHandler("slack", createMockHandler());
		await server.start();
		port = server.port;
	});

	afterAll(async () => {
		await server.stop();
	});

	it("GET /sync/:id/actions returns registered action descriptors", async () => {
		const response = await fetch(`http://localhost:${port}/sync/discover-test/actions`);

		expect(response.status).toBe(200);
		const data = (await response.json()) as {
			connectors: Record<string, Array<{ actionType: string; description: string }>>;
		};
		expect(data.connectors).toBeDefined();
		expect(data.connectors.slack).toHaveLength(1);
		expect(data.connectors.slack![0]!.actionType).toBe("send_message");
		expect(data.connectors.slack![0]!.description).toBe("Send a message");
	});

	it("GET /sync/:id/actions returns empty connectors when none registered", async () => {
		// Use the same server but ensure no extra handlers beyond "slack"
		server.gatewayInstance.unregisterActionHandler("slack");

		try {
			const response = await fetch(`http://localhost:${port}/sync/discover-test/actions`);
			expect(response.status).toBe(200);
			const data = (await response.json()) as {
				connectors: Record<string, unknown>;
			};
			expect(data.connectors).toEqual({});
		} finally {
			// Re-register for other tests
			server.gatewayInstance.registerActionHandler("slack", createMockHandler());
		}
	});
});

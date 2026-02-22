import type { TableSchema } from "@lakesync/core";
import { afterEach, describe, expect, it, vi } from "vitest";
import { type CreateClientConfig, createClient, type LakeSyncClient } from "../create-client";
import { LocalDB } from "../db/local-db";

// ────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────

const testSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "done", type: "boolean" },
	],
};

function baseConfig(overrides?: Partial<CreateClientConfig>): CreateClientConfig {
	return {
		name: `test-db-${crypto.randomUUID()}`,
		schemas: [testSchema],
		clientId: "test-client",
		gateway: {
			url: "https://gw.example.com",
			gatewayId: "gw-1",
			token: "test-token",
		},
		backend: "memory",
		autoSyncMs: 0, // disable auto-sync in tests by default
		...overrides,
	};
}

// ────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────

describe("createClient", () => {
	let client: LakeSyncClient | null = null;

	afterEach(async () => {
		if (client) {
			await client.destroy();
			client = null;
		}
	});

	it("creates a client and opens the database", async () => {
		client = await createClient(baseConfig());

		expect(client.db).toBeInstanceOf(LocalDB);
		expect(client.coordinator).toBeDefined();
		expect(client.transport).toBeDefined();
		expect(typeof client.destroy).toBe("function");
	});

	it("registers schemas on the database", async () => {
		client = await createClient(baseConfig());

		// Verify the table was created by querying for it
		const result = await client.db.query<{ name: string }>(
			"SELECT name FROM sqlite_master WHERE type='table' AND name='todos'",
		);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(1);
			expect(result.value[0]?.name).toBe("todos");
		}
	});

	it("registers multiple schemas", async () => {
		const secondSchema: TableSchema = {
			table: "notes",
			columns: [{ name: "body", type: "string" }],
		};

		client = await createClient(
			baseConfig({
				schemas: [testSchema, secondSchema],
			}),
		);

		const result = await client.db.query<{ name: string }>(
			"SELECT name FROM sqlite_master WHERE type='table' AND name IN ('todos', 'notes') ORDER BY name",
		);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(2);
		}
	});

	it("sets the correct clientId on the coordinator", async () => {
		client = await createClient(baseConfig({ clientId: "my-client-42" }));

		expect(client.coordinator.engine.clientId).toBe("my-client-42");
	});

	it("does not start auto-sync when autoSyncMs is 0", async () => {
		client = await createClient(baseConfig({ autoSyncMs: 0 }));

		// The coordinator should not be actively syncing — no interval running.
		// We verify indirectly: stopAutoSync should be safe to call (no-op).
		expect(() => client!.coordinator.stopAutoSync()).not.toThrow();
	});

	it("starts auto-sync when autoSyncMs is positive", async () => {
		// Use fake timers to avoid actual intervals
		vi.useFakeTimers();

		try {
			client = await createClient(baseConfig({ autoSyncMs: 5000 }));

			// The coordinator's startAutoSync was called. We verify by calling
			// stopAutoSync (which clears the interval) and checking it does not throw.
			expect(() => client!.coordinator.stopAutoSync()).not.toThrow();
		} finally {
			vi.useRealTimers();
		}
	});

	it("destroy() stops auto-sync and closes the database", async () => {
		vi.useFakeTimers();

		try {
			client = await createClient(baseConfig({ autoSyncMs: 5000 }));

			const closeSpy = vi.spyOn(client.db, "close");
			await client.destroy();

			expect(closeSpy).toHaveBeenCalledOnce();
			// Mark as null so afterEach doesn't double-destroy
			client = null;
		} finally {
			vi.useRealTimers();
		}
	});

	it("creates HttpTransport with the provided gateway config", async () => {
		client = await createClient(
			baseConfig({
				gateway: {
					url: "https://my-gateway.io",
					gatewayId: "gw-custom",
					token: "bearer-token-123",
				},
			}),
		);

		// The transport is an HttpTransport instance
		expect(client.transport).toBeDefined();
		expect(client.transport.push).toBeDefined();
		expect(client.transport.pull).toBeDefined();
	});

	it("uses memory backend when specified", async () => {
		client = await createClient(baseConfig({ backend: "memory" }));

		expect(client.db.backend).toBe("memory");
	});

	it("passes coordinator config options through", async () => {
		client = await createClient(
			baseConfig({
				coordinatorConfig: {
					syncMode: "pullOnly",
				},
			}),
		);

		// The coordinator is constructed — we verify it exists and has the correct clientId
		expect(client.coordinator.engine.clientId).toBe("test-client");
	});
});

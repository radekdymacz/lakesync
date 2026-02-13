import { describe, expect, it, vi } from "vitest";
import { createFlowEngine } from "../flow/engine";
import type { FlowConfig, FlowState } from "../flow/types";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const CDC_FLOW: FlowConfig = {
	name: "postgres-to-r2",
	source: { type: "cdc", adapter: "postgres-prod" },
	store: { type: "lake", adapter: "r2-backup", format: "parquet" },
	materialise: [
		{ type: "parquet", adapter: "r2-backup", path: "current/" },
	],
};

const PUSH_FLOW: FlowConfig = {
	name: "offline-app",
	source: { type: "push", gatewayId: "company-os" },
	store: { type: "database", adapter: "postgres-prod" },
	materialise: [
		{ type: "sql", adapter: "postgres-prod", schemas: "default" },
	],
	direction: "bidirectional",
};

const ANALYTICS_FLOW: FlowConfig = {
	name: "analytics",
	source: { type: "cdc", adapter: "postgres-prod", tables: ["events"] },
	materialise: [
		{ type: "sql", adapter: "bigquery-analytics", schemas: "events" },
	],
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("FlowEngine", () => {
	describe("addFlow", () => {
		it("registers a flow successfully", () => {
			const engine = createFlowEngine();
			const result = engine.addFlow(CDC_FLOW);
			expect(result.ok).toBe(true);

			const statuses = engine.getStatus();
			expect(statuses).toHaveLength(1);
			expect(statuses[0]!.name).toBe("postgres-to-r2");
			expect(statuses[0]!.state).toBe("idle");
		});

		it("rejects a duplicate flow name", () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);
			const result = engine.addFlow(CDC_FLOW);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("FLOW_ALREADY_EXISTS");
		});

		it("rejects an invalid flow config", () => {
			const engine = createFlowEngine();
			const badConfig: FlowConfig = {
				name: "",
				source: { type: "cdc", adapter: "pg" },
				store: { type: "memory" },
			};
			const result = engine.addFlow(badConfig);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("INVALID_CONFIG");
		});

		it("registers multiple flows", () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);
			engine.addFlow(PUSH_FLOW);
			engine.addFlow(ANALYTICS_FLOW);

			const statuses = engine.getStatus();
			expect(statuses).toHaveLength(3);
		});
	});

	describe("startFlow", () => {
		it("starts a registered flow", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);

			const result = await engine.startFlow("postgres-to-r2");
			expect(result.ok).toBe(true);

			const status = engine.getStatus().find((s) => s.name === "postgres-to-r2");
			expect(status?.state).toBe("running");
			expect(status?.lastActivityAt).toBeInstanceOf(Date);
		});

		it("returns error for non-existent flow", async () => {
			const engine = createFlowEngine();
			const result = await engine.startFlow("non-existent");
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("FLOW_NOT_FOUND");
		});

		it("is idempotent for already-running flow", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);

			await engine.startFlow("postgres-to-r2");
			const result = await engine.startFlow("postgres-to-r2");
			expect(result.ok).toBe(true);

			const status = engine.getStatus().find((s) => s.name === "postgres-to-r2");
			expect(status?.state).toBe("running");
		});
	});

	describe("stopFlow", () => {
		it("stops a running flow", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);
			await engine.startFlow("postgres-to-r2");

			const result = await engine.stopFlow("postgres-to-r2");
			expect(result.ok).toBe(true);

			const status = engine.getStatus().find((s) => s.name === "postgres-to-r2");
			expect(status?.state).toBe("stopped");
		});

		it("returns error for non-existent flow", async () => {
			const engine = createFlowEngine();
			const result = await engine.stopFlow("non-existent");
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("FLOW_NOT_FOUND");
		});

		it("is idempotent for already-stopped flow", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);
			await engine.startFlow("postgres-to-r2");
			await engine.stopFlow("postgres-to-r2");

			const result = await engine.stopFlow("postgres-to-r2");
			expect(result.ok).toBe(true);
		});

		it("is idempotent for idle flow", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);

			const result = await engine.stopFlow("postgres-to-r2");
			expect(result.ok).toBe(true);
		});
	});

	describe("startAll / stopAll", () => {
		it("starts all registered flows", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);
			engine.addFlow(PUSH_FLOW);
			engine.addFlow(ANALYTICS_FLOW);

			const result = await engine.startAll();
			expect(result.ok).toBe(true);

			const statuses = engine.getStatus();
			for (const status of statuses) {
				expect(status.state).toBe("running");
			}
		});

		it("stops all running flows", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);
			engine.addFlow(PUSH_FLOW);
			await engine.startAll();

			await engine.stopAll();

			const statuses = engine.getStatus();
			for (const status of statuses) {
				expect(status.state).toBe("stopped");
			}
		});

		it("skips already-running flows in startAll", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);
			engine.addFlow(PUSH_FLOW);

			await engine.startFlow("postgres-to-r2");
			const result = await engine.startAll();
			expect(result.ok).toBe(true);

			const statuses = engine.getStatus();
			expect(statuses.every((s) => s.state === "running")).toBe(true);
		});
	});

	describe("getStatus", () => {
		it("returns empty array when no flows", () => {
			const engine = createFlowEngine();
			expect(engine.getStatus()).toEqual([]);
		});

		it("returns correct initial status", () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);

			const statuses = engine.getStatus();
			expect(statuses).toHaveLength(1);
			expect(statuses[0]).toEqual({
				name: "postgres-to-r2",
				state: "idle",
				deltasProcessed: 0,
			});
		});

		it("tracks lastActivityAt after start", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);

			const before = new Date();
			await engine.startFlow("postgres-to-r2");
			const after = new Date();

			const status = engine.getStatus()[0]!;
			expect(status.lastActivityAt).toBeDefined();
			expect(status.lastActivityAt!.getTime()).toBeGreaterThanOrEqual(before.getTime());
			expect(status.lastActivityAt!.getTime()).toBeLessThanOrEqual(after.getTime());
		});
	});

	describe("state change callback", () => {
		it("invokes onFlowStateChange on transitions", async () => {
			const transitions: Array<{ name: string; from: FlowState; to: FlowState }> = [];

			const engine = createFlowEngine({
				onFlowStateChange: (name, from, to) => {
					transitions.push({ name, from, to });
				},
			});

			engine.addFlow(CDC_FLOW);
			await engine.startFlow("postgres-to-r2");
			await engine.stopFlow("postgres-to-r2");

			expect(transitions).toEqual([
				{ name: "postgres-to-r2", from: "idle", to: "running" },
				{ name: "postgres-to-r2", from: "running", to: "stopped" },
			]);
		});
	});

	describe("isolation", () => {
		it("one flow stopping does not affect others", async () => {
			const engine = createFlowEngine();
			engine.addFlow(CDC_FLOW);
			engine.addFlow(PUSH_FLOW);

			await engine.startAll();
			await engine.stopFlow("postgres-to-r2");

			const statuses = engine.getStatus();
			const cdcStatus = statuses.find((s) => s.name === "postgres-to-r2");
			const pushStatus = statuses.find((s) => s.name === "offline-app");

			expect(cdcStatus?.state).toBe("stopped");
			expect(pushStatus?.state).toBe("running");
		});
	});
});

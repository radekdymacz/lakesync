import { describe, expect, it } from "vitest";
import { parseFlowConfig, parseFlows, validateFlowConfig } from "../flow/dsl";
import type { FlowConfig } from "../flow/types";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const CDC_FLOW: FlowConfig = {
	name: "postgres-to-r2-backup",
	source: { type: "cdc", adapter: "postgres-prod" },
	store: { type: "lake", adapter: "r2-backup", format: "parquet" },
	materialise: [{ type: "parquet", adapter: "r2-backup", path: "current/" }],
};

const PUSH_BIDIR_FLOW: FlowConfig = {
	name: "offline-app",
	source: { type: "push", gatewayId: "company-os" },
	store: { type: "database", adapter: "postgres-prod" },
	materialise: [{ type: "sql", adapter: "postgres-prod", schemas: "default" }],
	direction: "bidirectional",
};

const ANALYTICS_FLOW: FlowConfig = {
	name: "analytics",
	source: { type: "cdc", adapter: "postgres-prod", tables: ["events"] },
	materialise: [{ type: "sql", adapter: "bigquery-analytics", schemas: "events" }],
};

// ---------------------------------------------------------------------------
// parseFlows
// ---------------------------------------------------------------------------

describe("parseFlows", () => {
	it("parses a single flow config as JSON string", () => {
		const result = parseFlows(JSON.stringify(CDC_FLOW));
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toHaveLength(1);
		expect(result.value[0]!.name).toBe("postgres-to-r2-backup");
	});

	it("parses an array of flow configs", () => {
		const result = parseFlows(JSON.stringify([CDC_FLOW, PUSH_BIDIR_FLOW, ANALYTICS_FLOW]));
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toHaveLength(3);
		expect(result.value[0]!.name).toBe("postgres-to-r2-backup");
		expect(result.value[1]!.name).toBe("offline-app");
		expect(result.value[2]!.name).toBe("analytics");
	});

	it("rejects invalid JSON", () => {
		const result = parseFlows("{not valid json}");
		expect(result.ok).toBe(false);
		if (result.ok) return;
		expect(result.error.code).toBe("INVALID_CONFIG");
		expect(result.error.message).toContain("JSON");
	});

	it("rejects configs with validation errors inside array", () => {
		const badFlow = { name: "", source: { type: "cdc", adapter: "pg" }, store: { type: "memory" } };
		const result = parseFlows(JSON.stringify([badFlow]));
		expect(result.ok).toBe(false);
		if (result.ok) return;
		expect(result.error.code).toBe("INVALID_CONFIG");
	});
});

// ---------------------------------------------------------------------------
// parseFlowConfig
// ---------------------------------------------------------------------------

describe("parseFlowConfig", () => {
	it("parses a valid CDC flow config", () => {
		const result = parseFlowConfig(CDC_FLOW);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.name).toBe("postgres-to-r2-backup");
		expect(result.value.source.type).toBe("cdc");
		expect(result.value.store?.type).toBe("lake");
	});

	it("parses a valid push bidirectional flow config", () => {
		const result = parseFlowConfig(PUSH_BIDIR_FLOW);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.direction).toBe("bidirectional");
		expect(result.value.source.type).toBe("push");
	});

	it("parses a materialise-only flow (no store)", () => {
		const result = parseFlowConfig(ANALYTICS_FLOW);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.store).toBeUndefined();
		expect(result.value.materialise).toHaveLength(1);
	});

	it("parses a store-only flow (no materialise)", () => {
		const config = {
			name: "store-only",
			source: { type: "cdc", adapter: "postgres-prod" },
			store: { type: "memory" },
		};
		const result = parseFlowConfig(config);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.materialise).toBeUndefined();
		expect(result.value.store?.type).toBe("memory");
	});

	it("parses a poll source", () => {
		const config = {
			name: "poll-flow",
			source: { type: "poll", adapter: "mysql-prod", intervalMs: 5000 },
			store: { type: "database", adapter: "pg" },
		};
		const result = parseFlowConfig(config);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.source.type).toBe("poll");
	});

	it("parses a watch source", () => {
		const config = {
			name: "watch-flow",
			source: { type: "watch", adapter: "s3-data", prefix: "incoming/" },
			store: { type: "lake", adapter: "r2-archive" },
		};
		const result = parseFlowConfig(config);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.source.type).toBe("watch");
	});

	it("parses client materialise type", () => {
		const config = {
			name: "client-mat",
			source: { type: "push", gatewayId: "gw-1" },
			materialise: [{ type: "client", gatewayId: "gw-1" }],
		};
		const result = parseFlowConfig(config);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.materialise![0]!.type).toBe("client");
	});

	describe("rejection cases", () => {
		it("rejects null input", () => {
			const result = parseFlowConfig(null);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("INVALID_CONFIG");
		});

		it("rejects array input", () => {
			const result = parseFlowConfig([]);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("INVALID_CONFIG");
		});

		it("rejects missing name", () => {
			const result = parseFlowConfig({
				source: { type: "cdc", adapter: "pg" },
				store: { type: "memory" },
			});
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("name");
		});

		it("rejects missing source", () => {
			const result = parseFlowConfig({ name: "test" });
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("source");
		});

		it("rejects invalid source type", () => {
			const result = parseFlowConfig({
				name: "test",
				source: { type: "invalid" },
				store: { type: "memory" },
			});
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("source type");
		});

		it("rejects invalid store type", () => {
			const result = parseFlowConfig({
				name: "test",
				source: { type: "cdc", adapter: "pg" },
				store: { type: "invalid" },
			});
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("store type");
		});

		it("rejects invalid materialise type", () => {
			const result = parseFlowConfig({
				name: "test",
				source: { type: "cdc", adapter: "pg" },
				materialise: [{ type: "invalid" }],
			});
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("materialise type");
		});

		it("rejects cdc source without adapter", () => {
			const result = parseFlowConfig({
				name: "test",
				source: { type: "cdc" },
				store: { type: "memory" },
			});
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("adapter");
		});

		it("rejects push source without gatewayId", () => {
			const result = parseFlowConfig({
				name: "test",
				source: { type: "push" },
				store: { type: "memory" },
			});
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("gatewayId");
		});

		it("rejects sql materialise without schemas", () => {
			const result = parseFlowConfig({
				name: "test",
				source: { type: "cdc", adapter: "pg" },
				materialise: [{ type: "sql", adapter: "pg" }],
			});
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("schemas");
		});

		it("rejects invalid direction value", () => {
			const result = parseFlowConfig({
				name: "test",
				source: { type: "push", gatewayId: "gw" },
				store: { type: "memory" },
				direction: "wrong",
			});
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("direction");
		});
	});
});

// ---------------------------------------------------------------------------
// validateFlowConfig
// ---------------------------------------------------------------------------

describe("validateFlowConfig", () => {
	it("accepts a valid config", () => {
		const result = validateFlowConfig(CDC_FLOW);
		expect(result.ok).toBe(true);
	});

	it("rejects empty name", () => {
		const result = validateFlowConfig({ ...CDC_FLOW, name: "" });
		expect(result.ok).toBe(false);
		if (result.ok) return;
		expect(result.error.message).toContain("name");
	});

	it("rejects whitespace-only name", () => {
		const result = validateFlowConfig({ ...CDC_FLOW, name: "   " });
		expect(result.ok).toBe(false);
		if (result.ok) return;
		expect(result.error.message).toContain("name");
	});

	it("rejects bidirectional with cdc source", () => {
		const result = validateFlowConfig({ ...CDC_FLOW, direction: "bidirectional" });
		expect(result.ok).toBe(false);
		if (result.ok) return;
		expect(result.error.message).toContain("Bidirectional");
		expect(result.error.message).toContain("push");
	});

	it("rejects bidirectional with poll source", () => {
		const config: FlowConfig = {
			name: "bad",
			source: { type: "poll", adapter: "pg" },
			store: { type: "memory" },
			direction: "bidirectional",
		};
		const result = validateFlowConfig(config);
		expect(result.ok).toBe(false);
		if (result.ok) return;
		expect(result.error.message).toContain("Bidirectional");
	});

	it("rejects bidirectional with watch source", () => {
		const config: FlowConfig = {
			name: "bad",
			source: { type: "watch", adapter: "s3" },
			store: { type: "memory" },
			direction: "bidirectional",
		};
		const result = validateFlowConfig(config);
		expect(result.ok).toBe(false);
	});

	it("accepts bidirectional with push source", () => {
		const result = validateFlowConfig(PUSH_BIDIR_FLOW);
		expect(result.ok).toBe(true);
	});

	it("rejects config with neither store nor materialise", () => {
		const config: FlowConfig = {
			name: "bare",
			source: { type: "cdc", adapter: "pg" },
		};
		const result = validateFlowConfig(config);
		expect(result.ok).toBe(false);
		if (result.ok) return;
		expect(result.error.message).toContain("store");
		expect(result.error.message).toContain("materialise");
	});

	it("rejects config with empty materialise array and no store", () => {
		const config: FlowConfig = {
			name: "bare",
			source: { type: "cdc", adapter: "pg" },
			materialise: [],
		};
		const result = validateFlowConfig(config);
		expect(result.ok).toBe(false);
	});

	it("accepts config with store only", () => {
		const config: FlowConfig = {
			name: "store-only",
			source: { type: "cdc", adapter: "pg" },
			store: { type: "memory" },
		};
		const result = validateFlowConfig(config);
		expect(result.ok).toBe(true);
	});

	it("accepts config with materialise only", () => {
		const result = validateFlowConfig(ANALYTICS_FLOW);
		expect(result.ok).toBe(true);
	});
});

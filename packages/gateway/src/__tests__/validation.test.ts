import type { HLCTimestamp, SyncRulesConfig } from "@lakesync/core";
import { bigintReplacer } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { MAX_DELTAS_PER_PUSH, MAX_PULL_LIMIT } from "../constants";
import {
	buildSyncRulesContext,
	parsePullParams,
	pushErrorToStatus,
	validateActionBody,
	validatePushBody,
	validateSchemaBody,
} from "../validation";

// ---------------------------------------------------------------------------
// validatePushBody
// ---------------------------------------------------------------------------

describe("validatePushBody", () => {
	it("returns Ok for a valid push body", () => {
		const raw = JSON.stringify(
			{ clientId: "c1", deltas: [{ table: "t", rowId: "r1" }], lastSeenHlc: 0n },
			bigintReplacer,
		);
		const result = validatePushBody(raw);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.clientId).toBe("c1");
			expect(result.value.deltas).toHaveLength(1);
		}
	});

	it("returns error for missing clientId", () => {
		const raw = JSON.stringify({ deltas: [] });
		const result = validatePushBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("clientId");
		}
	});

	it("returns error for missing deltas", () => {
		const raw = JSON.stringify({ clientId: "c1" });
		const result = validatePushBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("deltas");
		}
	});

	it("returns 403 when headerClientId does not match body clientId", () => {
		const raw = JSON.stringify({ clientId: "c1", deltas: [], lastSeenHlc: "0" });
		const result = validatePushBody(raw, "different-client");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(403);
			expect(result.error.message).toContain("mismatch");
		}
	});

	it("allows push when headerClientId matches body clientId", () => {
		const raw = JSON.stringify({ clientId: "c1", deltas: [], lastSeenHlc: "0" });
		const result = validatePushBody(raw, "c1");
		expect(result.ok).toBe(true);
	});

	it("returns error when delta count exceeds MAX_DELTAS_PER_PUSH", () => {
		const deltas = Array.from({ length: MAX_DELTAS_PER_PUSH + 1 }, (_, i) => ({
			table: "t",
			rowId: `r${i}`,
		}));
		const raw = JSON.stringify({ clientId: "c1", deltas, lastSeenHlc: "0" });
		const result = validatePushBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("10,000");
		}
	});

	it("returns error for invalid JSON", () => {
		const result = validatePushBody("not-json{{{");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("Invalid JSON");
		}
	});

	it("revives bigint HLC values", () => {
		const raw = JSON.stringify({ clientId: "c1", deltas: [], lastSeenHlc: 12345n }, bigintReplacer);
		const result = validatePushBody(raw);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.lastSeenHlc).toBe(12345n);
		}
	});
});

// ---------------------------------------------------------------------------
// parsePullParams
// ---------------------------------------------------------------------------

describe("parsePullParams", () => {
	it("returns Ok for valid params", () => {
		const result = parsePullParams({
			since: "100",
			clientId: "c1",
			limit: "50",
			source: null,
		});
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.clientId).toBe("c1");
			expect(result.value.sinceHlc).toBe(100n as HLCTimestamp);
			expect(result.value.maxDeltas).toBe(50);
			expect(result.value.source).toBeUndefined();
		}
	});

	it("returns error when since is missing", () => {
		const result = parsePullParams({
			since: null,
			clientId: "c1",
			limit: null,
			source: null,
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("since");
		}
	});

	it("returns error when clientId is missing", () => {
		const result = parsePullParams({
			since: "100",
			clientId: null,
			limit: null,
			source: null,
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("clientId");
		}
	});

	it("returns error for non-numeric since", () => {
		const result = parsePullParams({
			since: "not-a-number",
			clientId: "c1",
			limit: null,
			source: null,
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("decimal integer");
		}
	});

	it("clamps limit to MAX_PULL_LIMIT", () => {
		const result = parsePullParams({
			since: "0",
			clientId: "c1",
			limit: "999999",
			source: null,
		});
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.maxDeltas).toBe(MAX_PULL_LIMIT);
		}
	});

	it("passes source through when provided", () => {
		const result = parsePullParams({
			since: "0",
			clientId: "c1",
			limit: null,
			source: "postgres",
		});
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.source).toBe("postgres");
		}
	});

	it("uses default limit when not provided", () => {
		const result = parsePullParams({
			since: "0",
			clientId: "c1",
			limit: null,
			source: null,
		});
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.maxDeltas).toBe(100);
		}
	});

	it("returns error for negative limit", () => {
		const result = parsePullParams({
			since: "0",
			clientId: "c1",
			limit: "-5",
			source: null,
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("positive integer");
		}
	});
});

// ---------------------------------------------------------------------------
// validateActionBody
// ---------------------------------------------------------------------------

describe("validateActionBody", () => {
	it("returns Ok for a valid action body", () => {
		const raw = JSON.stringify({ clientId: "c1", actions: [{ actionType: "create" }] });
		const result = validateActionBody(raw);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.clientId).toBe("c1");
			expect(result.value.actions).toHaveLength(1);
		}
	});

	it("returns error for missing clientId", () => {
		const raw = JSON.stringify({ actions: [] });
		const result = validateActionBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("clientId");
		}
	});

	it("returns error for missing actions", () => {
		const raw = JSON.stringify({ clientId: "c1" });
		const result = validateActionBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("actions");
		}
	});

	it("returns 403 when headerClientId does not match", () => {
		const raw = JSON.stringify({ clientId: "c1", actions: [] });
		const result = validateActionBody(raw, "other-client");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(403);
			expect(result.error.message).toContain("mismatch");
		}
	});

	it("returns error for invalid JSON", () => {
		const result = validateActionBody("{broken");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("Invalid JSON");
		}
	});
});

// ---------------------------------------------------------------------------
// validateSchemaBody
// ---------------------------------------------------------------------------

describe("validateSchemaBody", () => {
	it("returns Ok for a valid schema", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [
				{ name: "id", type: "string" },
				{ name: "age", type: "number" },
			],
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.table).toBe("users");
			expect(result.value.columns).toHaveLength(2);
		}
	});

	it("returns error for missing table", () => {
		const raw = JSON.stringify({ columns: [{ name: "id", type: "string" }] });
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("table");
		}
	});

	it("returns error for missing columns", () => {
		const raw = JSON.stringify({ table: "users" });
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("columns");
		}
	});

	it("returns error for invalid column type", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [{ name: "id", type: "bigint" }],
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("bigint");
			expect(result.error.message).toContain("Allowed");
		}
	});

	it("returns error for empty column name", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [{ name: "", type: "string" }],
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("non-empty");
		}
	});

	it("returns error for invalid JSON", () => {
		const result = validateSchemaBody("{{bad");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
		}
	});

	it("returns Ok for a schema with all new fields", () => {
		const raw = JSON.stringify({
			table: "orders",
			columns: [
				{ name: "org_id", type: "string" },
				{ name: "order_id", type: "string" },
				{ name: "ext_ref", type: "string" },
			],
			primaryKey: ["org_id", "order_id"],
			softDelete: true,
			externalIdColumn: "ext_ref",
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.primaryKey).toEqual(["org_id", "order_id"]);
			expect(result.value.softDelete).toBe(true);
			expect(result.value.externalIdColumn).toBe("ext_ref");
		}
	});

	it("allows primaryKey with row_id entry", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [{ name: "name", type: "string" }],
			primaryKey: ["row_id"],
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(true);
	});

	it("returns error for empty primaryKey array", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [{ name: "name", type: "string" }],
			primaryKey: [],
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("primaryKey");
		}
	});

	it("returns error for primaryKey referencing non-existent column", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [{ name: "name", type: "string" }],
			primaryKey: ["missing_col"],
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("missing_col");
		}
	});

	it("returns error for non-boolean softDelete", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [{ name: "name", type: "string" }],
			softDelete: "yes",
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("softDelete");
		}
	});

	it("returns error for externalIdColumn referencing non-existent column", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [{ name: "name", type: "string" }],
			externalIdColumn: "ghost",
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("ghost");
		}
	});

	it("returns error for empty externalIdColumn string", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [{ name: "name", type: "string" }],
			externalIdColumn: "",
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.status).toBe(400);
			expect(result.error.message).toContain("externalIdColumn");
		}
	});

	it("allows softDelete false", () => {
		const raw = JSON.stringify({
			table: "users",
			columns: [{ name: "name", type: "string" }],
			softDelete: false,
		});
		const result = validateSchemaBody(raw);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.softDelete).toBe(false);
		}
	});
});

// ---------------------------------------------------------------------------
// pushErrorToStatus
// ---------------------------------------------------------------------------

describe("pushErrorToStatus", () => {
	it("maps CLOCK_DRIFT to 409", () => {
		expect(pushErrorToStatus("CLOCK_DRIFT")).toBe(409);
	});

	it("maps SCHEMA_MISMATCH to 422", () => {
		expect(pushErrorToStatus("SCHEMA_MISMATCH")).toBe(422);
	});

	it("maps BACKPRESSURE to 503", () => {
		expect(pushErrorToStatus("BACKPRESSURE")).toBe(503);
	});

	it("maps unknown codes to 500", () => {
		expect(pushErrorToStatus("SOMETHING_ELSE")).toBe(500);
	});
});

// ---------------------------------------------------------------------------
// buildSyncRulesContext
// ---------------------------------------------------------------------------

describe("buildSyncRulesContext", () => {
	const rules: SyncRulesConfig = {
		version: 1,
		buckets: [
			{ name: "b1", tables: ["users"], filters: [{ column: "org", op: "eq", value: "jwt:org" }] },
		],
	};

	it("returns SyncRulesContext when rules and claims are provided", () => {
		const claims = { org: "acme" };
		const ctx = buildSyncRulesContext(rules, claims);
		expect(ctx).toBeDefined();
		expect(ctx!.claims).toEqual({ org: "acme" });
		expect(ctx!.rules).toBe(rules);
	});

	it("returns undefined when buckets are empty", () => {
		const emptyRules: SyncRulesConfig = { version: 1, buckets: [] };
		const ctx = buildSyncRulesContext(emptyRules, { org: "acme" });
		expect(ctx).toBeUndefined();
	});

	it("returns undefined when rules are undefined", () => {
		const ctx = buildSyncRulesContext(undefined, { org: "acme" });
		expect(ctx).toBeUndefined();
	});
});

import { describe, expect, it } from "vitest";
import type { HLCTimestamp } from "../../hlc/types";
import { validateAction } from "../validate";

describe("validateAction", () => {
	const validAction = {
		actionId: "abc123",
		clientId: "client-1",
		hlc: 100n as HLCTimestamp,
		connector: "github",
		actionType: "create_pr",
		params: { title: "Fix bug" },
	};

	it("accepts a valid action", () => {
		const result = validateAction(validAction);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.actionId).toBe("abc123");
		}
	});

	it("accepts action with idempotencyKey", () => {
		const result = validateAction({ ...validAction, idempotencyKey: "key-1" });
		expect(result.ok).toBe(true);
	});

	it("rejects null", () => {
		const result = validateAction(null);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("non-null object");
		}
	});

	it("rejects missing actionId", () => {
		const result = validateAction({ ...validAction, actionId: "" });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("actionId");
		}
	});

	it("rejects missing clientId", () => {
		const result = validateAction({ ...validAction, clientId: "" });
		expect(result.ok).toBe(false);
	});

	it("rejects non-bigint hlc", () => {
		const result = validateAction({ ...validAction, hlc: 123 });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("bigint");
		}
	});

	it("rejects missing connector", () => {
		const result = validateAction({ ...validAction, connector: "" });
		expect(result.ok).toBe(false);
	});

	it("rejects missing actionType", () => {
		const result = validateAction({ ...validAction, actionType: "" });
		expect(result.ok).toBe(false);
	});

	it("rejects array as params", () => {
		const result = validateAction({ ...validAction, params: [1, 2] });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("params");
		}
	});

	it("rejects null params", () => {
		const result = validateAction({ ...validAction, params: null });
		expect(result.ok).toBe(false);
	});

	it("rejects non-string idempotencyKey", () => {
		const result = validateAction({ ...validAction, idempotencyKey: 42 });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("idempotencyKey");
		}
	});
});

import { describe, expect, it } from "vitest";
import type { HLCTimestamp } from "../../hlc/types";
import { generateActionId } from "../generate-id";
import type { ActionErrorResult, ActionResult } from "../types";
import { isActionError } from "../types";

describe("isActionError", () => {
	it("returns true for an error result", () => {
		const errorResult: ActionErrorResult = {
			actionId: "abc",
			code: "NOT_FOUND",
			message: "Not found",
			retryable: false,
		};
		expect(isActionError(errorResult)).toBe(true);
	});

	it("returns false for a success result", () => {
		const successResult: ActionResult = {
			actionId: "abc",
			data: { id: "123" },
			serverHlc: 100n as HLCTimestamp,
		};
		expect(isActionError(successResult)).toBe(false);
	});
});

describe("generateActionId", () => {
	it("produces a deterministic hex string", async () => {
		const params = {
			clientId: "client-1",
			hlc: 123456n as HLCTimestamp,
			connector: "github",
			actionType: "create_pr",
			params: { title: "Fix bug" },
		};

		const id1 = await generateActionId(params);
		const id2 = await generateActionId(params);

		expect(id1).toBe(id2);
		expect(id1).toMatch(/^[0-9a-f]{64}$/);
	});

	it("produces different IDs for different params", async () => {
		const base = {
			clientId: "client-1",
			hlc: 123456n as HLCTimestamp,
			connector: "github",
			actionType: "create_pr",
		};

		const id1 = await generateActionId({ ...base, params: { title: "A" } });
		const id2 = await generateActionId({ ...base, params: { title: "B" } });

		expect(id1).not.toBe(id2);
	});

	it("is stable regardless of params key order", async () => {
		const base = {
			clientId: "client-1",
			hlc: 100n as HLCTimestamp,
			connector: "slack",
			actionType: "send",
		};

		const id1 = await generateActionId({ ...base, params: { a: 1, b: 2 } });
		const id2 = await generateActionId({ ...base, params: { b: 2, a: 1 } });

		expect(id1).toBe(id2);
	});
});

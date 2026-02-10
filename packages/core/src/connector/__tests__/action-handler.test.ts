import { describe, expect, it } from "vitest";
import { isActionHandler } from "../action-handler";

describe("isActionHandler", () => {
	it("returns true for a valid action handler", () => {
		const handler = {
			supportedActions: [{ actionType: "send", description: "Send message" }],
			executeAction: async () => ({
				ok: true as const,
				value: { actionId: "x", data: {}, serverHlc: 0n },
			}),
		};
		expect(isActionHandler(handler)).toBe(true);
	});

	it("returns false for null", () => {
		expect(isActionHandler(null)).toBe(false);
	});

	it("returns false for undefined", () => {
		expect(isActionHandler(undefined)).toBe(false);
	});

	it("returns false for a plain object without supportedActions", () => {
		expect(isActionHandler({ executeAction: () => {} })).toBe(false);
	});

	it("returns false for a plain object without executeAction", () => {
		expect(isActionHandler({ supportedActions: [] })).toBe(false);
	});

	it("returns false for a string", () => {
		expect(isActionHandler("handler")).toBe(false);
	});

	it("returns true even if supportedActions is empty", () => {
		const handler = {
			supportedActions: [],
			executeAction: async () => ({ ok: true as const, value: null }),
		};
		expect(isActionHandler(handler)).toBe(true);
	});
});

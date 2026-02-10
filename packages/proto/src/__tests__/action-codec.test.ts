import type { HLCTimestamp } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import {
	decodeActionPush,
	decodeActionResponse,
	encodeActionPush,
	encodeActionResponse,
} from "../codec";

describe("ActionPush codec", () => {
	it("round-trips an ActionPush with a single action", () => {
		const push = {
			clientId: "client-1",
			actions: [
				{
					actionId: "a1",
					clientId: "client-1",
					hlc: 12345n as HLCTimestamp,
					connector: "github",
					actionType: "create_pr",
					params: { title: "Fix bug", branch: "fix/123" },
				},
			],
		};

		const encoded = encodeActionPush(push);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeActionPush(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.clientId).toBe("client-1");
		expect(decoded.value.actions).toHaveLength(1);
		expect(decoded.value.actions[0]!.actionId).toBe("a1");
		expect(decoded.value.actions[0]!.connector).toBe("github");
		expect(decoded.value.actions[0]!.actionType).toBe("create_pr");
		expect(decoded.value.actions[0]!.params).toEqual({ title: "Fix bug", branch: "fix/123" });
		expect(decoded.value.actions[0]!.hlc).toBe(12345n);
	});

	it("round-trips an ActionPush with idempotencyKey", () => {
		const push = {
			clientId: "c1",
			actions: [
				{
					actionId: "a1",
					clientId: "c1",
					hlc: 1n as HLCTimestamp,
					connector: "slack",
					actionType: "send",
					params: { text: "hello" },
					idempotencyKey: "idem-123",
				},
			],
		};

		const encoded = encodeActionPush(push);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeActionPush(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.actions[0]!.idempotencyKey).toBe("idem-123");
	});

	it("round-trips multiple actions", () => {
		const push = {
			clientId: "c1",
			actions: [
				{
					actionId: "a1",
					clientId: "c1",
					hlc: 1n as HLCTimestamp,
					connector: "github",
					actionType: "create_pr",
					params: { title: "PR 1" },
				},
				{
					actionId: "a2",
					clientId: "c1",
					hlc: 2n as HLCTimestamp,
					connector: "slack",
					actionType: "send",
					params: { text: "hello" },
				},
			],
		};

		const encoded = encodeActionPush(push);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeActionPush(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.actions).toHaveLength(2);
	});
});

describe("ActionResponse codec", () => {
	it("round-trips a success response", () => {
		const response = {
			results: [
				{
					actionId: "a1",
					data: { prUrl: "https://github.com/org/repo/pull/1" },
					serverHlc: 999n as HLCTimestamp,
				},
			],
			serverHlc: 1000n as HLCTimestamp,
		};

		const encoded = encodeActionResponse(response);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeActionResponse(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.serverHlc).toBe(1000n);
		expect(decoded.value.results).toHaveLength(1);

		const r = decoded.value.results[0]!;
		expect("data" in r).toBe(true);
		if ("data" in r) {
			expect(r.data).toEqual({ prUrl: "https://github.com/org/repo/pull/1" });
			expect(r.serverHlc).toBe(999n);
		}
	});

	it("round-trips an error response", () => {
		const response = {
			results: [
				{
					actionId: "a1",
					code: "NOT_FOUND",
					message: "Repository not found",
					retryable: false,
				},
			],
			serverHlc: 500n as HLCTimestamp,
		};

		const encoded = encodeActionResponse(response);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeActionResponse(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		const r = decoded.value.results[0]!;
		expect("code" in r).toBe(true);
		if ("code" in r) {
			expect(r.code).toBe("NOT_FOUND");
			expect(r.message).toBe("Repository not found");
			expect(r.retryable).toBe(false);
		}
	});

	it("round-trips mixed success and error results", () => {
		const response = {
			results: [
				{
					actionId: "a1",
					data: { id: "123" },
					serverHlc: 10n as HLCTimestamp,
				},
				{
					actionId: "a2",
					code: "RATE_LIMITED",
					message: "Too many requests",
					retryable: true,
				},
			],
			serverHlc: 20n as HLCTimestamp,
		};

		const encoded = encodeActionResponse(response);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeActionResponse(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.results).toHaveLength(2);
		expect("data" in decoded.value.results[0]!).toBe(true);
		expect("code" in decoded.value.results[1]!).toBe(true);
	});
});

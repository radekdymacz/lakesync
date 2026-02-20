import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { WEBHOOK_EVENT_TYPES } from "../webhooks/types";
import {
	signPayload,
	verifyPayloadSignature,
	WebhookDispatcher,
} from "../webhooks/webhook-dispatcher";
import { MemoryWebhookStore } from "../webhooks/webhook-store";

describe("MemoryWebhookStore", () => {
	let store: MemoryWebhookStore;

	beforeEach(() => {
		store = new MemoryWebhookStore();
	});

	afterEach(() => {
		store.clear();
	});

	it("creates and retrieves an endpoint", async () => {
		const result = await store.createEndpoint({
			orgId: "org_abc",
			url: "https://example.com/webhook",
			events: ["sync.push", "flush.complete"],
			secret: "my-secret",
		});
		expect(result.ok).toBe(true);
		if (!result.ok) return;

		const getResult = await store.getEndpoint(result.value.id);
		expect(getResult.ok).toBe(true);
		if (getResult.ok) {
			expect(getResult.value?.url).toBe("https://example.com/webhook");
			expect(getResult.value?.events).toEqual(["sync.push", "flush.complete"]);
		}
	});

	it("lists endpoints by org", async () => {
		await store.createEndpoint({
			orgId: "org_abc",
			url: "https://a.com/wh",
			events: ["sync.push"],
			secret: "s1",
		});
		await store.createEndpoint({
			orgId: "org_abc",
			url: "https://b.com/wh",
			events: ["flush.complete"],
			secret: "s2",
		});
		await store.createEndpoint({
			orgId: "org_other",
			url: "https://c.com/wh",
			events: ["sync.push"],
			secret: "s3",
		});

		const result = await store.listByOrg("org_abc");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(2);
		}
	});

	it("deletes an endpoint", async () => {
		const created = await store.createEndpoint({
			orgId: "org_abc",
			url: "https://example.com/wh",
			events: ["sync.push"],
			secret: "s",
		});
		expect(created.ok).toBe(true);
		if (!created.ok) return;

		const deleteResult = await store.deleteEndpoint(created.value.id);
		expect(deleteResult.ok).toBe(true);

		const getResult = await store.getEndpoint(created.value.id);
		expect(getResult.ok).toBe(true);
		if (getResult.ok) {
			expect(getResult.value).toBeNull();
		}
	});

	it("returns NOT_FOUND when deleting nonexistent endpoint", async () => {
		const result = await store.deleteEndpoint("nonexistent");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("NOT_FOUND");
		}
	});

	it("records and lists deliveries", async () => {
		await store.recordDelivery({
			id: "del_1",
			webhookId: "wh_1",
			eventType: "sync.push",
			payload: { event: "sync.push" },
			status: "success",
			statusCode: 200,
			attemptCount: 1,
			createdAt: new Date(),
		});
		await store.recordDelivery({
			id: "del_2",
			webhookId: "wh_1",
			eventType: "flush.complete",
			payload: { event: "flush.complete" },
			status: "failed",
			error: "timeout",
			attemptCount: 1,
			createdAt: new Date(),
		});

		const result = await store.listDeliveries("wh_1");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(2);
		}
	});
});

describe("Webhook Signature", () => {
	it("signs and verifies a payload", async () => {
		const payload = JSON.stringify({ event: "sync.push", data: { count: 5 } });
		const secret = "my-webhook-secret";

		const signature = await signPayload(payload, secret);
		expect(signature).toMatch(/^[0-9a-f]+$/);

		const valid = await verifyPayloadSignature(payload, signature, secret);
		expect(valid).toBe(true);
	});

	it("rejects tampered payload", async () => {
		const payload = JSON.stringify({ event: "sync.push", data: { count: 5 } });
		const secret = "my-webhook-secret";

		const signature = await signPayload(payload, secret);

		const tampered = JSON.stringify({ event: "sync.push", data: { count: 999 } });
		const valid = await verifyPayloadSignature(tampered, signature, secret);
		expect(valid).toBe(false);
	});

	it("rejects wrong secret", async () => {
		const payload = JSON.stringify({ event: "sync.push" });
		const signature = await signPayload(payload, "secret-a");

		const valid = await verifyPayloadSignature(payload, signature, "secret-b");
		expect(valid).toBe(false);
	});
});

describe("WebhookDispatcher", () => {
	let store: MemoryWebhookStore;

	beforeEach(() => {
		store = new MemoryWebhookStore();
	});

	afterEach(() => {
		store.clear();
	});

	it("dispatches to matching endpoints", async () => {
		const fetchMock = vi.fn().mockResolvedValue(new Response("ok", { status: 200 }));

		await store.createEndpoint({
			orgId: "org_abc",
			url: "https://example.com/wh",
			events: ["sync.push"],
			secret: "s",
		});

		const dispatcher = new WebhookDispatcher(store, {
			fetchFn: fetchMock as unknown as typeof fetch,
		});

		const result = await dispatcher.dispatch("org_abc", "sync.push", { count: 5 });
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBe(1);
		}

		// Wait for async delivery
		await new Promise((r) => setTimeout(r, 50));
		expect(fetchMock).toHaveBeenCalledOnce();
	});

	it("does not dispatch to non-matching events", async () => {
		const fetchMock = vi.fn().mockResolvedValue(new Response("ok", { status: 200 }));

		await store.createEndpoint({
			orgId: "org_abc",
			url: "https://example.com/wh",
			events: ["flush.complete"],
			secret: "s",
		});

		const dispatcher = new WebhookDispatcher(store, {
			fetchFn: fetchMock as unknown as typeof fetch,
		});

		const result = await dispatcher.dispatch("org_abc", "sync.push", { count: 5 });
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBe(0);
		}
	});

	it("includes signature header in delivery", async () => {
		const fetchMock = vi.fn().mockResolvedValue(new Response("ok", { status: 200 }));

		const created = await store.createEndpoint({
			orgId: "org_abc",
			url: "https://example.com/wh",
			events: ["sync.push"],
			secret: "test-secret",
		});
		expect(created.ok).toBe(true);
		if (!created.ok) return;

		const dispatcher = new WebhookDispatcher(store, {
			fetchFn: fetchMock as unknown as typeof fetch,
			maxRetries: 1,
		});

		await dispatcher.deliverWithRetry(created.value, {
			event: "sync.push",
			timestamp: "2025-01-01T00:00:00Z",
			data: { count: 5 },
		});

		expect(fetchMock).toHaveBeenCalledOnce();
		const callArgs = fetchMock.mock.calls[0];
		const headers = callArgs?.[1]?.headers as Record<string, string>;
		expect(headers["X-LakeSync-Signature"]).toBeTruthy();
		expect(headers["X-LakeSync-Event"]).toBe("sync.push");
	});

	it("retries on failure", async () => {
		const fetchMock = vi
			.fn()
			.mockResolvedValueOnce(new Response("error", { status: 500 }))
			.mockResolvedValueOnce(new Response("error", { status: 500 }))
			.mockResolvedValueOnce(new Response("ok", { status: 200 }));

		const created = await store.createEndpoint({
			orgId: "org_abc",
			url: "https://example.com/wh",
			events: ["sync.push"],
			secret: "s",
		});
		expect(created.ok).toBe(true);
		if (!created.ok) return;

		const dispatcher = new WebhookDispatcher(store, {
			fetchFn: fetchMock as unknown as typeof fetch,
			maxRetries: 3,
		});

		const delivery = await dispatcher.deliverWithRetry(created.value, {
			event: "sync.push",
			timestamp: "2025-01-01T00:00:00Z",
			data: {},
		});

		expect(delivery.status).toBe("success");
		expect(fetchMock).toHaveBeenCalledTimes(3);

		// Verify deliveries were recorded
		const deliveries = await store.listDeliveries(created.value.id);
		expect(deliveries.ok).toBe(true);
		if (deliveries.ok) {
			expect(deliveries.value).toHaveLength(3);
		}
	});

	it("records failed delivery after all retries exhausted", async () => {
		const fetchMock = vi.fn().mockResolvedValue(new Response("error", { status: 500 }));

		const created = await store.createEndpoint({
			orgId: "org_abc",
			url: "https://example.com/wh",
			events: ["sync.push"],
			secret: "s",
		});
		expect(created.ok).toBe(true);
		if (!created.ok) return;

		const dispatcher = new WebhookDispatcher(store, {
			fetchFn: fetchMock as unknown as typeof fetch,
			maxRetries: 2,
		});

		const delivery = await dispatcher.deliverWithRetry(created.value, {
			event: "sync.push",
			timestamp: "2025-01-01T00:00:00Z",
			data: {},
		});

		expect(delivery.status).toBe("failed");
		expect(fetchMock).toHaveBeenCalledTimes(2);
	});

	it("handles network errors gracefully", async () => {
		const fetchMock = vi.fn().mockRejectedValue(new Error("network error"));

		const created = await store.createEndpoint({
			orgId: "org_abc",
			url: "https://example.com/wh",
			events: ["sync.push"],
			secret: "s",
		});
		expect(created.ok).toBe(true);
		if (!created.ok) return;

		const dispatcher = new WebhookDispatcher(store, {
			fetchFn: fetchMock as unknown as typeof fetch,
			maxRetries: 1,
		});

		const delivery = await dispatcher.deliverWithRetry(created.value, {
			event: "sync.push",
			timestamp: "2025-01-01T00:00:00Z",
			data: {},
		});

		expect(delivery.status).toBe("failed");
		expect(delivery.error).toBe("network error");
	});
});

describe("WEBHOOK_EVENT_TYPES", () => {
	it("contains all expected event types", () => {
		expect(WEBHOOK_EVENT_TYPES).toContain("sync.push");
		expect(WEBHOOK_EVENT_TYPES).toContain("sync.pull");
		expect(WEBHOOK_EVENT_TYPES).toContain("flush.complete");
		expect(WEBHOOK_EVENT_TYPES).toContain("flush.error");
		expect(WEBHOOK_EVENT_TYPES).toContain("connector.error");
		expect(WEBHOOK_EVENT_TYPES).toContain("schema.change");
		expect(WEBHOOK_EVENT_TYPES).toContain("gateway.status");
		expect(WEBHOOK_EVENT_TYPES).toHaveLength(7);
	});
});

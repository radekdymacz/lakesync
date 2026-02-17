import { Ok, type Result } from "@lakesync/core";
import { ControlPlaneError } from "../errors";
import type {
	DeliveryStatus,
	WebhookDelivery,
	WebhookEndpoint,
	WebhookEventType,
	WebhookPayload,
} from "./types";
import type { WebhookStore } from "./webhook-store";

/**
 * Minimal Web Crypto typing for HMAC operations.
 * Avoids Uint8Array<ArrayBufferLike> vs Uint8Array<ArrayBuffer> issues in TS 5.7+.
 */
interface HmacSubtle {
	importKey(
		format: "raw",
		keyData: Uint8Array,
		algorithm: { name: string; hash: string },
		extractable: boolean,
		usages: string[],
	): Promise<unknown>;
	sign(algorithm: string, key: unknown, data: Uint8Array): Promise<ArrayBuffer>;
}

function generateId(): string {
	return crypto.randomUUID().replace(/-/g, "").slice(0, 21);
}

/** Configuration for the webhook dispatcher */
export interface WebhookDispatcherConfig {
	/** Maximum delivery attempts per event (default: 3) */
	readonly maxRetries?: number;
	/** Custom fetch function (for testing) */
	readonly fetchFn?: typeof fetch;
}

/**
 * Dispatches webhook events to registered endpoints.
 *
 * Handles HMAC-SHA256 signing, delivery, and retry with exponential backoff.
 */
export class WebhookDispatcher {
	private readonly store: WebhookStore;
	private readonly maxRetries: number;
	private readonly fetchFn: typeof fetch;

	constructor(store: WebhookStore, config?: WebhookDispatcherConfig) {
		this.store = store;
		this.maxRetries = config?.maxRetries ?? 3;
		this.fetchFn = config?.fetchFn ?? globalThis.fetch;
	}

	/**
	 * Dispatch an event to all matching webhook endpoints for an org.
	 *
	 * Returns the number of endpoints the event was dispatched to.
	 */
	async dispatch(
		orgId: string,
		eventType: WebhookEventType,
		data: Record<string, unknown>,
	): Promise<Result<number, ControlPlaneError>> {
		const listResult = await this.store.listByOrg(orgId);
		if (!listResult.ok) return listResult;

		const endpoints = listResult.value.filter((e) => e.events.includes(eventType));

		const payload: WebhookPayload = {
			event: eventType,
			timestamp: new Date().toISOString(),
			data,
		};

		let dispatched = 0;
		for (const endpoint of endpoints) {
			// Fire-and-forget: deliver async, do not block
			this.deliverWithRetry(endpoint, payload).catch(() => {
				// Swallowed — delivery failures are logged via the store
			});
			dispatched++;
		}

		return Ok(dispatched);
	}

	/**
	 * Deliver a payload to an endpoint with exponential backoff retry.
	 */
	async deliverWithRetry(
		endpoint: WebhookEndpoint,
		payload: WebhookPayload,
	): Promise<WebhookDelivery> {
		const body = JSON.stringify(payload);
		let lastDelivery: WebhookDelivery | undefined;

		for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
			const delivery = await this.attemptDelivery(endpoint, body, attempt);
			lastDelivery = delivery;

			await this.store.recordDelivery(delivery);

			if (delivery.status === "success") {
				return delivery;
			}

			// Exponential backoff: 1s, 2s, 4s
			if (attempt < this.maxRetries) {
				await sleep(1000 * 2 ** (attempt - 1));
			}
		}

		return lastDelivery!;
	}

	/**
	 * Make a single delivery attempt.
	 */
	private async attemptDelivery(
		endpoint: WebhookEndpoint,
		body: string,
		attemptCount: number,
	): Promise<WebhookDelivery> {
		const signature = await signPayload(body, endpoint.secret);

		try {
			const response = await this.fetchFn(endpoint.url, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					"X-LakeSync-Signature": signature,
					"X-LakeSync-Event": JSON.parse(body).event as string,
				},
				body,
			});

			const isSuccess = response.status >= 200 && response.status < 300;
			return {
				id: generateId(),
				webhookId: endpoint.id,
				eventType: JSON.parse(body).event as WebhookEventType,
				payload: JSON.parse(body) as Record<string, unknown>,
				status: isSuccess ? ("success" as DeliveryStatus) : ("failed" as DeliveryStatus),
				statusCode: response.status,
				attemptCount,
				createdAt: new Date(),
			};
		} catch (error) {
			return {
				id: generateId(),
				webhookId: endpoint.id,
				eventType: JSON.parse(body).event as WebhookEventType,
				payload: JSON.parse(body) as Record<string, unknown>,
				status: "failed" as DeliveryStatus,
				error: error instanceof Error ? error.message : String(error),
				attemptCount,
				createdAt: new Date(),
			};
		}
	}
}

/**
 * Sign a webhook payload with HMAC-SHA256.
 *
 * Returns the signature as a hex string.
 */
export async function signPayload(payload: string, secret: string): Promise<string> {
	const encoder = new TextEncoder();
	const subtle = crypto.subtle as unknown as HmacSubtle;

	const key = await subtle.importKey(
		"raw",
		encoder.encode(secret),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);

	const signatureBuffer = await subtle.sign("HMAC", key, encoder.encode(payload));
	const bytes = new Uint8Array(signatureBuffer);
	return Array.from(bytes)
		.map((b) => b.toString(16).padStart(2, "0"))
		.join("");
}

/**
 * Verify a webhook signature.
 *
 * Use this on the receiving end to validate that a webhook came from LakeSync.
 */
export async function verifyPayloadSignature(
	payload: string,
	signature: string,
	secret: string,
): Promise<boolean> {
	const expected = await signPayload(payload, secret);
	return expected === signature;
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

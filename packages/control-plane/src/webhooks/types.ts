/** Supported webhook event types */
export type WebhookEventType =
	| "sync.push"
	| "sync.pull"
	| "flush.complete"
	| "flush.error"
	| "connector.error"
	| "schema.change"
	| "gateway.status";

/** All supported event types */
export const WEBHOOK_EVENT_TYPES: readonly WebhookEventType[] = [
	"sync.push",
	"sync.pull",
	"flush.complete",
	"flush.error",
	"connector.error",
	"schema.change",
	"gateway.status",
] as const;

/** Delivery status */
export type DeliveryStatus = "pending" | "success" | "failed";

/** A registered webhook endpoint */
export interface WebhookEndpoint {
	readonly id: string;
	readonly orgId: string;
	readonly url: string;
	readonly events: readonly WebhookEventType[];
	readonly secret: string;
	readonly createdAt: Date;
}

/** Input for registering a webhook */
export interface CreateWebhookInput {
	readonly orgId: string;
	readonly url: string;
	readonly events: readonly WebhookEventType[];
	readonly secret: string;
}

/** A webhook delivery attempt */
export interface WebhookDelivery {
	readonly id: string;
	readonly webhookId: string;
	readonly eventType: WebhookEventType;
	readonly payload: Record<string, unknown>;
	readonly status: DeliveryStatus;
	readonly statusCode?: number;
	readonly error?: string;
	readonly attemptCount: number;
	readonly createdAt: Date;
}

/** A webhook event payload */
export interface WebhookPayload {
	readonly event: WebhookEventType;
	readonly timestamp: string;
	readonly data: Record<string, unknown>;
}

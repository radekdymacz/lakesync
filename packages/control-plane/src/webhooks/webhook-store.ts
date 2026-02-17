import { Err, Ok, type Result } from "@lakesync/core";
import { ControlPlaneError } from "../errors";
import type { CreateWebhookInput, WebhookDelivery, WebhookEndpoint } from "./types";

function generateId(): string {
	return crypto.randomUUID().replace(/-/g, "").slice(0, 21);
}

/** Repository interface for webhook persistence */
export interface WebhookStore {
	createEndpoint(input: CreateWebhookInput): Promise<Result<WebhookEndpoint, ControlPlaneError>>;
	getEndpoint(id: string): Promise<Result<WebhookEndpoint | null, ControlPlaneError>>;
	listByOrg(orgId: string): Promise<Result<WebhookEndpoint[], ControlPlaneError>>;
	deleteEndpoint(id: string): Promise<Result<void, ControlPlaneError>>;
	recordDelivery(delivery: WebhookDelivery): Promise<Result<void, ControlPlaneError>>;
	listDeliveries(webhookId: string, limit?: number): Promise<Result<WebhookDelivery[], ControlPlaneError>>;
}

/** In-memory webhook store for development and testing */
export class MemoryWebhookStore implements WebhookStore {
	private endpoints = new Map<string, WebhookEndpoint>();
	private deliveries = new Map<string, WebhookDelivery[]>();

	async createEndpoint(
		input: CreateWebhookInput,
	): Promise<Result<WebhookEndpoint, ControlPlaneError>> {
		const endpoint: WebhookEndpoint = {
			id: generateId(),
			orgId: input.orgId,
			url: input.url,
			events: [...input.events],
			secret: input.secret,
			createdAt: new Date(),
		};
		this.endpoints.set(endpoint.id, endpoint);
		return Ok(endpoint);
	}

	async getEndpoint(id: string): Promise<Result<WebhookEndpoint | null, ControlPlaneError>> {
		return Ok(this.endpoints.get(id) ?? null);
	}

	async listByOrg(orgId: string): Promise<Result<WebhookEndpoint[], ControlPlaneError>> {
		const result = Array.from(this.endpoints.values()).filter((e) => e.orgId === orgId);
		return Ok(result);
	}

	async deleteEndpoint(id: string): Promise<Result<void, ControlPlaneError>> {
		if (!this.endpoints.has(id)) {
			return Err(new ControlPlaneError(`Webhook "${id}" not found`, "NOT_FOUND"));
		}
		this.endpoints.delete(id);
		return Ok(undefined);
	}

	async recordDelivery(delivery: WebhookDelivery): Promise<Result<void, ControlPlaneError>> {
		const list = this.deliveries.get(delivery.webhookId) ?? [];
		list.push(delivery);
		this.deliveries.set(delivery.webhookId, list);
		return Ok(undefined);
	}

	async listDeliveries(
		webhookId: string,
		limit = 50,
	): Promise<Result<WebhookDelivery[], ControlPlaneError>> {
		const list = this.deliveries.get(webhookId) ?? [];
		return Ok(list.slice(-limit));
	}

	/** Clear all data (for testing) */
	clear(): void {
		this.endpoints.clear();
		this.deliveries.clear();
	}
}

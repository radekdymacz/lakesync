import type {
	ApiKey,
	BillingInfo,
	CreateApiKeyInput,
	CreateGatewayInput,
	Gateway,
	Organisation,
	OrgMember,
	PlanId,
	UpdateGatewayInput,
} from "@lakesync/control-plane";

export interface ApiClientConfig {
	baseUrl: string;
	getToken: () => Promise<string | null>;
}

export class ApiClient {
	private readonly baseUrl: string;
	private readonly getToken: () => Promise<string | null>;

	constructor(config: ApiClientConfig) {
		this.baseUrl = config.baseUrl;
		this.getToken = config.getToken;
	}

	private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
		const token = await this.getToken();
		const headers: Record<string, string> = {
			"Content-Type": "application/json",
			...(options.headers as Record<string, string>),
		};
		if (token) {
			headers.Authorization = `Bearer ${token}`;
		}

		const response = await fetch(`${this.baseUrl}${path}`, {
			...options,
			headers,
		});

		if (!response.ok) {
			const body = await response.text();
			throw new Error(`API request failed: ${response.status} ${response.statusText} — ${body}`);
		}

		return response.json() as Promise<T>;
	}

	// Organisation
	async getOrg(orgId: string): Promise<Organisation> {
		return this.request(`/v1/orgs/${orgId}`);
	}

	async getMembers(orgId: string): Promise<OrgMember[]> {
		return this.request(`/v1/orgs/${orgId}/members`);
	}

	// Gateways
	async listGateways(orgId: string): Promise<Gateway[]> {
		return this.request(`/v1/orgs/${orgId}/gateways`);
	}

	async getGateway(gatewayId: string): Promise<Gateway> {
		return this.request(`/v1/gateways/${gatewayId}`);
	}

	async createGateway(input: CreateGatewayInput): Promise<Gateway> {
		return this.request("/v1/gateways", {
			method: "POST",
			body: JSON.stringify(input),
		});
	}

	async updateGateway(gatewayId: string, input: UpdateGatewayInput): Promise<Gateway> {
		return this.request(`/v1/gateways/${gatewayId}`, {
			method: "PATCH",
			body: JSON.stringify(input),
		});
	}

	async deleteGateway(gatewayId: string): Promise<void> {
		await this.request(`/v1/gateways/${gatewayId}`, {
			method: "DELETE",
		});
	}

	// API Keys
	async listApiKeys(orgId: string): Promise<ApiKey[]> {
		return this.request(`/v1/orgs/${orgId}/api-keys`);
	}

	async createApiKey(input: CreateApiKeyInput): Promise<ApiKey & { rawKey: string }> {
		return this.request("/v1/api-keys", {
			method: "POST",
			body: JSON.stringify(input),
		});
	}

	async revokeApiKey(keyId: string): Promise<void> {
		await this.request(`/v1/api-keys/${keyId}`, {
			method: "DELETE",
		});
	}

	async rotateApiKey(
		keyId: string,
		input: CreateApiKeyInput,
	): Promise<ApiKey & { rawKey: string }> {
		return this.request(`/v1/api-keys/${keyId}/rotate`, {
			method: "POST",
			body: JSON.stringify(input),
		});
	}

	// Usage
	async getUsage(orgId: string): Promise<{ deltas: number; storageBytes: number }> {
		return this.request(`/v1/orgs/${orgId}/usage`);
	}

	async getUsageTimeSeries(
		orgId: string,
		from: string,
		to: string,
	): Promise<UsageTimeSeriesResponse> {
		return this.request(
			`/v1/orgs/${orgId}/usage/timeseries?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
		);
	}

	// Billing
	async getBillingInfo(orgId: string): Promise<BillingInfo> {
		return this.request(`/v1/orgs/${orgId}/billing`);
	}

	async createCheckoutSession(orgId: string, planId: PlanId): Promise<{ url: string }> {
		return this.request(`/v1/orgs/${orgId}/billing/checkout`, {
			method: "POST",
			body: JSON.stringify({ planId }),
		});
	}

	async createPortalSession(orgId: string): Promise<{ url: string }> {
		return this.request(`/v1/orgs/${orgId}/billing/portal`, {
			method: "POST",
		});
	}
}

/** Time-series usage data point */
export interface UsageDataPoint {
	date: string;
	pushDeltas: number;
	pullDeltas: number;
	apiCalls: number;
	storageBytes: number;
}

/** Response from the usage time-series endpoint */
export interface UsageTimeSeriesResponse {
	data: UsageDataPoint[];
	totals: {
		pushDeltas: number;
		pullDeltas: number;
		apiCalls: number;
		storageBytes: number;
	};
}

/**
 * BackendProvider abstracts the dev-store / production API split.
 *
 * In dev mode (Clerk not configured), calls go to the in-memory devStore.
 * In production, calls go to the remote control-plane API with a Clerk JWT.
 */

import type { BillingData, UsageData } from "@/types/api";

import { serverAuth } from "./auth-server";
import { type DevGateway, devStore } from "./dev-store";

const API_BASE_URL = process.env.CONTROL_PLANE_URL ?? "http://localhost:8787";

export interface BackendProvider {
	gateways: {
		list(orgId: string): Promise<DevGateway[]>;
		get(id: string): Promise<DevGateway | undefined>;
		create(input: { orgId: string; name: string; region?: string }): Promise<DevGateway>;
		update(
			id: string,
			patch: Partial<Pick<DevGateway, "name" | "status">>,
		): Promise<DevGateway | undefined>;
		delete(id: string): Promise<boolean>;
	};
	usage: {
		get(orgId: string, from: string, to: string): Promise<UsageData>;
	};
	billing: {
		get(orgId: string): Promise<BillingData>;
		checkout(orgId: string, planId: string): Promise<{ url?: string; message?: string }>;
		portal(orgId: string): Promise<{ url?: string; message?: string }>;
	};
}

// ---------------------------------------------------------------------------
// Helpers for the production backend
// ---------------------------------------------------------------------------

async function authedFetch(path: string, init?: RequestInit): Promise<Response> {
	const { getToken } = await serverAuth();
	const token = await getToken();
	const headers: Record<string, string> = {
		...(init?.headers as Record<string, string>),
	};
	if (token) headers.Authorization = `Bearer ${token}`;
	return fetch(`${API_BASE_URL}${path}`, { ...init, headers });
}

async function authedJson<T>(path: string, init?: RequestInit): Promise<T> {
	const res = await authedFetch(path, init);
	return res.json() as Promise<T>;
}

// ---------------------------------------------------------------------------
// Dev backend — wraps devStore calls in promises
// ---------------------------------------------------------------------------

const devBackend: BackendProvider = {
	gateways: {
		async list(orgId) {
			return devStore.listGateways(orgId);
		},
		async get(id) {
			return devStore.getGateway(id);
		},
		async create(input) {
			return devStore.createGateway(input);
		},
		async update(id, patch) {
			return devStore.updateGateway(id, patch);
		},
		async delete(id) {
			return devStore.deleteGateway(id);
		},
	},
	usage: {
		async get(orgId, from, to) {
			return devStore.getUsage(orgId, from, to);
		},
	},
	billing: {
		async get(orgId) {
			return devStore.getBilling(orgId) as BillingData;
		},
		async checkout(_orgId, _planId) {
			return { message: "Billing checkout is not available in dev mode." };
		},
		async portal(_orgId) {
			return { message: "Billing portal is not available in dev mode." };
		},
	},
};

// ---------------------------------------------------------------------------
// Production backend — proxies to the control-plane API
// ---------------------------------------------------------------------------

const prodBackend: BackendProvider = {
	gateways: {
		list(orgId) {
			return authedJson(`/v1/orgs/${orgId}/gateways`);
		},
		get(id) {
			return authedJson(`/v1/gateways/${id}`);
		},
		async create(input) {
			return authedJson("/v1/gateways", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(input),
			});
		},
		async update(id, patch) {
			return authedJson(`/v1/gateways/${id}`, {
				method: "PATCH",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(patch),
			});
		},
		async delete(id) {
			const res = await authedFetch(`/v1/gateways/${id}`, {
				method: "DELETE",
			});
			return res.status === 204 || res.ok;
		},
	},
	usage: {
		async get(orgId, from, to) {
			return authedJson<UsageData>(
				`/v1/orgs/${orgId}/usage/timeseries?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
			);
		},
	},
	billing: {
		get(orgId) {
			return authedJson<BillingData>(`/v1/orgs/${orgId}/billing`);
		},
		async checkout(orgId, planId) {
			return authedJson(`/v1/orgs/${orgId}/billing/checkout`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ planId }),
			});
		},
		async portal(orgId) {
			return authedJson(`/v1/orgs/${orgId}/billing/portal`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({}),
			});
		},
	},
};

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createBackend(): BackendProvider {
	const useRemote = !!process.env.CONTROL_PLANE_URL;
	return useRemote ? prodBackend : devBackend;
}

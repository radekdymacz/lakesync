"use client";

import type { Gateway } from "@lakesync/control-plane";
import { useApiQuery } from "./use-query";

export function useGateways() {
	const query = useApiQuery<Gateway[]>("/api/gateways");

	async function createGateway(name: string, region: string): Promise<void> {
		const res = await fetch("/api/gateways", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ name, region }),
		});
		if (!res.ok) {
			const body = await res.text();
			throw new Error(body || res.statusText);
		}
		query.refetch();
	}

	return { ...query, createGateway };
}

export function useGateway(id: string | null | undefined) {
	return useApiQuery<Gateway>(id ? `/api/gateways/${id}` : null);
}

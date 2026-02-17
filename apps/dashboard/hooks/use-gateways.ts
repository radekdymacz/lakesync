"use client";

import type { Gateway } from "@lakesync/control-plane";
import { useApiQuery } from "./use-query";

export function useGateways(orgId: string | null | undefined) {
	return useApiQuery<Gateway[]>(orgId ? `/api/gateways?orgId=${orgId}` : null);
}

export function useGateway(id: string | null | undefined) {
	return useApiQuery<Gateway>(id ? `/api/gateways/${id}` : null);
}

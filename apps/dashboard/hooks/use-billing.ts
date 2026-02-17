"use client";

import type { BillingData } from "@/types/api";
import { useApiQuery } from "./use-query";

export function useBilling(orgId: string | null | undefined) {
	return useApiQuery<BillingData>(orgId ? `/api/billing?orgId=${orgId}` : null);
}

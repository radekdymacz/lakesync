"use client";

import type { BillingData } from "@/types/api";
import { useApiQuery } from "./use-query";

export function useBilling() {
	return useApiQuery<BillingData>("/api/billing");
}

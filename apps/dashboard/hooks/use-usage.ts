"use client";

import { useMemo } from "react";
import type { UsageData } from "@/types/api";
import { useApiQuery } from "./use-query";

export function useUsage(orgId: string | null | undefined, from: string, to: string) {
	const url = useMemo(
		() =>
			orgId
				? `/api/usage?orgId=${orgId}&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`
				: null,
		[orgId, from, to],
	);
	return useApiQuery<UsageData>(url);
}

"use client";

import { useMemo } from "react";
import type { UsageData } from "@/types/api";
import { useApiQuery } from "./use-query";

export function useUsage(from: string, to: string) {
	const url = useMemo(
		() => `/api/usage?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
		[from, to],
	);
	return useApiQuery<UsageData>(url);
}

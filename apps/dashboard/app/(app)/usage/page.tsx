"use client";

import { useMemo } from "react";
import { UsageBar } from "@/components/usage-bar";
import { UsageChart } from "@/components/usage-chart";
import { useBilling } from "@/hooks/use-billing";
import { useUsage } from "@/hooks/use-usage";
import { formatBytes, formatDate } from "@/lib/format";
import { useAuth } from "@/lib/use-auth";

export default function UsagePage() {
	const { orgId } = useAuth();

	const { from, to } = useMemo(() => {
		const now = new Date();
		return {
			from: new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString(),
			to: now.toISOString(),
		};
	}, []);

	const { data: usage, loading: usageLoading } = useUsage(orgId, from, to);
	const { data: billing, loading: billingLoading } = useBilling(orgId);
	const loading = usageLoading || billingLoading;

	const limits = billing
		? {
				maxDeltasPerMonth: billing.maxDeltasPerMonth ?? -1,
				maxStorageBytes: billing.maxStorageBytes ?? -1,
			}
		: null;

	const totalDeltas = usage ? usage.totals.pushDeltas + usage.totals.pullDeltas : 0;

	return (
		<div>
			<h1 className="text-2xl font-bold">Usage</h1>
			<p className="mt-1 text-sm text-gray-600">
				Monitor your sync usage and quotas for the last 30 days.
			</p>

			{loading ? (
				<div className="mt-8 text-center text-gray-500">Loading...</div>
			) : (
				<>
					{limits && (
						<div className="mt-8 space-y-4 rounded-lg border border-gray-200 bg-white p-6">
							<h2 className="text-sm font-semibold text-gray-700">Current Period Usage</h2>
							<UsageBar
								label="Delta Operations"
								current={totalDeltas}
								limit={limits.maxDeltasPerMonth}
							/>
							<UsageBar
								label="Storage"
								current={usage?.totals.storageBytes ?? 0}
								limit={limits.maxStorageBytes}
								formatter={formatBytes}
							/>
						</div>
					)}

					<div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-2">
						<UsageChart
							title="Push Deltas (daily)"
							colour="bg-blue-500"
							data={
								usage?.data.map((d) => ({
									label: formatDate(d.date),
									value: d.pushDeltas,
								})) ?? []
							}
						/>
						<UsageChart
							title="Pull Deltas (daily)"
							colour="bg-indigo-500"
							data={
								usage?.data.map((d) => ({
									label: formatDate(d.date),
									value: d.pullDeltas,
								})) ?? []
							}
						/>
						<UsageChart
							title="API Calls (daily)"
							colour="bg-emerald-500"
							data={
								usage?.data.map((d) => ({
									label: formatDate(d.date),
									value: d.apiCalls,
								})) ?? []
							}
						/>
						<UsageChart
							title="Storage (daily)"
							colour="bg-amber-500"
							data={
								usage?.data.map((d) => ({
									label: formatDate(d.date),
									value: d.storageBytes,
								})) ?? []
							}
						/>
					</div>

					{usage && (
						<div className="mt-6 grid grid-cols-2 gap-4 lg:grid-cols-4">
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">Push Deltas</p>
								<p className="mt-1 text-xl font-semibold">
									{usage.totals.pushDeltas.toLocaleString()}
								</p>
							</div>
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">Pull Deltas</p>
								<p className="mt-1 text-xl font-semibold">
									{usage.totals.pullDeltas.toLocaleString()}
								</p>
							</div>
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">API Calls</p>
								<p className="mt-1 text-xl font-semibold">
									{usage.totals.apiCalls.toLocaleString()}
								</p>
							</div>
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">Storage Used</p>
								<p className="mt-1 text-xl font-semibold">
									{formatBytes(usage.totals.storageBytes)}
								</p>
							</div>
						</div>
					)}
				</>
			)}
		</div>
	);
}

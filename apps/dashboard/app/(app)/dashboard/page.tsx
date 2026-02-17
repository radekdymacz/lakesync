"use client";

import { useBilling } from "@/hooks/use-billing";
import { useGateways } from "@/hooks/use-gateways";
import { formatBytes } from "@/lib/format";
import { useAuth } from "@/lib/use-auth";

export default function DashboardPage() {
	const { orgId } = useAuth();
	const { data: gateways } = useGateways(orgId);
	const { data: billing } = useBilling(orgId);

	const activeGateways = gateways ? gateways.filter((g) => g.status === "active").length : null;
	const deltasThisMonth = billing?.usage?.deltasThisPeriod ?? null;
	const storageUsed = billing?.usage?.storageBytes ?? null;

	return (
		<div>
			<h1 className="text-2xl font-bold">Overview</h1>
			<p className="mt-2 text-gray-600">Welcome to your LakeSync dashboard.</p>

			<div className="mt-8 grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3">
				<div className="rounded-lg border border-gray-200 bg-white p-6">
					<h3 className="text-sm font-medium text-gray-500">Active Gateways</h3>
					<p className="mt-2 text-3xl font-semibold">{activeGateways ?? "--"}</p>
				</div>
				<div className="rounded-lg border border-gray-200 bg-white p-6">
					<h3 className="text-sm font-medium text-gray-500">Deltas This Month</h3>
					<p className="mt-2 text-3xl font-semibold">
						{deltasThisMonth != null ? deltasThisMonth.toLocaleString() : "--"}
					</p>
				</div>
				<div className="rounded-lg border border-gray-200 bg-white p-6">
					<h3 className="text-sm font-medium text-gray-500">Storage Used</h3>
					<p className="mt-2 text-3xl font-semibold">
						{storageUsed != null ? formatBytes(storageUsed) : "--"}
					</p>
				</div>
			</div>
		</div>
	);
}

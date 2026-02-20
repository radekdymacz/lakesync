"use client";

import type { PlanId } from "@lakesync/control-plane";
import { useState } from "react";
import { useBilling } from "@/hooks/use-billing";
import { formatCurrency, formatTimestamp } from "@/lib/format";

const PLAN_TIER: Record<string, number> = { free: 0, starter: 1, pro: 2, enterprise: 3 };

const PLAN_OPTIONS: Array<{
	id: PlanId;
	name: string;
	price: string;
	features: string[];
}> = [
	{
		id: "free",
		name: "Free",
		price: "$0/mo",
		features: ["1 gateway", "10,000 deltas/mo", "100 MB storage"],
	},
	{
		id: "starter",
		name: "Starter",
		price: "$29/mo",
		features: ["3 gateways", "100,000 deltas/mo", "1 GB storage"],
	},
	{
		id: "pro",
		name: "Pro",
		price: "$99/mo",
		features: ["10 gateways", "1,000,000 deltas/mo", "10 GB storage"],
	},
	{
		id: "enterprise",
		name: "Enterprise",
		price: "Custom",
		features: ["Unlimited gateways", "Unlimited deltas", "Unlimited storage"],
	},
];

export function BillingPage() {
	const billingResult = useBilling();
	const billing = billingResult.status === "success" ? billingResult.data : null;
	const loading = billingResult.status === "loading";
	const [upgrading, setUpgrading] = useState<PlanId | null>(null);
	const [portalLoading, setPortalLoading] = useState(false);

	async function handleUpgrade(planId: PlanId) {
		setUpgrading(planId);
		try {
			const res = await fetch("/api/billing/checkout", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ planId }),
			});
			if (res.ok) {
				const data = await res.json();
				if (data.url) {
					window.location.href = data.url;
				}
			}
		} finally {
			setUpgrading(null);
		}
	}

	async function handleManageBilling() {
		setPortalLoading(true);
		try {
			const res = await fetch("/api/billing/portal", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
			});
			if (res.ok) {
				const data = await res.json();
				if (data.url) {
					window.location.href = data.url;
				}
			}
		} finally {
			setPortalLoading(false);
		}
	}

	if (loading) {
		return <div className="py-4 text-center text-sm text-gray-500">Loading billing info...</div>;
	}

	if (!billing) {
		return (
			<div className="py-4">
				<p className="text-sm text-gray-500">
					Billing information is not available. Ensure the control plane API is configured.
				</p>
			</div>
		);
	}

	return (
		<div className="space-y-6 py-2">
			<div className="rounded-lg border border-gray-200 bg-white p-6">
				<div className="flex items-center justify-between">
					<div>
						<p className="text-sm text-gray-500">Current Plan</p>
						<p className="mt-1 text-2xl font-semibold">{billing.planName}</p>
						{billing.price > 0 && (
							<p className="text-sm text-gray-500">{formatCurrency(billing.price)}/month</p>
						)}
					</div>
					{billing.plan !== "free" && (
						<button
							type="button"
							onClick={handleManageBilling}
							disabled={portalLoading}
							className="rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50"
						>
							{portalLoading ? "Opening..." : "Manage Billing"}
						</button>
					)}
				</div>
				{billing.cancelAtPeriodEnd && billing.currentPeriodEnd && (
					<div className="mt-3 rounded-md bg-amber-50 px-3 py-2 text-sm text-amber-800">
						Your subscription will cancel on {formatTimestamp(billing.currentPeriodEnd)}.
					</div>
				)}
				{billing.currentPeriodEnd && !billing.cancelAtPeriodEnd && (
					<p className="mt-2 text-xs text-gray-400">
						Next billing date: {formatTimestamp(billing.currentPeriodEnd)}
					</p>
				)}
			</div>

			<div>
				<h3 className="mb-3 text-sm font-medium text-gray-700">Available Plans</h3>
				<div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
					{PLAN_OPTIONS.map((plan) => {
						const isCurrent = billing.plan === plan.id;
						return (
							<div
								key={plan.id}
								className={`rounded-lg border p-4 ${
									isCurrent ? "border-gray-900 bg-gray-50" : "border-gray-200 bg-white"
								}`}
							>
								<p className="font-semibold">{plan.name}</p>
								<p className="mt-1 text-lg font-bold">{plan.price}</p>
								<ul className="mt-3 space-y-1 text-xs text-gray-600">
									{plan.features.map((f) => (
										<li key={f}>{f}</li>
									))}
								</ul>
								{isCurrent ? (
									<div className="mt-4 rounded-md bg-gray-200 px-3 py-1.5 text-center text-xs font-medium text-gray-600">
										Current Plan
									</div>
								) : plan.id === "enterprise" ? (
									<a
										href="mailto:hello@lakesync.dev"
										className="mt-4 block rounded-md border border-gray-300 px-3 py-1.5 text-center text-xs font-medium text-gray-700 hover:bg-gray-50"
									>
										Contact Sales
									</a>
								) : (
									<button
										type="button"
										onClick={() => handleUpgrade(plan.id)}
										disabled={upgrading === plan.id}
										className="mt-4 w-full rounded-md bg-gray-900 px-3 py-1.5 text-xs font-medium text-white hover:bg-gray-800 disabled:opacity-50"
									>
										{upgrading === plan.id
											? "Redirecting..."
											: billing.plan === "free"
												? "Upgrade"
												: (PLAN_TIER[plan.id] ?? 0) > (PLAN_TIER[billing.plan] ?? 0)
													? "Upgrade"
													: "Switch"}
									</button>
								)}
							</div>
						);
					})}
				</div>
			</div>
		</div>
	);
}

export function WebhooksPage() {
	return (
		<div className="py-4">
			<p className="text-sm text-gray-500">
				Configure webhook endpoints for event notifications. Coming soon.
			</p>
		</div>
	);
}

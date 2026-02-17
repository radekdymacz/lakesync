"use client";

import type { Gateway } from "@lakesync/control-plane";
import { useParams, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { ConnectionInstructions } from "@/components/connection-instructions";
import { StatusBadge } from "@/components/status-badge";

type Tab = "overview" | "schema" | "sync-rules" | "connectors" | "metrics" | "settings";

const TABS: Array<{ id: Tab; label: string }> = [
	{ id: "overview", label: "Overview" },
	{ id: "schema", label: "Schema" },
	{ id: "sync-rules", label: "Sync Rules" },
	{ id: "connectors", label: "Connectors" },
	{ id: "metrics", label: "Metrics" },
	{ id: "settings", label: "Settings" },
];

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8787";

export default function GatewayDetailPage() {
	const params = useParams();
	const router = useRouter();
	const gatewayId = params.id as string;

	const [gateway, setGateway] = useState<Gateway | null>(null);
	const [loading, setLoading] = useState(true);
	const [activeTab, setActiveTab] = useState<Tab>("overview");
	const [editName, setEditName] = useState("");
	const [saving, setSaving] = useState(false);

	useEffect(() => {
		async function load() {
			try {
				const res = await fetch(`/api/gateways/${gatewayId}`);
				if (res.ok) {
					const gw: Gateway = await res.json();
					setGateway(gw);
					setEditName(gw.name);
				}
			} finally {
				setLoading(false);
			}
		}
		load();
	}, [gatewayId]);

	async function handleSuspend() {
		const res = await fetch(`/api/gateways/${gatewayId}`, {
			method: "PATCH",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ status: "suspended" }),
		});
		if (res.ok) setGateway(await res.json());
	}

	async function handleActivate() {
		const res = await fetch(`/api/gateways/${gatewayId}`, {
			method: "PATCH",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ status: "active" }),
		});
		if (res.ok) setGateway(await res.json());
	}

	async function handleRename() {
		setSaving(true);
		try {
			const res = await fetch(`/api/gateways/${gatewayId}`, {
				method: "PATCH",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ name: editName }),
			});
			if (res.ok) setGateway(await res.json());
		} finally {
			setSaving(false);
		}
	}

	async function handleDelete() {
		if (!confirm("Are you sure you want to delete this gateway? This cannot be undone.")) {
			return;
		}
		const res = await fetch(`/api/gateways/${gatewayId}`, { method: "DELETE" });
		if (res.ok) router.push("/gateways");
	}

	if (loading) {
		return <div className="text-center text-gray-500">Loading...</div>;
	}

	if (!gateway) {
		return <div className="text-center text-gray-500">Gateway not found.</div>;
	}

	return (
		<div>
			<div className="flex items-center justify-between">
				<div className="flex items-center gap-3">
					<h1 className="text-2xl font-bold">{gateway.name}</h1>
					<StatusBadge status={gateway.status} />
				</div>
			</div>

			<div className="mt-6 border-b border-gray-200">
				<nav className="-mb-px flex space-x-8">
					{TABS.map((tab) => (
						<button
							key={tab.id}
							type="button"
							onClick={() => setActiveTab(tab.id)}
							className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium ${
								activeTab === tab.id
									? "border-gray-900 text-gray-900"
									: "border-transparent text-gray-500 hover:border-gray-300 hover:text-gray-700"
							}`}
						>
							{tab.label}
						</button>
					))}
				</nav>
			</div>

			<div className="mt-6">
				{activeTab === "overview" && (
					<div className="space-y-6">
						<div className="grid grid-cols-1 gap-6 sm:grid-cols-3">
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">Gateway ID</p>
								<p className="mt-1 text-sm font-mono">{gateway.id}</p>
							</div>
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">Region</p>
								<p className="mt-1 text-sm">{gateway.region ?? "Default"}</p>
							</div>
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">Created</p>
								<p className="mt-1 text-sm">{new Date(gateway.createdAt).toLocaleString()}</p>
							</div>
						</div>

						<ConnectionInstructions gatewayId={gateway.id} gatewayUrl={API_BASE_URL} />
					</div>
				)}

				{activeTab === "schema" && (
					<div className="rounded-lg border border-gray-200 bg-white p-6">
						<h3 className="text-sm font-medium text-gray-700">Table Schemas</h3>
						<p className="mt-2 text-sm text-gray-500">
							Schemas are automatically managed by the gateway as clients push data. This view will
							show the current table schemas once data has been synced.
						</p>
						<div className="mt-4 flex h-32 items-center justify-center text-sm text-gray-400">
							No schemas registered yet.
						</div>
					</div>
				)}

				{activeTab === "sync-rules" && (
					<div className="rounded-lg border border-gray-200 bg-white p-6">
						<h3 className="text-sm font-medium text-gray-700">Sync Rules</h3>
						<p className="mt-2 text-sm text-gray-500">
							Sync rules control which data each client receives. Rules use a declarative
							bucket-based filtering model.
						</p>
						<div className="mt-4 flex h-32 items-center justify-center text-sm text-gray-400">
							No sync rules configured.
						</div>
					</div>
				)}

				{activeTab === "connectors" && (
					<div className="rounded-lg border border-gray-200 bg-white p-6">
						<h3 className="text-sm font-medium text-gray-700">Connectors</h3>
						<p className="mt-2 text-sm text-gray-500">
							Connectors ingest data from external systems (databases, APIs) into this gateway.
						</p>
						<div className="mt-4 flex h-32 items-center justify-center text-sm text-gray-400">
							No connectors registered.
						</div>
					</div>
				)}

				{activeTab === "metrics" && (
					<div className="space-y-6">
						<div className="grid grid-cols-1 gap-6 sm:grid-cols-3">
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">Buffer Size</p>
								<p className="mt-1 text-2xl font-semibold">--</p>
							</div>
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">Push Count</p>
								<p className="mt-1 text-2xl font-semibold">--</p>
							</div>
							<div className="rounded-lg border border-gray-200 bg-white p-4">
								<p className="text-xs font-medium text-gray-500">Pull Count</p>
								<p className="mt-1 text-2xl font-semibold">--</p>
							</div>
						</div>
					</div>
				)}

				{activeTab === "settings" && (
					<div className="space-y-6">
						<div className="rounded-lg border border-gray-200 bg-white p-6">
							<h3 className="text-sm font-medium text-gray-700">Rename Gateway</h3>
							<div className="mt-3 flex items-center gap-3">
								<input
									type="text"
									value={editName}
									onChange={(e) => setEditName(e.target.value)}
									className="block w-64 rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-gray-500 focus:outline-none focus:ring-1 focus:ring-gray-500"
								/>
								<button
									type="button"
									onClick={handleRename}
									disabled={saving || editName === gateway.name}
									className="rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800 disabled:opacity-50"
								>
									{saving ? "Saving..." : "Save"}
								</button>
							</div>
						</div>

						<div className="rounded-lg border border-gray-200 bg-white p-6">
							<h3 className="text-sm font-medium text-gray-700">Gateway Status</h3>
							<p className="mt-1 text-sm text-gray-500">
								Suspending a gateway prevents all sync operations.
							</p>
							<div className="mt-3">
								{gateway.status === "active" ? (
									<button
										type="button"
										onClick={handleSuspend}
										className="rounded-md border border-yellow-300 bg-yellow-50 px-4 py-2 text-sm font-medium text-yellow-800 hover:bg-yellow-100"
									>
										Suspend Gateway
									</button>
								) : gateway.status === "suspended" ? (
									<button
										type="button"
										onClick={handleActivate}
										className="rounded-md border border-green-300 bg-green-50 px-4 py-2 text-sm font-medium text-green-800 hover:bg-green-100"
									>
										Reactivate Gateway
									</button>
								) : null}
							</div>
						</div>

						<div className="rounded-lg border border-red-200 bg-white p-6">
							<h3 className="text-sm font-medium text-red-700">Danger Zone</h3>
							<p className="mt-1 text-sm text-gray-500">
								Permanently delete this gateway and all its data.
							</p>
							<div className="mt-3">
								<button
									type="button"
									onClick={handleDelete}
									className="rounded-md bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700"
								>
									Delete Gateway
								</button>
							</div>
						</div>
					</div>
				)}
			</div>
		</div>
	);
}

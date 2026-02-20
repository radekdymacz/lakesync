"use client";

import Link from "next/link";
import { useState } from "react";
import { CreateGatewayModal } from "@/components/create-gateway-modal";
import { StatusBadge } from "@/components/status-badge";
import { useGateways } from "@/hooks/use-gateways";

export default function GatewaysPage() {
	const gatewaysQuery = useGateways();
	const [modalOpen, setModalOpen] = useState(false);

	return (
		<div>
			<div className="flex items-center justify-between">
				<div>
					<h1 className="text-2xl font-bold">Gateways</h1>
					<p className="mt-1 text-sm text-gray-600">Manage your sync gateways.</p>
				</div>
				<button
					type="button"
					onClick={() => setModalOpen(true)}
					className="rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800"
				>
					Create Gateway
				</button>
			</div>

			{gatewaysQuery.status === "loading" ? (
				<div className="mt-8 text-center text-gray-500">Loading...</div>
			) : gatewaysQuery.status === "success" && gatewaysQuery.data.length === 0 ? (
				<div className="mt-8 rounded-lg border border-gray-200 bg-white p-12 text-center text-gray-500">
					No gateways yet. Create one to get started.
				</div>
			) : gatewaysQuery.status === "success" ? (
				<div className="mt-6 overflow-hidden rounded-lg border border-gray-200 bg-white">
					<table className="min-w-full divide-y divide-gray-200">
						<thead className="bg-gray-50">
							<tr>
								<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
									Name
								</th>
								<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
									Status
								</th>
								<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
									Region
								</th>
								<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
									Created
								</th>
							</tr>
						</thead>
						<tbody className="divide-y divide-gray-200">
							{gatewaysQuery.data.map((gw) => (
								<tr key={gw.id} className="hover:bg-gray-50">
									<td className="px-6 py-4">
										<Link
											href={`/gateways/${gw.id}`}
											className="text-sm font-medium text-gray-900 hover:underline"
										>
											{gw.name}
										</Link>
									</td>
									<td className="px-6 py-4">
										<StatusBadge status={gw.status} />
									</td>
									<td className="px-6 py-4 text-sm text-gray-500">{gw.region ?? "\u2014"}</td>
									<td className="px-6 py-4 text-sm text-gray-500">
										{new Date(gw.createdAt).toLocaleDateString()}
									</td>
								</tr>
							))}
						</tbody>
					</table>
				</div>
			) : null}

			<CreateGatewayModal
				open={modalOpen}
				onClose={() => setModalOpen(false)}
				onSubmit={gatewaysQuery.createGateway}
			/>
		</div>
	);
}

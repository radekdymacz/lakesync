"use client";

import { useState } from "react";

interface CreateGatewayModalProps {
	orgId: string;
	open: boolean;
	onClose: () => void;
	onCreated: () => void;
}

const REGIONS = [
	{ value: "us-east-1", label: "US East (Virginia)" },
	{ value: "us-west-2", label: "US West (Oregon)" },
	{ value: "eu-west-1", label: "EU West (Ireland)" },
	{ value: "ap-southeast-1", label: "Asia Pacific (Singapore)" },
];

export function CreateGatewayModal({ orgId, open, onClose, onCreated }: CreateGatewayModalProps) {
	const [name, setName] = useState("");
	const [region, setRegion] = useState("us-east-1");
	const [submitting, setSubmitting] = useState(false);
	const [error, setError] = useState<string | null>(null);

	if (!open) return null;

	async function handleSubmit(e: React.FormEvent) {
		e.preventDefault();
		setSubmitting(true);
		setError(null);

		try {
			const response = await fetch("/api/gateways", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ orgId, name, region }),
			});

			if (!response.ok) {
				const body = await response.text();
				throw new Error(body || response.statusText);
			}

			setName("");
			setRegion("us-east-1");
			onCreated();
			onClose();
		} catch (err) {
			setError(err instanceof Error ? err.message : "Failed to create gateway");
		} finally {
			setSubmitting(false);
		}
	}

	return (
		<div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
			<div className="w-full max-w-md rounded-lg bg-white p-6 shadow-lg">
				<h2 className="text-lg font-semibold">Create Gateway</h2>
				<p className="mt-1 text-sm text-gray-500">
					Set up a new sync gateway for your application.
				</p>

				<form onSubmit={handleSubmit} className="mt-4 space-y-4">
					<div>
						<label htmlFor="gw-name" className="block text-sm font-medium text-gray-700">
							Name
						</label>
						<input
							id="gw-name"
							type="text"
							required
							value={name}
							onChange={(e) => setName(e.target.value)}
							placeholder="my-app-gateway"
							className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-gray-500 focus:outline-none focus:ring-1 focus:ring-gray-500"
						/>
					</div>

					<div>
						<label htmlFor="gw-region" className="block text-sm font-medium text-gray-700">
							Region
						</label>
						<select
							id="gw-region"
							value={region}
							onChange={(e) => setRegion(e.target.value)}
							className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-gray-500 focus:outline-none focus:ring-1 focus:ring-gray-500"
						>
							{REGIONS.map((r) => (
								<option key={r.value} value={r.value}>
									{r.label}
								</option>
							))}
						</select>
					</div>

					{error && <p className="text-sm text-red-600">{error}</p>}

					<div className="flex justify-end gap-3 pt-2">
						<button
							type="button"
							onClick={onClose}
							className="rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
						>
							Cancel
						</button>
						<button
							type="submit"
							disabled={submitting || !name.trim()}
							className="rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800 disabled:opacity-50"
						>
							{submitting ? "Creating..." : "Create"}
						</button>
					</div>
				</form>
			</div>
		</div>
	);
}

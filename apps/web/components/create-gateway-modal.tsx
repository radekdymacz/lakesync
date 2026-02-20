"use client";

import { useState } from "react";

interface CreateGatewayModalProps {
	open: boolean;
	onClose: () => void;
	onSubmit: (name: string, region: string) => Promise<void>;
}

const INITIAL_FORM = { name: "", region: "us-east-1" } as const;

const REGIONS = [
	{ value: "us-east-1", label: "US East (Virginia)" },
	{ value: "us-west-2", label: "US West (Oregon)" },
	{ value: "eu-west-1", label: "EU West (Ireland)" },
	{ value: "ap-southeast-1", label: "Asia Pacific (Singapore)" },
];

type FormState =
	| { mode: "editing"; name: string; region: string }
	| { mode: "submitting"; name: string; region: string }
	| { mode: "error"; name: string; region: string; error: string };

export function CreateGatewayModal({ open, onClose, onSubmit }: CreateGatewayModalProps) {
	const [form, setForm] = useState<FormState>({
		mode: "editing",
		name: INITIAL_FORM.name,
		region: INITIAL_FORM.region,
	});

	if (!open) return null;

	const submitting = form.mode === "submitting";

	async function handleSubmit(e: React.FormEvent) {
		e.preventDefault();
		if (form.mode === "submitting") return;
		const { name, region } = form;
		setForm({ mode: "submitting", name, region });
		try {
			await onSubmit(name, region);
			setForm({ mode: "editing", name: INITIAL_FORM.name, region: INITIAL_FORM.region });
			onClose();
		} catch (err) {
			setForm({
				mode: "error",
				name,
				region,
				error: err instanceof Error ? err.message : "Failed to create gateway",
			});
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
							value={form.name}
							onChange={(e) =>
								setForm((s) => ({ mode: "editing", name: e.target.value, region: s.region }))
							}
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
							value={form.region}
							onChange={(e) =>
								setForm((s) => ({ mode: "editing", name: s.name, region: e.target.value }))
							}
							className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-gray-500 focus:outline-none focus:ring-1 focus:ring-gray-500"
						>
							{REGIONS.map((r) => (
								<option key={r.value} value={r.value}>
									{r.label}
								</option>
							))}
						</select>
					</div>

					{form.mode === "error" && <p className="text-sm text-red-600">{form.error}</p>}

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
							disabled={submitting || !form.name.trim()}
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

"use client";

import type { ApiKeyRole } from "@lakesync/control-plane";
import { useCallback, useState } from "react";

interface CreateKeyDialogProps {
	readonly open: boolean;
	readonly onClose: () => void;
	readonly onSubmit: (input: {
		name: string;
		role: ApiKeyRole;
		gatewayId?: string;
	}) => Promise<{ rawKey: string } | undefined>;
}

export function CreateKeyDialog({ open, onClose, onSubmit }: CreateKeyDialogProps) {
	const [name, setName] = useState("");
	const [role, setRole] = useState<ApiKeyRole>("client");
	const [gatewayId, setGatewayId] = useState("");
	const [submitting, setSubmitting] = useState(false);
	const [rawKey, setRawKey] = useState<string | null>(null);
	const [copied, setCopied] = useState(false);
	const [error, setError] = useState<string | null>(null);

	const handleSubmit = useCallback(
		async (e: React.FormEvent) => {
			e.preventDefault();
			setSubmitting(true);
			setError(null);

			try {
				const result = await onSubmit({
					name,
					role,
					gatewayId: gatewayId || undefined,
				});
				if (result) {
					setRawKey(result.rawKey);
				}
			} catch (err) {
				setError(err instanceof Error ? err.message : "Failed to create key");
			} finally {
				setSubmitting(false);
			}
		},
		[name, role, gatewayId, onSubmit],
	);

	const handleCopy = useCallback(async () => {
		if (!rawKey) return;
		await navigator.clipboard.writeText(rawKey);
		setCopied(true);
		setTimeout(() => setCopied(false), 2000);
	}, [rawKey]);

	const handleClose = useCallback(() => {
		setName("");
		setRole("client");
		setGatewayId("");
		setRawKey(null);
		setCopied(false);
		setError(null);
		onClose();
	}, [onClose]);

	if (!open) return null;

	return (
		<div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
			<div className="w-full max-w-md rounded-lg bg-white p-6 shadow-xl">
				{rawKey ? (
					<div>
						<h2 className="text-lg font-semibold">API Key Created</h2>
						<p className="mt-2 text-sm text-gray-600">
							Copy this key now. You will not be able to see it again.
						</p>

						<div className="mt-4 flex items-center gap-2">
							<code className="flex-1 overflow-x-auto rounded-md bg-gray-100 p-3 font-mono text-sm">
								{rawKey}
							</code>
							<button
								type="button"
								onClick={handleCopy}
								className="shrink-0 rounded-md border border-gray-300 px-3 py-2 text-sm font-medium hover:bg-gray-50"
							>
								{copied ? "Copied" : "Copy"}
							</button>
						</div>

						<div className="mt-6 flex justify-end">
							<button
								type="button"
								onClick={handleClose}
								className="rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800"
							>
								Done
							</button>
						</div>
					</div>
				) : (
					<form onSubmit={handleSubmit}>
						<h2 className="text-lg font-semibold">Create API Key</h2>
						<p className="mt-1 text-sm text-gray-600">Create a new key for programmatic access.</p>

						{error && (
							<div className="mt-4 rounded-md bg-red-50 p-3 text-sm text-red-800">{error}</div>
						)}

						<div className="mt-4 space-y-4">
							<div>
								<label htmlFor="key-name" className="block text-sm font-medium text-gray-700">
									Name
								</label>
								<input
									id="key-name"
									type="text"
									required
									value={name}
									onChange={(e) => setName(e.target.value)}
									placeholder="e.g. Production backend"
									className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-gray-500 focus:outline-none focus:ring-1 focus:ring-gray-500"
								/>
							</div>

							<div>
								<label htmlFor="key-role" className="block text-sm font-medium text-gray-700">
									Role
								</label>
								<select
									id="key-role"
									value={role}
									onChange={(e) => setRole(e.target.value as ApiKeyRole)}
									className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-gray-500 focus:outline-none focus:ring-1 focus:ring-gray-500"
								>
									<option value="client">Client (sync only)</option>
									<option value="admin">Admin (full access)</option>
								</select>
							</div>

							<div>
								<label htmlFor="key-gateway" className="block text-sm font-medium text-gray-700">
									Gateway Scope (optional)
								</label>
								<input
									id="key-gateway"
									type="text"
									value={gatewayId}
									onChange={(e) => setGatewayId(e.target.value)}
									placeholder="Leave empty for org-wide access"
									className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-gray-500 focus:outline-none focus:ring-1 focus:ring-gray-500"
								/>
								<p className="mt-1 text-xs text-gray-500">
									Restrict this key to a specific gateway ID.
								</p>
							</div>
						</div>

						<div className="mt-6 flex justify-end gap-3">
							<button
								type="button"
								onClick={handleClose}
								className="rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
							>
								Cancel
							</button>
							<button
								type="submit"
								disabled={submitting || !name}
								className="rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800 disabled:opacity-50"
							>
								{submitting ? "Creating..." : "Create Key"}
							</button>
						</div>
					</form>
				)}
			</div>
		</div>
	);
}

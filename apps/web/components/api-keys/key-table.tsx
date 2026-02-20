"use client";

import type { ApiKey } from "@lakesync/control-plane";

function formatDate(date: Date | string | undefined): string {
	if (!date) return "--";
	const d = typeof date === "string" ? new Date(date) : date;
	return d.toLocaleDateString("en-GB", {
		day: "numeric",
		month: "short",
		year: "numeric",
	});
}

function formatRelative(date: Date | string | undefined): string {
	if (!date) return "Never";
	const d = typeof date === "string" ? new Date(date) : date;
	const now = Date.now();
	const diff = now - d.getTime();
	const minutes = Math.floor(diff / 60_000);
	if (minutes < 1) return "Just now";
	if (minutes < 60) return `${minutes}m ago`;
	const hours = Math.floor(minutes / 60);
	if (hours < 24) return `${hours}h ago`;
	const days = Math.floor(hours / 24);
	if (days < 30) return `${days}d ago`;
	return formatDate(date);
}

interface KeyTableProps {
	readonly keys: ReadonlyArray<ApiKey>;
	readonly onRevoke: (key: ApiKey) => void;
	readonly onRotate: (key: ApiKey) => void;
}

export function KeyTable({ keys, onRevoke, onRotate }: KeyTableProps) {
	if (keys.length === 0) {
		return (
			<div className="mt-8 rounded-lg border border-gray-200 bg-white p-12 text-center text-sm text-gray-500">
				No API keys yet. Create one to get started.
			</div>
		);
	}

	return (
		<div className="mt-8 overflow-hidden rounded-lg border border-gray-200 bg-white">
			<table className="min-w-full divide-y divide-gray-200">
				<thead className="bg-gray-50">
					<tr>
						<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
							Name
						</th>
						<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
							Key
						</th>
						<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
							Role
						</th>
						<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
							Scope
						</th>
						<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
							Last Used
						</th>
						<th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
							Created
						</th>
						<th className="px-6 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
							Actions
						</th>
					</tr>
				</thead>
				<tbody className="divide-y divide-gray-200">
					{keys.map((key) => (
						<tr key={key.id}>
							<td className="whitespace-nowrap px-6 py-4 text-sm font-medium text-gray-900">
								{key.name}
							</td>
							<td className="whitespace-nowrap px-6 py-4 font-mono text-sm text-gray-500">
								{key.keyPrefix}...
							</td>
							<td className="whitespace-nowrap px-6 py-4 text-sm">
								<span
									className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${
										key.role === "admin"
											? "bg-purple-100 text-purple-800"
											: "bg-blue-100 text-blue-800"
									}`}
								>
									{key.role}
								</span>
							</td>
							<td className="whitespace-nowrap px-6 py-4 text-sm text-gray-500">
								{key.gatewayId ? (
									<span className="font-mono text-xs" title={key.gatewayId}>
										{key.gatewayId.slice(0, 8)}...
									</span>
								) : (
									"Org-wide"
								)}
							</td>
							<td className="whitespace-nowrap px-6 py-4 text-sm text-gray-500">
								{formatRelative(key.lastUsedAt)}
							</td>
							<td className="whitespace-nowrap px-6 py-4 text-sm text-gray-500">
								{formatDate(key.createdAt)}
							</td>
							<td className="whitespace-nowrap px-6 py-4 text-right text-sm">
								<button
									type="button"
									onClick={() => onRotate(key)}
									className="mr-3 text-gray-600 hover:text-gray-900"
								>
									Rotate
								</button>
								<button
									type="button"
									onClick={() => onRevoke(key)}
									className="text-red-600 hover:text-red-800"
								>
									Revoke
								</button>
							</td>
						</tr>
					))}
				</tbody>
			</table>
		</div>
	);
}

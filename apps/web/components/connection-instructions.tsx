"use client";

import { useState } from "react";

interface ConnectionInstructionsProps {
	gatewayId: string;
	gatewayUrl: string;
}

export function ConnectionInstructions({ gatewayId, gatewayUrl }: ConnectionInstructionsProps) {
	const [copied, setCopied] = useState(false);

	const snippet = `import { createSyncCoordinator } from "lakesync";
import { HttpTransport } from "lakesync/client";

const transport = new HttpTransport({
  url: "${gatewayUrl}/sync/${gatewayId}",
  getToken: async () => {
    // Exchange your session token for a LakeSync JWT
    // See: https://lakesync.dev/docs/auth
    return "<your-jwt-token>";
  },
});

const coordinator = createSyncCoordinator({
  transport,
  tables: [
    { table: "todos", columns: [
      { name: "id", type: "TEXT" },
      { name: "title", type: "TEXT" },
      { name: "completed", type: "INTEGER" },
    ]},
  ],
});

await coordinator.start();`;

	async function copyToClipboard() {
		await navigator.clipboard.writeText(snippet);
		setCopied(true);
		setTimeout(() => setCopied(false), 2000);
	}

	return (
		<div className="rounded-lg border border-gray-200 bg-white">
			<div className="flex items-center justify-between border-b border-gray-200 px-4 py-3">
				<h3 className="text-sm font-medium">Connect your client</h3>
				<button
					type="button"
					onClick={copyToClipboard}
					className="rounded-md border border-gray-300 px-3 py-1 text-xs font-medium text-gray-600 hover:bg-gray-50"
				>
					{copied ? "Copied" : "Copy"}
				</button>
			</div>
			<pre className="overflow-x-auto p-4 text-xs leading-relaxed text-gray-800">
				<code>{snippet}</code>
			</pre>
		</div>
	);
}

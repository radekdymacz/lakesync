"use client";

import type { ApiKey, ApiKeyRole } from "@lakesync/control-plane";
import { useCallback, useReducer } from "react";
import { ConfirmDialog } from "@/components/api-keys/confirm-dialog";
import { CreateKeyDialog } from "@/components/api-keys/create-key-dialog";
import { KeyTable } from "@/components/api-keys/key-table";
import { useAuth } from "@/lib/use-auth";

type ApiKeysState =
	| { mode: "idle"; keys: ApiKey[] }
	| { mode: "creating"; keys: ApiKey[] }
	| { mode: "confirmingRevoke"; keys: ApiKey[]; target: ApiKey }
	| { mode: "confirmingRotate"; keys: ApiKey[]; target: ApiKey }
	| { mode: "showingRotatedKey"; keys: ApiKey[]; rawKey: string; copied: boolean };

type ApiKeysAction =
	| { type: "OPEN_CREATE" }
	| { type: "CLOSE_CREATE" }
	| { type: "KEY_CREATED"; key: ApiKey }
	| { type: "OPEN_REVOKE"; target: ApiKey }
	| { type: "CLOSE_REVOKE" }
	| { type: "KEY_REVOKED" }
	| { type: "OPEN_ROTATE"; target: ApiKey }
	| { type: "CLOSE_ROTATE" }
	| { type: "KEY_ROTATED"; rotated: ApiKey; rawKey: string }
	| { type: "COPY_ROTATED_KEY" }
	| { type: "RESET_COPIED" }
	| { type: "DISMISS_ROTATED_KEY" };

function apiKeysReducer(state: ApiKeysState, action: ApiKeysAction): ApiKeysState {
	switch (action.type) {
		case "OPEN_CREATE":
			return { mode: "creating", keys: state.keys };
		case "CLOSE_CREATE":
			return { mode: "idle", keys: state.keys };
		case "KEY_CREATED":
			return { mode: "idle", keys: [...state.keys, action.key] };
		case "OPEN_REVOKE":
			return { mode: "confirmingRevoke", keys: state.keys, target: action.target };
		case "CLOSE_REVOKE":
			return { mode: "idle", keys: state.keys };
		case "KEY_REVOKED": {
			if (state.mode !== "confirmingRevoke") return state;
			return { mode: "idle", keys: state.keys.filter((k) => k.id !== state.target.id) };
		}
		case "OPEN_ROTATE":
			return { mode: "confirmingRotate", keys: state.keys, target: action.target };
		case "CLOSE_ROTATE":
			return { mode: "idle", keys: state.keys };
		case "KEY_ROTATED": {
			const keys =
				state.mode === "confirmingRotate"
					? state.keys.map((k) => (k.id === state.target.id ? action.rotated : k))
					: state.keys;
			return { mode: "showingRotatedKey", keys, rawKey: action.rawKey, copied: false };
		}
		case "COPY_ROTATED_KEY":
			if (state.mode !== "showingRotatedKey") return state;
			return { ...state, copied: true };
		case "RESET_COPIED":
			if (state.mode !== "showingRotatedKey") return state;
			return { ...state, copied: false };
		case "DISMISS_ROTATED_KEY":
			return { mode: "idle", keys: state.keys };
		default:
			return state;
	}
}

function RotatedKeyModal({
	rawKey,
	copied,
	onCopy,
	onDismiss,
}: {
	rawKey: string;
	copied: boolean;
	onCopy: () => void;
	onDismiss: () => void;
}) {
	return (
		<div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
			<div className="w-full max-w-md rounded-lg bg-white p-6 shadow-xl">
				<h2 className="text-lg font-semibold">Key Rotated</h2>
				<p className="mt-2 text-sm text-gray-600">
					Copy the new key now. You will not be able to see it again.
				</p>
				<div className="mt-4 flex items-center gap-2">
					<code className="flex-1 overflow-x-auto rounded-md bg-gray-100 p-3 font-mono text-sm">
						{rawKey}
					</code>
					<button
						type="button"
						onClick={onCopy}
						className="shrink-0 rounded-md border border-gray-300 px-3 py-2 text-sm font-medium hover:bg-gray-50"
					>
						{copied ? "Copied" : "Copy"}
					</button>
				</div>
				<div className="mt-6 flex justify-end">
					<button
						type="button"
						onClick={onDismiss}
						className="rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800"
					>
						Done
					</button>
				</div>
			</div>
		</div>
	);
}

export default function ApiKeysPage() {
	const { orgId } = useAuth();
	const ORG_ID = orgId ?? "dev-org-1";
	const [state, dispatch] = useReducer(apiKeysReducer, { mode: "idle", keys: [] });

	const handleCreate = useCallback(
		async (input: { name: string; role: ApiKeyRole; gatewayId?: string }) => {
			const mockKey: ApiKey = {
				id: crypto.randomUUID().replace(/-/g, "").slice(0, 21),
				orgId: ORG_ID,
				name: input.name,
				role: input.role,
				gatewayId: input.gatewayId,
				keyHash: "sha256:mock",
				keyPrefix: "lk_live_",
				createdAt: new Date(),
			};
			const rawKey = `lk_live_${crypto.randomUUID().replace(/-/g, "")}`;
			dispatch({ type: "KEY_CREATED", key: mockKey });
			return { rawKey };
		},
		[ORG_ID],
	);

	const handleRevoke = useCallback(async () => {
		dispatch({ type: "KEY_REVOKED" });
	}, []);

	const handleRotate = useCallback(async () => {
		if (state.mode !== "confirmingRotate") return;
		const newId = crypto.randomUUID().replace(/-/g, "").slice(0, 21);
		const rawKey = `lk_live_${crypto.randomUUID().replace(/-/g, "")}`;
		const rotated: ApiKey = {
			...state.target,
			id: newId,
			keyPrefix: "lk_live_",
			createdAt: new Date(),
			lastUsedAt: undefined,
		};
		dispatch({ type: "KEY_ROTATED", rotated, rawKey });
	}, [state]);

	const handleCopyRotateKey = useCallback(async () => {
		if (state.mode !== "showingRotatedKey") return;
		await navigator.clipboard.writeText(state.rawKey);
		dispatch({ type: "COPY_ROTATED_KEY" });
		setTimeout(() => dispatch({ type: "RESET_COPIED" }), 2000);
	}, [state]);

	const revokeTarget = state.mode === "confirmingRevoke" ? state.target : null;
	const rotateTarget = state.mode === "confirmingRotate" ? state.target : null;

	return (
		<div>
			<div className="flex items-center justify-between">
				<div>
					<h1 className="text-2xl font-bold">API Keys</h1>
					<p className="mt-1 text-sm text-gray-600">
						Manage API keys for programmatic access to your gateways.
					</p>
				</div>
				<button
					type="button"
					onClick={() => dispatch({ type: "OPEN_CREATE" })}
					className="rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800"
				>
					Create Key
				</button>
			</div>

			<KeyTable
				keys={state.keys}
				onRevoke={(key) => dispatch({ type: "OPEN_REVOKE", target: key })}
				onRotate={(key) => dispatch({ type: "OPEN_ROTATE", target: key })}
			/>

			<CreateKeyDialog
				open={state.mode === "creating"}
				onClose={() => dispatch({ type: "CLOSE_CREATE" })}
				onSubmit={handleCreate}
			/>

			<ConfirmDialog
				open={revokeTarget !== null}
				title="Revoke API Key"
				message={`Are you sure you want to revoke "${revokeTarget?.name}"? This action cannot be undone. Any applications using this key will lose access immediately.`}
				confirmLabel="Revoke Key"
				variant="danger"
				onClose={() => dispatch({ type: "CLOSE_REVOKE" })}
				onConfirm={handleRevoke}
			/>

			<ConfirmDialog
				open={rotateTarget !== null}
				title="Rotate API Key"
				message={`This will revoke "${rotateTarget?.name}" and create a new replacement key. Applications using the old key will need to be updated.`}
				confirmLabel="Rotate Key"
				variant="default"
				onClose={() => dispatch({ type: "CLOSE_ROTATE" })}
				onConfirm={handleRotate}
			/>

			{state.mode === "showingRotatedKey" && (
				<RotatedKeyModal
					rawKey={state.rawKey}
					copied={state.copied}
					onCopy={handleCopyRotateKey}
					onDismiss={() => dispatch({ type: "DISMISS_ROTATED_KEY" })}
				/>
			)}
		</div>
	);
}

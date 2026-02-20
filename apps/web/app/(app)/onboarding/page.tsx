"use client";

import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { CLERK_ENABLED } from "@/lib/auth-config";
import { useCreateOrg } from "@/lib/identity-widgets";

function ClerkOnboarding() {
	const { create, creating, error } = useCreateOrg();
	const router = useRouter();
	const [name, setName] = useState("");

	async function handleSubmit(e: React.FormEvent) {
		e.preventDefault();
		const ok = await create(name);
		if (ok) router.push("/dashboard");
	}

	return (
		<div className="flex min-h-[60vh] items-center justify-center">
			<div className="w-full max-w-md space-y-6 rounded-lg border border-gray-200 bg-white p-8">
				<div>
					<h1 className="text-xl font-semibold text-gray-900">Welcome to LakeSync</h1>
					<p className="mt-2 text-sm text-gray-500">
						Create an organisation to get started. Organisations group your gateways, API keys, and
						team members.
					</p>
				</div>

				<form onSubmit={handleSubmit} className="space-y-4">
					<div>
						<label htmlFor="org-name" className="block text-sm font-medium text-gray-700">
							Organisation name
						</label>
						<input
							id="org-name"
							type="text"
							value={name}
							onChange={(e) => setName(e.target.value)}
							placeholder="e.g. My Company"
							className="mt-1 w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
							// biome-ignore lint/a11y/noAutofocus: onboarding form is primary content
							autoFocus
						/>
					</div>

					{error && <p className="text-sm text-red-600">{error}</p>}

					<button
						type="submit"
						disabled={creating || !name.trim()}
						className="w-full rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800 disabled:opacity-50"
					>
						{creating ? "Creating..." : "Create Organisation"}
					</button>
				</form>
			</div>
		</div>
	);
}

function DevRedirect() {
	const router = useRouter();
	useEffect(() => {
		router.push("/dashboard");
	}, [router]);
	return null;
}

export default function OnboardingPage() {
	if (!CLERK_ENABLED) return <DevRedirect />;
	return <ClerkOnboarding />;
}

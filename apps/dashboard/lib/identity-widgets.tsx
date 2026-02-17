"use client";

import { CLERK_ENABLED } from "./auth-config";

function DevOrgSwitcher() {
	return (
		<div className="rounded-md bg-gray-100 px-3 py-2 text-xs text-gray-600">
			Dev Mode (dev-org-1)
		</div>
	);
}

function DevUserButton() {
	return <div className="rounded-md bg-gray-100 px-3 py-2 text-xs text-gray-600">dev-user-1</div>;
}

function DevOrgProfile() {
	return (
		<div className="rounded-lg border border-gray-200 bg-white p-6">
			<p className="text-sm text-gray-500">
				Organisation management is available when Clerk is configured.
			</p>
		</div>
	);
}

/** Returns identity widgets backed by Clerk (prod) or dev stubs. */
export function getIdentityWidgets() {
	if (!CLERK_ENABLED) {
		return {
			OrgSwitcher: DevOrgSwitcher,
			UserButton: DevUserButton,
			OrgProfile: DevOrgProfile,
		};
	}

	// eslint-disable-next-line @typescript-eslint/no-require-imports
	const clerk = require("@clerk/nextjs") as typeof import("@clerk/nextjs");

	return {
		OrgSwitcher: () => (
			<clerk.OrganizationSwitcher
				afterSelectOrganizationUrl="/dashboard"
				appearance={{ elements: { rootBox: "w-full" } }}
			/>
		),
		UserButton: () => (
			<clerk.UserButton
				afterSignOutUrl="/sign-in"
				appearance={{ elements: { rootBox: "w-full" } }}
			/>
		),
		OrgProfile: () => <clerk.OrganizationProfile />,
	};
}

"use client";

import { UserButton as ClerkUserButtonComponent, OrganizationSwitcher } from "@clerk/nextjs";
import { CLERK_ENABLED } from "./auth-config";
import { BillingPage, WebhooksPage } from "./settings-pages";

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

function ClerkOrgSwitcher() {
	return (
		<OrganizationSwitcher
			hidePersonal
			appearance={{
				elements: {
					rootBox: "w-full",
					organizationSwitcherTrigger:
						"w-full justify-between rounded-md bg-gray-100 px-3 py-2 text-sm text-gray-700 hover:bg-gray-200",
				},
			}}
		/>
	);
}

const BillingIcon = (
	<svg
		className="h-4 w-4"
		fill="none"
		viewBox="0 0 24 24"
		stroke="currentColor"
		strokeWidth={2}
		role="img"
		aria-label="Billing"
	>
		<path
			strokeLinecap="round"
			strokeLinejoin="round"
			d="M3 10h18M7 15h1m4 0h1m-7 4h12a3 3 0 003-3V8a3 3 0 00-3-3H6a3 3 0 00-3 3v8a3 3 0 003 3z"
		/>
	</svg>
);

const WebhooksIcon = (
	<svg
		className="h-4 w-4"
		fill="none"
		viewBox="0 0 24 24"
		stroke="currentColor"
		strokeWidth={2}
		role="img"
		aria-label="Webhooks"
	>
		<path
			strokeLinecap="round"
			strokeLinejoin="round"
			d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
		/>
	</svg>
);

function ClerkUserButton() {
	return (
		<ClerkUserButtonComponent
			showName
			appearance={{
				elements: {
					rootBox: "w-full",
					userButtonTrigger: "w-full justify-start",
				},
			}}
		>
			<ClerkUserButtonComponent.UserProfilePage
				label="Billing"
				url="billing"
				labelIcon={BillingIcon}
			>
				<BillingPage />
			</ClerkUserButtonComponent.UserProfilePage>
			<ClerkUserButtonComponent.UserProfilePage
				label="Webhooks"
				url="webhooks"
				labelIcon={WebhooksIcon}
			>
				<WebhooksPage />
			</ClerkUserButtonComponent.UserProfilePage>
		</ClerkUserButtonComponent>
	);
}

/** Returns identity widgets backed by Clerk (prod) or dev stubs. */
export function getIdentityWidgets() {
	if (!CLERK_ENABLED) {
		return {
			OrgSwitcher: DevOrgSwitcher,
			UserButton: DevUserButton,
		};
	}

	return {
		OrgSwitcher: ClerkOrgSwitcher,
		UserButton: ClerkUserButton,
	};
}

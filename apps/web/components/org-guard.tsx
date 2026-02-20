"use client";

import { useOrganization, useOrganizationList } from "@clerk/nextjs";
import { usePathname, useRouter } from "next/navigation";
import { useEffect } from "react";
import { CLERK_ENABLED } from "@/lib/auth-config";

function ClerkOrgGuard({ children }: { children: React.ReactNode }) {
	const { organization } = useOrganization();
	const { isLoaded, userMemberships, setActive } = useOrganizationList({
		userMemberships: { infinite: true },
	});
	const pathname = usePathname();
	const router = useRouter();

	const memberships = userMemberships.data ?? [];
	const isOnboarding = pathname === "/onboarding";

	useEffect(() => {
		if (!isLoaded || organization || isOnboarding) return;

		if (memberships.length === 0) {
			router.push("/onboarding");
		} else {
			// Has memberships but no active org — auto-select first
			setActive?.({ organization: memberships[0].organization.id });
		}
	}, [isLoaded, organization, memberships, isOnboarding, router, setActive]);

	if (!isLoaded) {
		return (
			<div className="flex h-full items-center justify-center">
				<p className="text-sm text-gray-400">Loading...</p>
			</div>
		);
	}

	return <>{children}</>;
}

/** Ensures the user has an active organisation before rendering children. */
export function OrgGuard({ children }: { children: React.ReactNode }) {
	if (!CLERK_ENABLED) return <>{children}</>;
	return <ClerkOrgGuard>{children}</ClerkOrgGuard>;
}

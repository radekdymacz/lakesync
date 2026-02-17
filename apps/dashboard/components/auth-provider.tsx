"use client";

import { ClerkProvider } from "@clerk/nextjs";

/**
 * Wraps children with ClerkProvider when Clerk is configured,
 * otherwise renders children directly for dev mode.
 */
export function AuthProvider({
	enabled,
	children,
}: {
	enabled: boolean;
	children: React.ReactNode;
}) {
	if (enabled) {
		return <ClerkProvider>{children}</ClerkProvider>;
	}
	return <>{children}</>;
}

"use client";

import { CLERK_ENABLED, DEV_ORG_ID, DEV_USER_ID } from "./auth-config";

interface AuthState {
	orgId: string | null | undefined;
	userId: string | null | undefined;
	isSignedIn: boolean | undefined;
}

const DEV_AUTH: AuthState = {
	orgId: DEV_ORG_ID,
	userId: DEV_USER_ID,
	isSignedIn: true,
};

/**
 * Drop-in replacement for Clerk's useAuth().
 * Returns hardcoded dev values when Clerk is not configured.
 *
 * CLERK_ENABLED is a build-time constant (env var presence check),
 * so the branch is dead-code-eliminated — the hook call order is
 * stable at runtime.
 */
export function useAuth(): AuthState {
	if (!CLERK_ENABLED) {
		return DEV_AUTH;
	}
	// eslint-disable-next-line @typescript-eslint/no-require-imports
	const clerk = require("@clerk/nextjs") as typeof import("@clerk/nextjs");
	// biome-ignore lint/correctness/useHookAtTopLevel: CLERK_ENABLED is a build-time constant — branch is dead-code-eliminated
	const auth = clerk.useAuth();
	return { orgId: auth.orgId, userId: auth.userId, isSignedIn: auth.isSignedIn };
}

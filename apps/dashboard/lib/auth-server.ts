import { CLERK_ENABLED, DEV_ORG_ID, DEV_USER_ID } from "./auth-config";

interface ServerAuth {
	userId: string | null;
	orgId: string | null | undefined;
	getToken: () => Promise<string | null>;
}

const DEV_AUTH: ServerAuth = {
	userId: DEV_USER_ID,
	orgId: DEV_ORG_ID,
	getToken: async () => "dev-token",
};

/**
 * Drop-in replacement for Clerk's server-side auth().
 * Returns hardcoded dev values when Clerk is not configured.
 */
export async function serverAuth(): Promise<ServerAuth> {
	if (!CLERK_ENABLED) {
		return DEV_AUTH;
	}
	const { auth } = await import("@clerk/nextjs/server");
	return auth();
}

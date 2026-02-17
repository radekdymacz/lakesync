/**
 * Dev-mode JWT helper for local testing.
 * Uses the signToken utility from @lakesync/core.
 * For local development only — production would use an auth endpoint.
 */

import { signToken } from "@lakesync/core";

const DEV_SECRET = "lakesync-dev-secret-do-not-use-in-production";

/** Create a dev-mode JWT for the given client and gateway */
export async function createDevJwt(clientId: string, gatewayId: string): Promise<string> {
	return signToken({ sub: clientId, gw: gatewayId }, DEV_SECRET);
}

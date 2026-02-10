import stableStringify from "fast-json-stable-stringify";
import type { HLCTimestamp } from "../hlc/types";

/**
 * Generate a deterministic action ID using SHA-256.
 *
 * Same pattern as `generateDeltaId` in `delta/extract.ts` — uses the
 * Web Crypto API for cross-runtime compatibility (Node, Bun, browsers).
 */
export async function generateActionId(params: {
	clientId: string;
	hlc: HLCTimestamp;
	connector: string;
	actionType: string;
	params: Record<string, unknown>;
}): Promise<string> {
	const payload = stableStringify({
		clientId: params.clientId,
		hlc: params.hlc.toString(),
		connector: params.connector,
		actionType: params.actionType,
		params: params.params,
	});

	const data = new TextEncoder().encode(payload);
	const hashBuffer = await crypto.subtle.digest("SHA-256", data);
	const bytes = new Uint8Array(hashBuffer);

	let hex = "";
	for (const b of bytes) {
		hex += b.toString(16).padStart(2, "0");
	}
	return hex;
}

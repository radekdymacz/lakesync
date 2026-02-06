/**
 * Dev-mode JWT helper for local testing.
 * Signs an HS256 JWT using the Web Crypto API.
 * For local development only — production would use an auth endpoint.
 */

const DEV_SECRET = "lakesync-dev-secret-do-not-use-in-production";

/** Create a dev-mode JWT for the given client and gateway */
export async function createDevJwt(clientId: string, gatewayId: string): Promise<string> {
	const header = { alg: "HS256", typ: "JWT" };
	const payload = {
		sub: clientId,
		gw: gatewayId,
		iat: Math.floor(Date.now() / 1000),
		exp: Math.floor(Date.now() / 1000) + 3600, // 1 hour
	};

	const encoder = new TextEncoder();
	const headerB64 = btoa(JSON.stringify(header)).replace(/=/g, "");
	const payloadB64 = btoa(JSON.stringify(payload)).replace(/=/g, "");
	const signingInput = `${headerB64}.${payloadB64}`;

	const key = await crypto.subtle.importKey(
		"raw",
		encoder.encode(DEV_SECRET),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);

	const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(signingInput));
	const signatureB64 = btoa(String.fromCharCode(...new Uint8Array(signature)))
		.replace(/=/g, "")
		.replace(/\+/g, "-")
		.replace(/\//g, "_");

	return `${headerB64}.${payloadB64}.${signatureB64}`;
}

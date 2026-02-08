import { describe, expect, it } from "vitest";
import { verifyToken } from "../auth";

const TEST_SECRET = "test-secret-key-for-jwt-verification";

/**
 * Base64url-encode a Uint8Array, stripping padding.
 */
function base64urlEncode(data: Uint8Array): string {
	let binary = "";
	for (const byte of data) {
		binary += String.fromCharCode(byte);
	}
	return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/**
 * Base64url-encode a UTF-8 string.
 */
function base64urlEncodeString(str: string): string {
	return base64urlEncode(new TextEncoder().encode(str));
}

/**
 * Create a valid JWT signed with HMAC-SHA256 using Web Crypto.
 */
async function createJwt(
	payload: Record<string, unknown>,
	secret: string,
	header: Record<string, unknown> = { alg: "HS256", typ: "JWT" },
): Promise<string> {
	const headerB64 = base64urlEncodeString(JSON.stringify(header));
	const payloadB64 = base64urlEncodeString(JSON.stringify(payload));
	const signingInput = `${headerB64}.${payloadB64}`;

	const encoder = new TextEncoder();
	const keyData = encoder.encode(secret);

	const cryptoKey = await crypto.subtle.importKey(
		"raw",
		keyData,
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);

	const signatureBuffer = await crypto.subtle.sign("HMAC", cryptoKey, encoder.encode(signingInput));

	const signatureB64 = base64urlEncode(new Uint8Array(signatureBuffer));
	return `${headerB64}.${payloadB64}.${signatureB64}`;
}

/**
 * Return a payload with valid `sub`, `gw`, and a future `exp` claim.
 */
function validPayload(): Record<string, unknown> {
	return {
		sub: "client-1",
		gw: "gateway-1",
		exp: Math.floor(Date.now() / 1000) + 3600, // 1 hour from now
	};
}

describe("verifyToken", () => {
	// ── Structure checks ──────────────────────────────────────────────

	it("rejects a token with no dots (not a JWT)", async () => {
		const result = await verifyToken("nodots", TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Malformed JWT");
		}
	});

	it("rejects a token with only two segments", async () => {
		const result = await verifyToken("two.parts", TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Malformed JWT");
		}
	});

	it("rejects a token with four segments", async () => {
		const result = await verifyToken("a.b.c.d", TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Malformed JWT");
		}
	});

	// ── Header checks ─────────────────────────────────────────────────

	it("rejects a token with wrong algorithm (RS256)", async () => {
		const token = await createJwt(validPayload(), TEST_SECRET, {
			alg: "RS256",
			typ: "JWT",
		});
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Unsupported JWT");
		}
	});

	it("rejects a token with wrong typ header", async () => {
		const token = await createJwt(validPayload(), TEST_SECRET, {
			alg: "HS256",
			typ: "JWS",
		});
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Unsupported JWT");
		}
	});

	// ── Signature checks ──────────────────────────────────────────────

	it("rejects a token signed with a different secret", async () => {
		const token = await createJwt(validPayload(), "wrong-secret-key");
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Invalid JWT signature");
		}
	});

	// ── Expiry checks ─────────────────────────────────────────────────

	it("rejects an expired token", async () => {
		const payload = {
			...validPayload(),
			exp: Math.floor(Date.now() / 1000) - 60, // expired 1 minute ago
		};
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("expired");
		}
	});

	// ── Claim checks ──────────────────────────────────────────────────

	it("rejects a token missing the sub claim", async () => {
		const payload = { gw: "gateway-1", exp: Math.floor(Date.now() / 1000) + 3600 };
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("sub");
		}
	});

	it("rejects a token with empty sub claim", async () => {
		const payload = {
			sub: "",
			gw: "gateway-1",
			exp: Math.floor(Date.now() / 1000) + 3600,
		};
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("sub");
		}
	});

	it("rejects a token missing the gw claim", async () => {
		const payload = { sub: "client-1", exp: Math.floor(Date.now() / 1000) + 3600 };
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("gw");
		}
	});

	it("rejects a token with empty gw claim", async () => {
		const payload = {
			sub: "client-1",
			gw: "",
			exp: Math.floor(Date.now() / 1000) + 3600,
		};
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("gw");
		}
	});

	// ── Success ───────────────────────────────────────────────────────

	it("returns Ok with correct claims for a valid token", async () => {
		const token = await createJwt(validPayload(), TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.clientId).toBe("client-1");
			expect(result.value.gatewayId).toBe("gateway-1");
		}
	});

	it("accepts a valid token without an exp claim", async () => {
		const payload = { sub: "client-2", gw: "gateway-2" };
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.clientId).toBe("client-2");
			expect(result.value.gatewayId).toBe("gateway-2");
		}
	});

	// ── Custom claims extraction ─────────────────────────────────────

	it("extracts string custom claims from JWT payload", async () => {
		const payload = {
			...validPayload(),
			org_id: "org-42",
			role: "admin",
		};
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.customClaims.org_id).toBe("org-42");
			expect(result.value.customClaims.role).toBe("admin");
		}
	});

	it("extracts array custom claims from JWT payload", async () => {
		const payload = {
			...validPayload(),
			orgs: ["org-1", "org-2"],
		};
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.customClaims.orgs).toEqual(["org-1", "org-2"]);
		}
	});

	it("always includes sub in custom claims", async () => {
		const token = await createJwt(validPayload(), TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.customClaims.sub).toBe("client-1");
		}
	});

	it("excludes standard claims (gw, exp, iat, iss, aud) from custom claims", async () => {
		const payload = {
			...validPayload(),
			iat: Math.floor(Date.now() / 1000),
			iss: "test-issuer",
			aud: "test-audience",
		};
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.customClaims.gw).toBeUndefined();
			expect(result.value.customClaims.exp).toBeUndefined();
			expect(result.value.customClaims.iat).toBeUndefined();
			expect(result.value.customClaims.iss).toBeUndefined();
			expect(result.value.customClaims.aud).toBeUndefined();
		}
	});

	it("ignores non-string, non-string-array custom claims", async () => {
		const payload = {
			...validPayload(),
			numeric_claim: 42,
			nested: { foo: "bar" },
			valid_claim: "kept",
		};
		const token = await createJwt(payload, TEST_SECRET);
		const result = await verifyToken(token, TEST_SECRET);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.customClaims.numeric_claim).toBeUndefined();
			expect(result.value.customClaims.nested).toBeUndefined();
			expect(result.value.customClaims.valid_claim).toBe("kept");
		}
	});
});

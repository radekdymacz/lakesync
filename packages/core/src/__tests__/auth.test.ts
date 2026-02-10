import { describe, expect, it } from "vitest";
import { verifyToken } from "../auth";

// ---------------------------------------------------------------------------
// Helper: create test JWTs signed with HMAC-SHA256 via Web Crypto
// ---------------------------------------------------------------------------

const TEST_SECRET = "test-secret-key-for-lakesync";

/** Web Crypto subtle interface with HMAC methods (available at runtime in Bun/Node 20+) */
const subtle = crypto.subtle as unknown as {
	importKey(
		format: string,
		keyData: Uint8Array,
		algorithm: { name: string; hash: string },
		extractable: boolean,
		usages: string[],
	): Promise<unknown>;
	sign(algorithm: string, key: unknown, data: Uint8Array): Promise<ArrayBuffer>;
};

async function createTestJWT(
	payload: Record<string, unknown>,
	secret: string = TEST_SECRET,
): Promise<string> {
	const header = { alg: "HS256", typ: "JWT" };
	const enc = (obj: Record<string, unknown>) =>
		btoa(JSON.stringify(obj)).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
	const headerB64 = enc(header);
	const payloadB64 = enc(payload);
	const data = `${headerB64}.${payloadB64}`;
	const key = await subtle.importKey(
		"raw",
		new TextEncoder().encode(secret),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);
	const sig = await subtle.sign("HMAC", key, new TextEncoder().encode(data));
	const sigB64 = btoa(String.fromCharCode(...new Uint8Array(sig)))
		.replace(/=/g, "")
		.replace(/\+/g, "-")
		.replace(/\//g, "_");
	return `${data}.${sigB64}`;
}

/** Returns an expiry 1 hour in the future */
function futureExp(): number {
	return Math.floor(Date.now() / 1000) + 3600;
}

/** Returns an expiry 1 hour in the past */
function pastExp(): number {
	return Math.floor(Date.now() / 1000) - 3600;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("verifyToken", () => {
	describe("happy path", () => {
		it("returns Ok with AuthClaims for a valid token", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.clientId).toBe("client-1");
			expect(result.value.gatewayId).toBe("gateway-1");
		});

		it("extracts custom string claims", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
				tenantId: "tenant-abc",
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.customClaims.tenantId).toBe("tenant-abc");
		});

		it("extracts custom array claims", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
				groups: ["admin", "users"],
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.customClaims.groups).toEqual(["admin", "users"]);
		});

		it("populates role from token", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
				role: "admin",
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.role).toBe("admin");
		});

		it("defaults role to 'client' when absent", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.role).toBe("client");
		});
	});

	describe("expiry", () => {
		it("rejects an expired token", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: pastExp(),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("expired");
		});

		it("rejects a token with missing exp", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("exp");
		});

		it("accepts a token expiring far in the future", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: Math.floor(Date.now() / 1000) + 86400 * 365,
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
		});
	});

	describe("required claims", () => {
		it("rejects missing sub", async () => {
			const token = await createTestJWT({
				gw: "gateway-1",
				exp: futureExp(),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("sub");
		});

		it("rejects empty sub", async () => {
			const token = await createTestJWT({
				sub: "",
				gw: "gateway-1",
				exp: futureExp(),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("sub");
		});

		it("rejects missing gw", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				exp: futureExp(),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("gw");
		});

		it("rejects empty gw", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "",
				exp: futureExp(),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("gw");
		});
	});

	describe("signature verification", () => {
		it("rejects a tampered payload", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
			});
			const parts = token.split(".");
			// Tamper with payload by changing a character
			const tamperedPayload = `${parts[0]}.${parts[1]}x.${parts[2]}`;
			const result = await verifyToken(tamperedPayload, TEST_SECRET);
			expect(result.ok).toBe(false);
		});

		it("rejects a token signed with the wrong secret", async () => {
			const token = await createTestJWT(
				{ sub: "client-1", gw: "gateway-1", exp: futureExp() },
				"wrong-secret",
			);
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("signature");
		});

		it("rejects a token with empty signature segment", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
			});
			const parts = token.split(".");
			const noSig = `${parts[0]}.${parts[1]}.`;
			const result = await verifyToken(noSig, TEST_SECRET);
			expect(result.ok).toBe(false);
		});
	});

	describe("malformed tokens", () => {
		it("rejects a token with only 2 segments", async () => {
			const result = await verifyToken("abc.def", TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("three");
		});

		it("rejects a token with 4+ segments", async () => {
			const result = await verifyToken("a.b.c.d", TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("three");
		});

		it("rejects an empty string", async () => {
			const result = await verifyToken("", TEST_SECRET);
			expect(result.ok).toBe(false);
		});

		it("rejects non-base64url characters in header", async () => {
			const result = await verifyToken("!!!.payload.sig", TEST_SECRET);
			expect(result.ok).toBe(false);
		});

		it("rejects invalid JSON in payload", async () => {
			// Create a valid header, invalid payload JSON, dummy signature
			const header = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" }))
				.replace(/=/g, "")
				.replace(/\+/g, "-")
				.replace(/\//g, "_");
			const badPayload = btoa("not-json{{{")
				.replace(/=/g, "")
				.replace(/\+/g, "-")
				.replace(/\//g, "_");
			// Sign properly so we get past signature check — but the payload won't parse
			const key = await subtle.importKey(
				"raw",
				new TextEncoder().encode(TEST_SECRET),
				{ name: "HMAC", hash: "SHA-256" },
				false,
				["sign"],
			);
			const sigInput = `${header}.${badPayload}`;
			const sig = await subtle.sign("HMAC", key, new TextEncoder().encode(sigInput));
			const sigB64 = btoa(String.fromCharCode(...new Uint8Array(sig)))
				.replace(/=/g, "")
				.replace(/\+/g, "-")
				.replace(/\//g, "_");
			const token = `${header}.${badPayload}.${sigB64}`;
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.message).toContain("JSON");
		});
	});

	describe("custom claims filtering", () => {
		it("excludes reserved claims (iss, aud, iat) from customClaims", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
				iss: "lakesync",
				aud: "api",
				iat: Math.floor(Date.now() / 1000),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.customClaims.iss).toBeUndefined();
			expect(result.value.customClaims.aud).toBeUndefined();
			expect(result.value.customClaims.iat).toBeUndefined();
		});

		it("skips non-string non-array claims", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
				numClaim: 42,
				boolClaim: true,
				objClaim: { nested: true },
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.customClaims.numClaim).toBeUndefined();
			expect(result.value.customClaims.boolClaim).toBeUndefined();
			expect(result.value.customClaims.objClaim).toBeUndefined();
		});

		it("extracts only string and array custom claims from a mix", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
				org: "acme",
				tags: ["a", "b"],
				count: 99,
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.customClaims.org).toBe("acme");
			expect(result.value.customClaims.tags).toEqual(["a", "b"]);
			expect(result.value.customClaims.count).toBeUndefined();
		});

		it("always includes sub in customClaims for sync rules", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.customClaims.sub).toBe("client-1");
		});
	});

	describe("edge cases", () => {
		it("handles a large payload", async () => {
			const bigPayload: Record<string, unknown> = {
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
			};
			// Add many custom claims
			for (let i = 0; i < 100; i++) {
				bigPayload[`claim_${i}`] = `value_${i}`;
			}
			const token = await createTestJWT(bigPayload);
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value.customClaims.claim_0).toBe("value_0");
			expect(result.value.customClaims.claim_99).toBe("value_99");
		});

		it("returns empty customClaims when no custom claims are present", async () => {
			const token = await createTestJWT({
				sub: "client-1",
				gw: "gateway-1",
				exp: futureExp(),
			});
			const result = await verifyToken(token, TEST_SECRET);
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			// Only sub should be in customClaims (always included)
			const keys = Object.keys(result.value.customClaims);
			expect(keys).toEqual(["sub"]);
		});
	});
});

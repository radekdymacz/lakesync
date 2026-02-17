import { Err, Ok, type Result } from "./result/result";

/**
 * Minimal Web Crypto typing for HMAC operations.
 * The core package uses `lib: ["ES2022"]` which doesn't include DOM types.
 * These declarations cover the methods we need without pulling in the full DOM lib.
 */
interface HmacSubtle {
	importKey(
		format: "raw",
		keyData: Uint8Array,
		algorithm: { name: string; hash: string },
		extractable: boolean,
		usages: string[],
	): Promise<unknown>;
	sign(algorithm: string, key: unknown, data: Uint8Array): Promise<ArrayBuffer>;
	verify(
		algorithm: string,
		key: unknown,
		signature: Uint8Array,
		data: Uint8Array,
	): Promise<boolean>;
}

/** Claims extracted from a verified JWT token */
export interface AuthClaims {
	/** Client identifier (from JWT `sub` claim) */
	clientId: string;
	/** Authorised gateway ID (from JWT `gw` claim) */
	gatewayId: string;
	/** Role for route-level access control (from JWT `role` claim, defaults to "client") */
	role: string;
	/** Non-standard JWT claims for sync rule evaluation */
	customClaims: Record<string, string | string[]>;
}

/** Authentication error returned when JWT verification fails */
export class AuthError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "AuthError";
	}
}

/** Expected JWT header for HMAC-SHA256 tokens */
interface JwtHeader {
	alg: string;
	typ: string;
}

/** JWT payload with required claims */
interface JwtPayload {
	sub?: string;
	gw?: string;
	exp?: number;
	[key: string]: unknown;
}

/**
 * Encode a Uint8Array to a base64url string (no padding).
 */
function base64urlEncode(bytes: Uint8Array): string {
	let binary = "";
	for (let i = 0; i < bytes.length; i++) {
		binary += String.fromCharCode(bytes[i]!);
	}
	return btoa(binary).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

/**
 * Encode a UTF-8 string to a base64url string (no padding).
 */
function base64urlEncodeString(input: string): string {
	return btoa(input).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

/**
 * Decode a base64url-encoded string to a Uint8Array.
 * Handles the URL-safe alphabet (+/- replaced with -/_) and missing padding.
 */
function base64urlDecode(input: string): Uint8Array {
	// Restore standard base64 characters and padding
	const base64 = input.replace(/-/g, "+").replace(/_/g, "/");
	const padded = base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), "=");
	const binary = atob(padded);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) {
		bytes[i] = binary.charCodeAt(i);
	}
	return bytes;
}

/**
 * Parse a JSON string safely, returning null on failure.
 */
function parseJson(text: string): unknown {
	try {
		return JSON.parse(text);
	} catch {
		return null;
	}
}

/**
 * Verify a JWT token signed with HMAC-SHA256 and extract authentication claims.
 *
 * Uses the Web Crypto API exclusively (no external dependencies), making it
 * suitable for Cloudflare Workers and other edge runtimes.
 *
 * Accepts a single secret string or a `[primary, previous]` tuple for
 * zero-downtime secret rotation. When a tuple is provided, the primary
 * secret is tried first; if signature verification fails, the previous
 * secret is tried. `signToken` always signs with the first (primary) secret.
 *
 * @param token - The raw JWT string (header.payload.signature)
 * @param secret - The HMAC-SHA256 secret key, or `[primary, previous]` for rotation
 * @returns A Result containing AuthClaims on success, or AuthError on failure
 */
export async function verifyToken(
	token: string,
	secret: string | [string, string],
): Promise<Result<AuthClaims, AuthError>> {
	if (Array.isArray(secret)) {
		const primaryResult = await verifyTokenWithSecret(token, secret[0]);
		if (primaryResult.ok) return primaryResult;
		// Primary failed — try previous secret before giving up
		return verifyTokenWithSecret(token, secret[1]);
	}
	return verifyTokenWithSecret(token, secret);
}

/**
 * Verify a JWT with a single HMAC-SHA256 secret.
 */
async function verifyTokenWithSecret(
	token: string,
	secret: string,
): Promise<Result<AuthClaims, AuthError>> {
	// Split into three parts
	const parts = token.split(".");
	if (parts.length !== 3) {
		return Err(new AuthError("Malformed JWT: expected three dot-separated segments"));
	}

	const [headerB64, payloadB64, signatureB64] = parts;
	if (!headerB64 || !payloadB64 || !signatureB64) {
		return Err(new AuthError("Malformed JWT: empty segment"));
	}

	// Decode and verify header
	let headerBytes: Uint8Array;
	try {
		headerBytes = base64urlDecode(headerB64);
	} catch {
		return Err(new AuthError("Malformed JWT: invalid base64url in header"));
	}

	const header = parseJson(new TextDecoder().decode(headerBytes)) as JwtHeader | null;
	if (!header || header.alg !== "HS256" || header.typ !== "JWT") {
		return Err(new AuthError('Unsupported JWT: header must be {"alg":"HS256","typ":"JWT"}'));
	}

	// Import the HMAC key via Web Crypto
	const encoder = new TextEncoder();
	const keyData = encoder.encode(secret);

	let cryptoKey: unknown;
	try {
		cryptoKey = await (crypto.subtle as unknown as HmacSubtle).importKey(
			"raw",
			keyData,
			{ name: "HMAC", hash: "SHA-256" },
			false,
			["verify"],
		);
	} catch {
		return Err(new AuthError("Failed to import HMAC key"));
	}

	// Verify signature
	let signatureBytes: Uint8Array;
	try {
		signatureBytes = base64urlDecode(signatureB64);
	} catch {
		return Err(new AuthError("Malformed JWT: invalid base64url in signature"));
	}

	const signingInput = encoder.encode(`${headerB64}.${payloadB64}`);

	let valid: boolean;
	try {
		valid = await (crypto.subtle as unknown as HmacSubtle).verify(
			"HMAC",
			cryptoKey,
			signatureBytes,
			signingInput,
		);
	} catch {
		return Err(new AuthError("Signature verification failed"));
	}

	if (!valid) {
		return Err(new AuthError("Invalid JWT signature"));
	}

	// Decode payload
	let payloadBytes: Uint8Array;
	try {
		payloadBytes = base64urlDecode(payloadB64);
	} catch {
		return Err(new AuthError("Malformed JWT: invalid base64url in payload"));
	}

	const payload = parseJson(new TextDecoder().decode(payloadBytes)) as JwtPayload | null;
	if (!payload) {
		return Err(new AuthError("Malformed JWT: payload is not valid JSON"));
	}

	// Check expiry — exp claim is mandatory
	if (payload.exp === undefined || typeof payload.exp !== "number") {
		return Err(new AuthError('Missing or invalid "exp" claim (expiry)'));
	}
	const nowSeconds = Math.floor(Date.now() / 1000);
	if (payload.exp <= nowSeconds) {
		return Err(new AuthError("JWT has expired"));
	}

	// Extract required claims
	if (typeof payload.sub !== "string" || payload.sub.length === 0) {
		return Err(new AuthError('Missing or invalid "sub" claim (clientId)'));
	}

	if (typeof payload.gw !== "string" || payload.gw.length === 0) {
		return Err(new AuthError('Missing or invalid "gw" claim (gatewayId)'));
	}

	// Extract non-standard claims for sync rules evaluation
	const standardClaims = new Set(["sub", "gw", "exp", "iat", "iss", "aud", "role"]);
	const customClaims: Record<string, string | string[]> = {};

	for (const [key, value] of Object.entries(payload)) {
		if (standardClaims.has(key)) continue;
		if (typeof value === "string") {
			customClaims[key] = value;
		} else if (Array.isArray(value) && value.every((v) => typeof v === "string")) {
			customClaims[key] = value as string[];
		}
	}

	// Always include `sub` in custom claims so sync rules can reference jwt:sub
	customClaims.sub = payload.sub;

	// Extract role claim (default to "client" if absent)
	const role =
		typeof payload.role === "string" && payload.role.length > 0 ? payload.role : "client";

	return Ok({
		clientId: payload.sub,
		gatewayId: payload.gw,
		role,
		customClaims,
	});
}

// ---------------------------------------------------------------------------
// signToken — server-side JWT creation
// ---------------------------------------------------------------------------

/** Payload for signing a LakeSync JWT. */
export interface TokenPayload {
	/** Client identifier (becomes JWT `sub` claim) */
	sub: string;
	/** Authorised gateway ID (becomes JWT `gw` claim) */
	gw: string;
	/** Role for route-level access control. Defaults to `"client"`. */
	role?: "admin" | "client";
	/** Expiry as Unix seconds. Defaults to now + 3600 (1 hour). */
	exp?: number;
	/** Additional custom claims for sync rule evaluation */
	[key: string]: string | string[] | number | undefined;
}

/**
 * Sign a LakeSync JWT using HMAC-SHA256 via the Web Crypto API.
 *
 * Edge-runtime compatible (Cloudflare Workers, Deno, Bun, Node 20+).
 * The token header is always `{"alg":"HS256","typ":"JWT"}`.
 *
 * @param payload - The token payload. `role` defaults to `"client"`, `exp` defaults to now + 1 hour.
 * @param secret - The HMAC-SHA256 secret key.
 * @returns The signed JWT string.
 */
export async function signToken(payload: TokenPayload, secret: string): Promise<string> {
	const headerB64 = base64urlEncodeString(JSON.stringify({ alg: "HS256", typ: "JWT" }));

	// Apply defaults
	const claims: Record<string, unknown> = { ...payload };
	if (claims.role === undefined) {
		claims.role = "client";
	}
	if (claims.exp === undefined) {
		claims.exp = Math.floor(Date.now() / 1000) + 3600;
	}

	const payloadB64 = base64urlEncodeString(JSON.stringify(claims));
	const signingInput = `${headerB64}.${payloadB64}`;

	const encoder = new TextEncoder();
	const key = await (crypto.subtle as unknown as HmacSubtle).importKey(
		"raw",
		encoder.encode(secret),
		{ name: "HMAC", hash: "SHA-256" },
		false,
		["sign"],
	);

	const signature = await (crypto.subtle as unknown as HmacSubtle).sign(
		"HMAC",
		key,
		encoder.encode(signingInput),
	);

	const signatureB64 = base64urlEncode(new Uint8Array(signature));
	return `${signingInput}.${signatureB64}`;
}

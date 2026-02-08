import { Err, Ok, type Result } from "@lakesync/core";

/** Claims extracted from a verified JWT token. */
export interface AuthClaims {
	/** Client identifier (from JWT `sub` claim). */
	clientId: string;
	/** Authorised gateway ID (from JWT `gw` claim). */
	gatewayId: string;
	/** Role for route-level access control (from JWT `role` claim, defaults to "client"). */
	role: string;
	/** Non-standard JWT claims for sync rule evaluation. */
	customClaims: Record<string, string | string[]>;
}

/** Authentication error returned when JWT verification fails. */
export class AuthError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "AuthError";
	}
}

interface JwtHeader {
	alg: string;
	typ: string;
}

interface JwtPayload {
	sub?: string;
	gw?: string;
	exp?: number;
	[key: string]: unknown;
}

/**
 * Decode a base64url-encoded string to a Uint8Array.
 * Handles URL-safe alphabet and missing padding.
 */
function base64urlDecode(input: string): Uint8Array {
	const base64 = input.replace(/-/g, "+").replace(/_/g, "/");
	const padded = base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), "=");
	const binary = atob(padded);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i++) {
		bytes[i] = binary.charCodeAt(i);
	}
	return bytes;
}

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
 * Uses the Web Crypto API exclusively (no external dependencies).
 *
 * @param token - The raw JWT string (header.payload.signature).
 * @param secret - The HMAC-SHA256 secret key.
 * @returns A Result containing AuthClaims on success, or AuthError on failure.
 */
export async function verifyToken(
	token: string,
	secret: string,
): Promise<Result<AuthClaims, AuthError>> {
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

	let cryptoKey: CryptoKey;
	try {
		cryptoKey = await crypto.subtle.importKey(
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
		valid = await crypto.subtle.verify("HMAC", cryptoKey, signatureBytes, signingInput);
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

	// Check expiry
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

	// Extract non-standard claims
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

	// Always include sub so sync rules can reference jwt:sub
	customClaims.sub = payload.sub;

	const role =
		typeof payload.role === "string" && payload.role.length > 0 ? payload.role : "client";

	return Ok({
		clientId: payload.sub,
		gatewayId: payload.gw,
		role,
		customClaims,
	});
}

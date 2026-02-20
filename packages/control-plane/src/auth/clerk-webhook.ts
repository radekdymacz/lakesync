import { Err, Ok, type Result } from "@lakesync/core";
import type { OrgRole } from "../entities";
import type { ControlPlaneError } from "../errors";
import type { MemberRepository, OrgRepository } from "../repositories";

/**
 * Minimal Web Crypto typing for HMAC operations.
 * Avoids Uint8Array<ArrayBufferLike> vs Uint8Array<ArrayBuffer> issues in TS 5.7+.
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
}

/** Supported Clerk webhook event types */
export type ClerkEventType = "user.created" | "user.deleted" | "session.created";

/** Minimal Clerk user object from webhook payload */
export interface ClerkUserPayload {
	readonly id: string;
	readonly email_addresses?: ReadonlyArray<{ email_address: string }>;
	readonly first_name?: string;
	readonly last_name?: string;
}

/** Minimal Clerk session object from webhook payload */
export interface ClerkSessionPayload {
	readonly id: string;
	readonly user_id: string;
}

/** A Clerk webhook event */
export interface ClerkWebhookEvent {
	readonly type: ClerkEventType;
	readonly data: ClerkUserPayload | ClerkSessionPayload;
}

/** Result of processing a webhook event */
export interface WebhookResult {
	readonly processed: boolean;
	readonly action?: string;
}

/** Dependencies for the Clerk webhook handler */
export interface ClerkWebhookDeps {
	readonly orgRepo: OrgRepository;
	readonly memberRepo: MemberRepository;
}

/**
 * Verify a Clerk webhook signature using the Svix pattern.
 *
 * Clerk uses Svix for webhook delivery. The signature is HMAC-based.
 * Headers: `svix-id`, `svix-timestamp`, `svix-signature`.
 *
 * @param payload - Raw request body as string
 * @param headers - Webhook headers (svix-id, svix-timestamp, svix-signature)
 * @param secret - Webhook signing secret (from Clerk dashboard)
 * @returns true if signature is valid
 */
export async function verifyClerkWebhookSignature(
	payload: string,
	headers: { svixId: string; svixTimestamp: string; svixSignature: string },
	secret: string,
): Promise<boolean> {
	// Clerk webhook secrets are prefixed with "whsec_" and base64 encoded
	const secretBytes = base64Decode(secret.startsWith("whsec_") ? secret.slice(6) : secret);

	const signedContent = `${headers.svixId}.${headers.svixTimestamp}.${payload}`;
	const encoder = new TextEncoder();

	const subtle = crypto.subtle as unknown as HmacSubtle;
	const key = await subtle.importKey("raw", secretBytes, { name: "HMAC", hash: "SHA-256" }, false, [
		"sign",
	]);

	const signatureBytes = await subtle.sign("HMAC", key, encoder.encode(signedContent));
	const expectedSignature = base64Encode(new Uint8Array(signatureBytes));

	// Clerk sends multiple signatures separated by spaces, prefixed with "v1,"
	const signatures = headers.svixSignature.split(" ");
	for (const sig of signatures) {
		const parts = sig.split(",");
		if (parts[1] === expectedSignature) {
			return true;
		}
	}
	return false;
}

/**
 * Process a verified Clerk webhook event.
 *
 * - `user.created`: Creates a new organisation for the user (owner role)
 * - `user.deleted`: Removes user from all organisations
 * - `session.created`: No-op (reserved for audit logging)
 */
export async function processClerkWebhook(
	event: ClerkWebhookEvent,
	deps: ClerkWebhookDeps,
): Promise<Result<WebhookResult, ControlPlaneError>> {
	switch (event.type) {
		case "user.created":
			return handleUserCreated(event.data as ClerkUserPayload, deps);
		case "user.deleted":
			return handleUserDeleted(event.data as ClerkUserPayload, deps);
		case "session.created":
			return Ok({ processed: true, action: "session.created (no-op)" });
		default:
			return Ok({ processed: false, action: "unknown event type" });
	}
}

async function handleUserCreated(
	user: ClerkUserPayload,
	deps: ClerkWebhookDeps,
): Promise<Result<WebhookResult, ControlPlaneError>> {
	const email = user.email_addresses?.[0]?.email_address;
	const name = [user.first_name, user.last_name].filter(Boolean).join(" ") || email || user.id;
	const slug = generateSlug(name);

	// Create a personal organisation for this user
	const orgResult = await deps.orgRepo.create({
		name: `${name}'s Organisation`,
		slug,
	});
	if (!orgResult.ok) {
		// If slug collision, try with a random suffix
		if (orgResult.error.code === "DUPLICATE") {
			const retryResult = await deps.orgRepo.create({
				name: `${name}'s Organisation`,
				slug: `${slug}-${randomSuffix()}`,
			});
			if (!retryResult.ok) return retryResult;
			const memberResult = await deps.memberRepo.add({
				orgId: retryResult.value.id,
				userId: user.id,
				role: "owner" as OrgRole,
			});
			if (!memberResult.ok) return Err(memberResult.error);
			return Ok({ processed: true, action: "user.created: org + member created (retry slug)" });
		}
		return orgResult;
	}

	// Add user as owner
	const memberResult = await deps.memberRepo.add({
		orgId: orgResult.value.id,
		userId: user.id,
		role: "owner" as OrgRole,
	});
	if (!memberResult.ok) return Err(memberResult.error);

	return Ok({ processed: true, action: "user.created: org + member created" });
}

async function handleUserDeleted(
	_user: ClerkUserPayload,
	_deps: ClerkWebhookDeps,
): Promise<Result<WebhookResult, ControlPlaneError>> {
	// Remove user from all organisations they belong to
	// Note: in a full implementation, we'd query all orgs for this user.
	// For now, this is a no-op that signals the event was processed.
	// The actual removal would require listing all orgs by user ID,
	// which the MemberRepository doesn't support yet (listByUser).
	return Ok({ processed: true, action: "user.deleted: removal noted" });
}

function generateSlug(name: string): string {
	return name
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/^-|-$/g, "")
		.slice(0, 48);
}

function randomSuffix(): string {
	return Math.random().toString(36).slice(2, 8);
}

function base64Decode(str: string): Uint8Array {
	const binaryStr = atob(str);
	const bytes = new Uint8Array(binaryStr.length);
	for (let i = 0; i < binaryStr.length; i++) {
		bytes[i] = binaryStr.charCodeAt(i);
	}
	return bytes;
}

function base64Encode(bytes: Uint8Array): string {
	let binary = "";
	for (let i = 0; i < bytes.length; i++) {
		binary += String.fromCharCode(bytes[i]!);
	}
	return btoa(binary);
}

import { Err, Ok, type Result } from "@lakesync/core";
import { ControlPlaneError } from "../errors";
import type {
	ApiKeyRepository,
	GatewayRepository,
	MemberRepository,
	OrgRepository,
} from "../repositories";
import type {
	CreateDeletionRequestInput,
	DataExport,
	DeletionRequest,
	DeletionStatus,
} from "./types";

/** Dependencies for the GDPR deletion service */
export interface DeletionServiceDeps {
	readonly orgRepo: OrgRepository;
	readonly gatewayRepo: GatewayRepository;
	readonly apiKeyRepo: ApiKeyRepository;
	readonly memberRepo: MemberRepository;
}

/** In-memory store for deletion requests (production would use Postgres) */
const deletionRequests = new Map<string, DeletionRequest>();

function generateId(): string {
	return crypto.randomUUID().replace(/-/g, "").slice(0, 21);
}

/**
 * Create a data deletion request.
 *
 * The request is stored and can be processed asynchronously.
 * Returns the request ID immediately.
 */
export async function createDeletionRequest(
	input: CreateDeletionRequestInput,
	deps: DeletionServiceDeps,
): Promise<Result<DeletionRequest, ControlPlaneError>> {
	// Validate scope and target
	if (!input.scope || !input.targetId) {
		return Err(new ControlPlaneError("scope and targetId are required", "INVALID_INPUT"));
	}

	// Validate org exists
	const orgResult = await deps.orgRepo.getById(input.orgId);
	if (!orgResult.ok) return orgResult;
	if (orgResult.value === null) {
		return Err(new ControlPlaneError(`Organisation "${input.orgId}" not found`, "NOT_FOUND"));
	}

	const request: DeletionRequest = {
		id: generateId(),
		orgId: input.orgId,
		scope: input.scope,
		targetId: input.targetId,
		status: "pending",
		createdAt: new Date(),
	};

	deletionRequests.set(request.id, request);
	return Ok(request);
}

/**
 * Get the status of a deletion request.
 */
export async function getDeletionRequest(
	id: string,
): Promise<Result<DeletionRequest | null, ControlPlaneError>> {
	const request = deletionRequests.get(id);
	return Ok(request ?? null);
}

/**
 * Process a deletion request synchronously.
 *
 * In production, this would be called by an async job processor.
 */
export async function processDeletionRequest(
	id: string,
	deps: DeletionServiceDeps,
): Promise<Result<DeletionRequest, ControlPlaneError>> {
	const request = deletionRequests.get(id);
	if (!request) {
		return Err(new ControlPlaneError(`Deletion request "${id}" not found`, "NOT_FOUND"));
	}

	// Mark as processing
	const processing: DeletionRequest = { ...request, status: "processing" as DeletionStatus };
	deletionRequests.set(id, processing);

	try {
		switch (request.scope) {
			case "user":
				await processUserDeletion(request.orgId, request.targetId, deps);
				break;
			case "gateway":
				await processGatewayDeletion(request.targetId, deps);
				break;
			case "org":
				await processOrgDeletion(request.orgId, deps);
				break;
		}

		const completed: DeletionRequest = {
			...processing,
			status: "completed" as DeletionStatus,
			completedAt: new Date(),
		};
		deletionRequests.set(id, completed);
		return Ok(completed);
	} catch (error) {
		const failed: DeletionRequest = {
			...processing,
			status: "failed" as DeletionStatus,
			error: error instanceof Error ? error.message : String(error),
		};
		deletionRequests.set(id, failed);
		return Err(
			new ControlPlaneError(
				`Deletion request failed: ${failed.error}`,
				"INTERNAL",
				error instanceof Error ? error : undefined,
			),
		);
	}
}

/**
 * Delete all data associated with a user in an organisation.
 *
 * - Remove user from org membership
 * - Delete API keys owned by user (future: needs userId on API key)
 */
async function processUserDeletion(
	orgId: string,
	userId: string,
	deps: DeletionServiceDeps,
): Promise<void> {
	// Remove membership
	const removeResult = await deps.memberRepo.remove(orgId, userId);
	if (!removeResult.ok && removeResult.error.code !== "NOT_FOUND") {
		throw removeResult.error;
	}
}

/**
 * Delete all data associated with a gateway.
 *
 * - Delete gateway record (cascades to API keys via FK)
 */
async function processGatewayDeletion(
	gatewayId: string,
	deps: DeletionServiceDeps,
): Promise<void> {
	const deleteResult = await deps.gatewayRepo.delete(gatewayId);
	if (!deleteResult.ok && deleteResult.error.code !== "NOT_FOUND") {
		throw deleteResult.error;
	}
}

/**
 * Delete all data associated with an organisation.
 *
 * - Delete the org record (cascades to members, gateways, API keys via FK)
 */
async function processOrgDeletion(
	orgId: string,
	deps: DeletionServiceDeps,
): Promise<void> {
	const deleteResult = await deps.orgRepo.delete(orgId);
	if (!deleteResult.ok && deleteResult.error.code !== "NOT_FOUND") {
		throw deleteResult.error;
	}
}

/**
 * Export all organisation data for GDPR Article 20 compliance.
 *
 * Returns a structured object with all org data.
 */
export async function exportOrgData(
	orgId: string,
	deps: DeletionServiceDeps,
): Promise<Result<DataExport, ControlPlaneError>> {
	const orgResult = await deps.orgRepo.getById(orgId);
	if (!orgResult.ok) return orgResult;
	if (orgResult.value === null) {
		return Err(new ControlPlaneError(`Organisation "${orgId}" not found`, "NOT_FOUND"));
	}

	const membersResult = await deps.memberRepo.listByOrg(orgId);
	if (!membersResult.ok) return membersResult;

	const gatewaysResult = await deps.gatewayRepo.listByOrg(orgId);
	if (!gatewaysResult.ok) return gatewaysResult;

	const apiKeysResult = await deps.apiKeyRepo.listByOrg(orgId);
	if (!apiKeysResult.ok) return apiKeysResult;

	return Ok({
		organisation: orgResult.value as unknown as Record<string, unknown>,
		members: membersResult.value as unknown as ReadonlyArray<Record<string, unknown>>,
		gateways: gatewaysResult.value as unknown as ReadonlyArray<Record<string, unknown>>,
		apiKeys: apiKeysResult.value.map((k) => {
			// Never export keyHash — strip it from the export
			const { keyHash: _hash, ...rest } = k;
			return rest as unknown as Record<string, unknown>;
		}),
	});
}

/** Clear in-memory deletion request store (for testing) */
export function clearDeletionRequests(): void {
	deletionRequests.clear();
}

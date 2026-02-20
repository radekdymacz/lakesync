import { Err, Ok, type Result } from "@lakesync/core";
import type { CreateGatewayInput, Gateway, UpdateGatewayInput } from "../entities";
import { ControlPlaneError } from "../errors";
import { getPlan } from "../plans";
import type { GatewayRepository, OrgRepository } from "../repositories";

/** Dependencies for the gateway provisioning service */
export interface GatewayServiceDeps {
	readonly gatewayRepo: GatewayRepository;
	readonly orgRepo: OrgRepository;
}

/**
 * Create a new gateway for an organisation.
 *
 * Checks the org's plan quota before creating. Returns QUOTA_EXCEEDED
 * if the org has reached its maxGateways limit.
 */
export async function createGateway(
	orgId: string,
	input: { name: string; region?: string },
	deps: GatewayServiceDeps,
): Promise<Result<Gateway, ControlPlaneError>> {
	// Look up the org to get the plan
	const orgResult = await deps.orgRepo.getById(orgId);
	if (!orgResult.ok) return orgResult;
	if (orgResult.value === null) {
		return Err(new ControlPlaneError(`Organisation "${orgId}" not found`, "NOT_FOUND"));
	}

	const org = orgResult.value;
	const plan = getPlan(org.plan);

	// Check gateway quota (-1 means unlimited)
	if (plan.maxGateways !== -1) {
		const listResult = await deps.gatewayRepo.listByOrg(orgId);
		if (!listResult.ok) return listResult;

		const activeCount = listResult.value.filter((gw) => gw.status !== "deleted").length;
		if (activeCount >= plan.maxGateways) {
			return Err(
				new ControlPlaneError(
					`Gateway limit reached (${plan.maxGateways} on ${plan.name} plan). Upgrade to create more.`,
					"QUOTA_EXCEEDED",
				),
			);
		}
	}

	const createInput: CreateGatewayInput = {
		orgId,
		name: input.name,
		region: input.region,
	};

	return deps.gatewayRepo.create(createInput);
}

/** List all gateways for an organisation */
export async function listGateways(
	orgId: string,
	deps: GatewayServiceDeps,
): Promise<Result<Gateway[], ControlPlaneError>> {
	return deps.gatewayRepo.listByOrg(orgId);
}

/** Get a single gateway by ID */
export async function getGateway(
	id: string,
	deps: GatewayServiceDeps,
): Promise<Result<Gateway | null, ControlPlaneError>> {
	return deps.gatewayRepo.getById(id);
}

/** Update a gateway's name, region, or status */
export async function updateGateway(
	id: string,
	input: UpdateGatewayInput,
	deps: GatewayServiceDeps,
): Promise<Result<Gateway, ControlPlaneError>> {
	return deps.gatewayRepo.update(id, input);
}

/**
 * Soft-delete a gateway by setting its status to "deleted".
 *
 * Does not remove data immediately — data retained for 30 days.
 */
export async function deleteGateway(
	id: string,
	deps: GatewayServiceDeps,
): Promise<Result<Gateway, ControlPlaneError>> {
	return deps.gatewayRepo.update(id, { status: "deleted" });
}

/**
 * Suspend all active gateways for an organisation.
 *
 * Called when payment fails or the org is manually suspended.
 * Suspended gateways reject all sync requests.
 */
export async function suspendOrgGateways(
	orgId: string,
	deps: GatewayServiceDeps,
): Promise<Result<number, ControlPlaneError>> {
	const listResult = await deps.gatewayRepo.listByOrg(orgId);
	if (!listResult.ok) return listResult;

	let suspended = 0;
	for (const gw of listResult.value) {
		if (gw.status === "active") {
			const updateResult = await deps.gatewayRepo.update(gw.id, { status: "suspended" });
			if (!updateResult.ok) return updateResult;
			suspended++;
		}
	}

	return Ok(suspended);
}

/**
 * Reactivate all suspended gateways for an organisation.
 *
 * Called when payment is restored or manual reactivation.
 */
export async function reactivateOrgGateways(
	orgId: string,
	deps: GatewayServiceDeps,
): Promise<Result<number, ControlPlaneError>> {
	const listResult = await deps.gatewayRepo.listByOrg(orgId);
	if (!listResult.ok) return listResult;

	let reactivated = 0;
	for (const gw of listResult.value) {
		if (gw.status === "suspended") {
			const updateResult = await deps.gatewayRepo.update(gw.id, { status: "active" });
			if (!updateResult.ok) return updateResult;
			reactivated++;
		}
	}

	return Ok(reactivated);
}

/**
 * Check whether a gateway is allowed to process requests.
 *
 * Returns Ok(true) if active, Err with appropriate code otherwise.
 */
export async function checkGatewayStatus(
	gatewayId: string,
	deps: GatewayServiceDeps,
): Promise<Result<Gateway, ControlPlaneError>> {
	const result = await deps.gatewayRepo.getById(gatewayId);
	if (!result.ok) return result;

	if (result.value === null) {
		return Err(new ControlPlaneError(`Gateway "${gatewayId}" not found`, "NOT_FOUND"));
	}

	const gw = result.value;
	if (gw.status === "suspended") {
		return Err(
			new ControlPlaneError(
				`Gateway "${gatewayId}" is suspended. Contact your organisation administrator.`,
				"QUOTA_EXCEEDED",
			),
		);
	}

	if (gw.status === "deleted") {
		return Err(new ControlPlaneError(`Gateway "${gatewayId}" has been deleted`, "NOT_FOUND"));
	}

	return Ok(gw);
}

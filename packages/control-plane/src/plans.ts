import type { Plan, PlanId } from "./entities";

const MB = 1024 * 1024;
const GB = 1024 * MB;

/** All available billing plans */
export const PLANS: Record<PlanId, Plan> = {
	free: {
		id: "free",
		name: "Free",
		maxGateways: 1,
		maxDeltasPerMonth: 10_000,
		maxStorageBytes: 100 * MB,
		maxConnectionsPerGateway: 5,
		rateLimit: 60,
		price: 0,
	},
	starter: {
		id: "starter",
		name: "Starter",
		maxGateways: 3,
		maxDeltasPerMonth: 100_000,
		maxStorageBytes: 1 * GB,
		maxConnectionsPerGateway: 25,
		rateLimit: 300,
		price: 2900,
		stripePriceId: "price_starter",
	},
	pro: {
		id: "pro",
		name: "Pro",
		maxGateways: 10,
		maxDeltasPerMonth: 1_000_000,
		maxStorageBytes: 10 * GB,
		maxConnectionsPerGateway: 100,
		rateLimit: 1000,
		price: 9900,
		stripePriceId: "price_pro",
	},
	enterprise: {
		id: "enterprise",
		name: "Enterprise",
		maxGateways: -1,
		maxDeltasPerMonth: -1,
		maxStorageBytes: -1,
		maxConnectionsPerGateway: -1,
		rateLimit: -1,
		price: -1,
		stripePriceId: "price_enterprise",
	},
};

/** Look up a plan by its identifier */
export function getPlan(id: PlanId): Plan {
	return PLANS[id];
}

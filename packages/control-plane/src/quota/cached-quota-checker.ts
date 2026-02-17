import type { Plan } from "../entities";
import { getPlan } from "../plans";
import type { GatewayRepository, OrgRepository, UsageRepository } from "../repositories";
import type { QuotaChecker, QuotaResult } from "./types";

/** Dependencies for the CachedQuotaChecker. */
export interface QuotaCheckerDeps {
	readonly orgRepo: OrgRepository;
	readonly gatewayRepo: GatewayRepository;
	readonly usageRepo: UsageRepository;
}

/** Configuration for cache behaviour. */
export interface QuotaCheckerConfig {
	/** Cache TTL in milliseconds (default 60_000). */
	cacheTtlMs?: number;
}

/** Cached entry for usage counter. */
interface CacheEntry {
	count: number;
	fetchedAt: number;
}

/**
 * Quota checker that caches current-period usage with a configurable TTL.
 *
 * Fail-open: if any backing store is unavailable, allows the request
 * and logs a warning.
 */
export class CachedQuotaChecker implements QuotaChecker {
	private readonly deps: QuotaCheckerDeps;
	private readonly cacheTtlMs: number;

	/** Per-org delta usage cache: orgId -> CacheEntry */
	private readonly deltaCache = new Map<string, CacheEntry>();
	/** Per-gateway connection count cache: gatewayId -> CacheEntry */
	private readonly connectionCache = new Map<string, CacheEntry>();
	/** Per-org gateway count cache: orgId -> CacheEntry */
	private readonly gatewayCountCache = new Map<string, CacheEntry>();

	/** In-flight delta counter: orgId -> accumulated since last cache refresh */
	private readonly inflightDeltas = new Map<string, number>();

	constructor(deps: QuotaCheckerDeps, config?: QuotaCheckerConfig) {
		this.deps = deps;
		this.cacheTtlMs = config?.cacheTtlMs ?? 60_000;
	}

	async checkPush(orgId: string, deltaCount: number): Promise<QuotaResult> {
		try {
			const plan = await this.getOrgPlan(orgId);
			if (!plan) {
				return { allowed: true, remaining: Number.MAX_SAFE_INTEGER };
			}

			// Unlimited plan
			if (plan.maxDeltasPerMonth === -1) {
				return { allowed: true, remaining: Number.MAX_SAFE_INTEGER };
			}

			const used = await this.getDeltaUsage(orgId);
			const inflight = this.inflightDeltas.get(orgId) ?? 0;
			const total = used + inflight + deltaCount;
			const remaining = Math.max(0, plan.maxDeltasPerMonth - (used + inflight));

			if (total > plan.maxDeltasPerMonth) {
				return {
					allowed: false,
					reason: `Monthly delta quota exceeded (${plan.maxDeltasPerMonth} deltas on ${plan.name} plan)`,
					resetAt: this.getMonthEnd(),
				};
			}

			// Track in-flight optimistically
			this.inflightDeltas.set(orgId, inflight + deltaCount);

			return { allowed: true, remaining: remaining - deltaCount };
		} catch {
			// Fail-open
			console.warn(`[lakesync] Quota check failed for org ${orgId}, allowing request (fail-open)`);
			return { allowed: true, remaining: Number.MAX_SAFE_INTEGER };
		}
	}

	async checkConnection(orgId: string, gatewayId: string): Promise<QuotaResult> {
		try {
			const plan = await this.getOrgPlan(orgId);
			if (!plan) {
				return { allowed: true, remaining: Number.MAX_SAFE_INTEGER };
			}

			if (plan.maxConnectionsPerGateway === -1) {
				return { allowed: true, remaining: Number.MAX_SAFE_INTEGER };
			}

			const currentConnections = await this.getConnectionCount(gatewayId);
			const remaining = Math.max(0, plan.maxConnectionsPerGateway - currentConnections);

			if (currentConnections >= plan.maxConnectionsPerGateway) {
				return {
					allowed: false,
					reason: `Connection limit reached (${plan.maxConnectionsPerGateway} per gateway on ${plan.name} plan)`,
				};
			}

			return { allowed: true, remaining: remaining - 1 };
		} catch {
			console.warn(
				`[lakesync] Connection quota check failed for org ${orgId}, allowing (fail-open)`,
			);
			return { allowed: true, remaining: Number.MAX_SAFE_INTEGER };
		}
	}

	async checkGatewayCreate(orgId: string): Promise<QuotaResult> {
		try {
			const plan = await this.getOrgPlan(orgId);
			if (!plan) {
				return { allowed: true, remaining: Number.MAX_SAFE_INTEGER };
			}

			if (plan.maxGateways === -1) {
				return { allowed: true, remaining: Number.MAX_SAFE_INTEGER };
			}

			const currentCount = await this.getGatewayCount(orgId);
			const remaining = Math.max(0, plan.maxGateways - currentCount);

			if (currentCount >= plan.maxGateways) {
				return {
					allowed: false,
					reason: `Gateway limit reached (${plan.maxGateways} gateways on ${plan.name} plan)`,
				};
			}

			return { allowed: true, remaining: remaining - 1 };
		} catch {
			console.warn(`[lakesync] Gateway quota check failed for org ${orgId}, allowing (fail-open)`);
			return { allowed: true, remaining: Number.MAX_SAFE_INTEGER };
		}
	}

	// -----------------------------------------------------------------------
	// Cache helpers
	// -----------------------------------------------------------------------

	private async getOrgPlan(orgId: string): Promise<Plan | null> {
		const result = await this.deps.orgRepo.getById(orgId);
		if (!result.ok || !result.value) return null;
		return getPlan(result.value.plan);
	}

	private async getDeltaUsage(orgId: string): Promise<number> {
		const now = Date.now();
		const cached = this.deltaCache.get(orgId);
		if (cached && now - cached.fetchedAt < this.cacheTtlMs) {
			return cached.count;
		}

		const { from, to } = this.getCurrentMonth();
		const result = await this.deps.usageRepo.queryUsage({
			orgId,
			from,
			to,
			eventType: "push_deltas",
		});

		if (!result.ok) {
			// Use stale cache if available, else 0
			return cached?.count ?? 0;
		}

		const total = result.value.reduce((sum, row) => sum + row.count, 0);
		this.deltaCache.set(orgId, { count: total, fetchedAt: now });
		// Reset in-flight counter on fresh cache
		this.inflightDeltas.delete(orgId);
		return total;
	}

	private async getConnectionCount(gatewayId: string): Promise<number> {
		const now = Date.now();
		const cached = this.connectionCache.get(gatewayId);
		if (cached && now - cached.fetchedAt < this.cacheTtlMs) {
			return cached.count;
		}

		// Query ws_connection events for the current hour as a proxy for active connections
		const hourStart = new Date();
		hourStart.setMinutes(0, 0, 0);
		const result = await this.deps.usageRepo.queryUsage({
			orgId: gatewayId, // UsageQuery uses orgId, but we filter by gateway in the result
			from: hourStart,
			to: new Date(),
			eventType: "ws_connection",
		});

		if (!result.ok) {
			return cached?.count ?? 0;
		}

		const total = result.value
			.filter((row) => row.gatewayId === gatewayId)
			.reduce((sum, row) => sum + row.count, 0);
		this.connectionCache.set(gatewayId, { count: total, fetchedAt: now });
		return total;
	}

	private async getGatewayCount(orgId: string): Promise<number> {
		const now = Date.now();
		const cached = this.gatewayCountCache.get(orgId);
		if (cached && now - cached.fetchedAt < this.cacheTtlMs) {
			return cached.count;
		}

		const result = await this.deps.gatewayRepo.listByOrg(orgId);
		if (!result.ok) {
			return cached?.count ?? 0;
		}

		const count = result.value.filter((gw) => gw.status !== "deleted").length;
		this.gatewayCountCache.set(orgId, { count, fetchedAt: now });
		return count;
	}

	private getCurrentMonth(): { from: Date; to: Date } {
		const now = new Date();
		const from = new Date(now.getFullYear(), now.getMonth(), 1);
		const to = new Date(now.getFullYear(), now.getMonth() + 1, 1);
		return { from, to };
	}

	private getMonthEnd(): Date {
		const now = new Date();
		return new Date(now.getFullYear(), now.getMonth() + 1, 1);
	}
}

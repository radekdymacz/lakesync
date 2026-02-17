import type { Result, UsageAggregate, UsageEventType } from "@lakesync/core";
import type {
	ApiKey,
	CreateApiKeyInput,
	CreateGatewayInput,
	CreateMemberInput,
	CreateOrgInput,
	Gateway,
	OrgMember,
	OrgRole,
	Organisation,
	UpdateGatewayInput,
	UpdateOrgInput,
} from "./entities";
import type { ControlPlaneError } from "./errors";

/** Repository for organisation CRUD operations */
export interface OrgRepository {
	create(org: CreateOrgInput): Promise<Result<Organisation, ControlPlaneError>>;
	getById(id: string): Promise<Result<Organisation | null, ControlPlaneError>>;
	getBySlug(slug: string): Promise<Result<Organisation | null, ControlPlaneError>>;
	update(id: string, input: UpdateOrgInput): Promise<Result<Organisation, ControlPlaneError>>;
	delete(id: string): Promise<Result<void, ControlPlaneError>>;
}

/** Repository for gateway CRUD operations */
export interface GatewayRepository {
	create(gw: CreateGatewayInput): Promise<Result<Gateway, ControlPlaneError>>;
	getById(id: string): Promise<Result<Gateway | null, ControlPlaneError>>;
	listByOrg(orgId: string): Promise<Result<Gateway[], ControlPlaneError>>;
	update(id: string, input: UpdateGatewayInput): Promise<Result<Gateway, ControlPlaneError>>;
	delete(id: string): Promise<Result<void, ControlPlaneError>>;
}

/** Repository for API key operations */
export interface ApiKeyRepository {
	create(
		key: CreateApiKeyInput,
	): Promise<Result<{ apiKey: ApiKey; rawKey: string }, ControlPlaneError>>;
	getByHash(keyHash: string): Promise<Result<ApiKey | null, ControlPlaneError>>;
	listByOrg(orgId: string): Promise<Result<ApiKey[], ControlPlaneError>>;
	revoke(id: string): Promise<Result<void, ControlPlaneError>>;
	updateLastUsed(id: string): Promise<Result<void, ControlPlaneError>>;
}

/** Repository for organisation member operations */
export interface MemberRepository {
	add(member: CreateMemberInput): Promise<Result<OrgMember, ControlPlaneError>>;
	remove(orgId: string, userId: string): Promise<Result<void, ControlPlaneError>>;
	listByOrg(orgId: string): Promise<Result<OrgMember[], ControlPlaneError>>;
	getRole(orgId: string, userId: string): Promise<Result<OrgRole | null, ControlPlaneError>>;
	updateRole(orgId: string, userId: string, role: OrgRole): Promise<Result<void, ControlPlaneError>>;
}

/** Time-range query input for usage data. */
export interface UsageQuery {
	/** Organisation ID to query. */
	orgId: string;
	/** Start of the time range (inclusive). */
	from: Date;
	/** End of the time range (exclusive). */
	to: Date;
	/** Optional event type filter. */
	eventType?: UsageEventType;
}

/** Aggregated usage row returned by queries. */
export interface UsageRow {
	/** Gateway that produced the events. */
	gatewayId: string;
	/** Organisation ID. */
	orgId: string;
	/** Type of billable event. */
	eventType: UsageEventType;
	/** Summed count for the window. */
	count: number;
	/** Start of the aggregation window. */
	windowStart: Date;
}

/** Repository for usage metering data. Implements {@link UsageStore} for writes and adds query methods. */
export interface UsageRepository {
	/** Write aggregated counters to persistent storage. */
	recordAggregates(aggregates: UsageAggregate[]): Promise<Result<void, ControlPlaneError>>;
	/** Query usage for an organisation within a time range. */
	queryUsage(query: UsageQuery): Promise<Result<UsageRow[], ControlPlaneError>>;
	/** Query usage per gateway for an organisation within a time range. */
	queryGatewayUsage(
		query: UsageQuery,
	): Promise<Result<UsageRow[], ControlPlaneError>>;
}

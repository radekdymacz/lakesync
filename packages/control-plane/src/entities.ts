/** Plan tier identifier */
export type PlanId = "free" | "starter" | "pro" | "enterprise";

/** Organisation member role */
export type OrgRole = "owner" | "admin" | "member" | "viewer";

/** Gateway lifecycle status */
export type GatewayStatus = "active" | "suspended" | "deleted";

/** API key role scope */
export type ApiKeyRole = "admin" | "client";

/** A tenant organisation */
export interface Organisation {
	readonly id: string;
	readonly name: string;
	readonly slug: string;
	readonly createdAt: Date;
	readonly updatedAt: Date;
	readonly plan: PlanId;
	readonly stripeCustomerId?: string;
	readonly stripeSubscriptionId?: string;
}

/** A member of an organisation */
export interface OrgMember {
	readonly orgId: string;
	readonly userId: string;
	readonly role: OrgRole;
	readonly createdAt: Date;
}

/** A sync gateway belonging to an organisation */
export interface Gateway {
	readonly id: string;
	readonly orgId: string;
	readonly name: string;
	readonly region?: string;
	readonly status: GatewayStatus;
	readonly createdAt: Date;
	readonly updatedAt: Date;
}

/** An API key for programmatic access */
export interface ApiKey {
	readonly id: string;
	readonly orgId: string;
	readonly gatewayId?: string;
	readonly name: string;
	readonly keyHash: string;
	readonly keyPrefix: string;
	readonly role: ApiKeyRole;
	readonly scopes?: readonly string[];
	readonly expiresAt?: Date;
	readonly lastUsedAt?: Date;
	readonly createdAt: Date;
}

/** A billing plan definition */
export interface Plan {
	readonly id: PlanId;
	readonly name: string;
	readonly maxGateways: number;
	readonly maxDeltasPerMonth: number;
	readonly maxStorageBytes: number;
	readonly maxConnectionsPerGateway: number;
	readonly rateLimit: number;
	readonly price: number;
	readonly stripePriceId?: string;
}

/** Input for creating an organisation */
export interface CreateOrgInput {
	readonly name: string;
	readonly slug: string;
	readonly plan?: PlanId;
	readonly stripeCustomerId?: string;
	readonly stripeSubscriptionId?: string;
}

/** Input for updating an organisation */
export interface UpdateOrgInput {
	readonly name?: string;
	readonly slug?: string;
	readonly plan?: PlanId;
	readonly stripeCustomerId?: string;
	readonly stripeSubscriptionId?: string;
}

/** Input for creating a gateway */
export interface CreateGatewayInput {
	readonly orgId: string;
	readonly name: string;
	readonly region?: string;
}

/** Input for updating a gateway */
export interface UpdateGatewayInput {
	readonly name?: string;
	readonly region?: string;
	readonly status?: GatewayStatus;
}

/** Input for creating an API key */
export interface CreateApiKeyInput {
	readonly orgId: string;
	readonly gatewayId?: string;
	readonly name: string;
	readonly role: ApiKeyRole;
	readonly scopes?: readonly string[];
	readonly expiresAt?: Date;
}

/** Input for creating an org member */
export interface CreateMemberInput {
	readonly orgId: string;
	readonly userId: string;
	readonly role: OrgRole;
}

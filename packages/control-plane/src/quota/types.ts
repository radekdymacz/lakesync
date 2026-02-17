/** Result of a quota check. */
export type QuotaResult =
	| { readonly allowed: true; readonly remaining: number }
	| { readonly allowed: false; readonly reason: string; readonly resetAt?: Date };

/** Checks usage against plan limits. */
export interface QuotaChecker {
	/** Check whether a push of `deltaCount` deltas is allowed. */
	checkPush(orgId: string, deltaCount: number): Promise<QuotaResult>;
	/** Check whether a new WebSocket connection is allowed. */
	checkConnection(orgId: string, gatewayId: string): Promise<QuotaResult>;
	/** Check whether creating a new gateway is allowed. */
	checkGatewayCreate(orgId: string): Promise<QuotaResult>;
}

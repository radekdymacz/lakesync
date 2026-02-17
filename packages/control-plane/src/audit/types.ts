/** Actor type for audit events */
export type AuditActorType = "user" | "api_key" | "system";

/** Exhaustive set of auditable actions */
export type AuditAction =
	| "gateway.create"
	| "gateway.update"
	| "gateway.delete"
	| "gateway.suspend"
	| "api_key.create"
	| "api_key.revoke"
	| "api_key.rotate"
	| "schema.update"
	| "sync_rules.update"
	| "connector.register"
	| "connector.unregister"
	| "member.add"
	| "member.remove"
	| "member.role_change"
	| "billing.plan_change"
	| "billing.payment_failed"
	| "flush.manual";

/** A single audit log entry */
export interface AuditEvent {
	readonly id: string;
	readonly orgId: string;
	readonly actorId: string;
	readonly actorType: AuditActorType;
	readonly action: AuditAction;
	readonly resource: string;
	readonly metadata?: Record<string, unknown>;
	readonly ipAddress?: string;
	readonly timestamp: Date;
}

/** Input for recording an audit event */
export interface RecordAuditInput {
	readonly orgId: string;
	readonly actorId: string;
	readonly actorType: AuditActorType;
	readonly action: AuditAction;
	readonly resource: string;
	readonly metadata?: Record<string, unknown>;
	readonly ipAddress?: string;
}

/** Query parameters for listing audit events */
export interface AuditQuery {
	readonly orgId: string;
	readonly from?: Date;
	readonly to?: Date;
	readonly action?: AuditAction;
	readonly actorId?: string;
	readonly cursor?: string;
	readonly limit?: number;
}

/** Paginated audit event result */
export interface AuditPage {
	readonly events: AuditEvent[];
	readonly cursor?: string;
	readonly hasMore: boolean;
}

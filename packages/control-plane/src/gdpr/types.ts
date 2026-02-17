/** Scope of a data deletion request */
export type DeletionScope = "user" | "gateway" | "org";

/** Status of a data deletion request */
export type DeletionStatus = "pending" | "processing" | "completed" | "failed";

/** A data deletion request */
export interface DeletionRequest {
	readonly id: string;
	readonly orgId: string;
	readonly scope: DeletionScope;
	readonly targetId: string;
	readonly status: DeletionStatus;
	readonly error?: string;
	readonly createdAt: Date;
	readonly completedAt?: Date;
}

/** Input for creating a deletion request */
export interface CreateDeletionRequestInput {
	readonly orgId: string;
	readonly scope: DeletionScope;
	readonly targetId: string;
}

/** Summary of exported organisation data */
export interface DataExport {
	readonly organisation: Record<string, unknown>;
	readonly members: ReadonlyArray<Record<string, unknown>>;
	readonly gateways: ReadonlyArray<Record<string, unknown>>;
	readonly apiKeys: ReadonlyArray<Record<string, unknown>>;
}

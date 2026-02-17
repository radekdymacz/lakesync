import type { Result } from "@lakesync/core";
import type { ControlPlaneError } from "../errors";
import type { AuditPage, AuditQuery, RecordAuditInput } from "./types";

/** Append-only audit log interface */
export interface AuditLogger {
	/** Record a new audit event (append-only, never fails the calling operation) */
	record(input: RecordAuditInput): Promise<Result<void, ControlPlaneError>>;

	/** Query audit events with optional filters and cursor pagination */
	query(params: AuditQuery): Promise<Result<AuditPage, ControlPlaneError>>;

	/** Delete events older than the given date (for retention enforcement) */
	deleteOlderThan(orgId: string, before: Date): Promise<Result<number, ControlPlaneError>>;
}

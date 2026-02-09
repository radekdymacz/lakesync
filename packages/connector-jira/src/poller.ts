// ---------------------------------------------------------------------------
// JiraSourcePoller — polls Jira Cloud and pushes deltas to SyncGateway
// ---------------------------------------------------------------------------

import type { RowDelta } from "@lakesync/core";
import { BaseSourcePoller, extractDelta, type PushTarget } from "@lakesync/core";
import { JiraClient } from "./client";
import { mapComment, mapIssue, mapProject } from "./mapping";
import type { JiraConnectorConfig, JiraIngestConfig } from "./types";

const DEFAULT_INTERVAL_MS = 30_000;

/**
 * Polls Jira Cloud for issues, comments, and projects and pushes
 * detected changes into a gateway via `handlePush()`.
 *
 * Extends {@link BaseSourcePoller} for lifecycle (start/stop/schedule)
 * and push logic.
 */
export class JiraSourcePoller extends BaseSourcePoller {
	private readonly connectionConfig: JiraConnectorConfig;
	private readonly client: JiraClient;

	/** Cursor: max `fields.updated` value from the last issue poll. */
	private lastUpdated: string | undefined;

	/** In-memory snapshot for comment diff (keyed by rowId). */
	private commentSnapshot = new Map<string, Record<string, unknown>>();

	/** In-memory snapshot for project diff (keyed by project key). */
	private projectSnapshot = new Map<string, Record<string, unknown>>();

	constructor(
		connectionConfig: JiraConnectorConfig,
		ingestConfig: JiraIngestConfig | undefined,
		name: string,
		gateway: PushTarget,
		client?: JiraClient,
	) {
		super({
			name,
			intervalMs: ingestConfig?.intervalMs ?? DEFAULT_INTERVAL_MS,
			gateway,
		});
		this.connectionConfig = connectionConfig;
		this.client = client ?? new JiraClient(connectionConfig);
	}

	/** Execute a single poll cycle across all entity types. */
	async poll(): Promise<void> {
		const allDeltas: RowDelta[] = [];

		// 1. Issues (cursor strategy via `updated` field)
		const issueDeltas = await this.pollIssues();
		for (const d of issueDeltas.deltas) {
			allDeltas.push(d);
		}

		// 2. Comments (diff per-issue, only for issues returned in this poll)
		const includeComments = this.connectionConfig.includeComments ?? true;
		if (includeComments && issueDeltas.issueKeys.length > 0) {
			const commentDeltas = await this.pollComments(issueDeltas.issueKeys);
			for (const d of commentDeltas) {
				allDeltas.push(d);
			}
		}

		// 3. Projects (full diff)
		const includeProjects = this.connectionConfig.includeProjects ?? true;
		if (includeProjects) {
			const projectDeltas = await this.pollProjects();
			for (const d of projectDeltas) {
				allDeltas.push(d);
			}
		}

		this.pushDeltas(allDeltas);
	}

	// -----------------------------------------------------------------------
	// Issues — cursor strategy via JQL `updated` field
	// -----------------------------------------------------------------------

	private async pollIssues(): Promise<{ deltas: RowDelta[]; issueKeys: string[] }> {
		const result = await this.client.searchIssues(
			this.connectionConfig.jql ?? "",
			this.lastUpdated,
		);

		if (!result.ok) {
			return { deltas: [], issueKeys: [] };
		}

		const issues = result.value;
		if (issues.length === 0) {
			return { deltas: [], issueKeys: [] };
		}

		const deltas: RowDelta[] = [];
		const issueKeys: string[] = [];
		let maxUpdated = this.lastUpdated;

		for (const issue of issues) {
			const { rowId, row } = mapIssue(issue);
			issueKeys.push(issue.key);

			const delta = await extractDelta(null, row, {
				table: "jira_issues",
				rowId,
				clientId: this.clientId,
				hlc: this.hlc.now(),
			});

			if (delta) {
				deltas.push(delta);
			}

			// Track max updated timestamp for cursor advancement
			const updated = issue.fields.updated;
			if (updated && (!maxUpdated || updated > maxUpdated)) {
				maxUpdated = updated;
			}
		}

		this.lastUpdated = maxUpdated;
		return { deltas, issueKeys };
	}

	// -----------------------------------------------------------------------
	// Comments — diff per-issue
	// -----------------------------------------------------------------------

	private async pollComments(issueKeys: string[]): Promise<RowDelta[]> {
		const deltas: RowDelta[] = [];

		for (const issueKey of issueKeys) {
			const result = await this.client.getComments(issueKey);
			if (!result.ok) continue;

			const currentMap = new Map<string, Record<string, unknown>>();

			for (const comment of result.value) {
				const { rowId, row } = mapComment(issueKey, comment);
				currentMap.set(rowId, row);

				const previous = this.commentSnapshot.get(rowId) ?? null;
				const delta = await extractDelta(previous, row, {
					table: "jira_comments",
					rowId,
					clientId: this.clientId,
					hlc: this.hlc.now(),
				});

				if (delta) {
					deltas.push(delta);
				}
			}

			// Update snapshot for this issue's comments
			for (const [rowId, row] of currentMap) {
				this.commentSnapshot.set(rowId, row);
			}
		}

		return deltas;
	}

	// -----------------------------------------------------------------------
	// Projects — full diff
	// -----------------------------------------------------------------------

	private async pollProjects(): Promise<RowDelta[]> {
		const result = await this.client.getProjects();
		if (!result.ok) return [];

		const currentMap = new Map<string, Record<string, unknown>>();

		for (const project of result.value) {
			const { rowId, row } = mapProject(project);
			currentMap.set(rowId, row);
		}

		const previousMap = this.projectSnapshot;
		const deltas: RowDelta[] = [];

		// Detect inserts and updates
		for (const [rowId, currentRow] of currentMap) {
			const previousRow = previousMap.get(rowId) ?? null;

			const delta = await extractDelta(previousRow, currentRow, {
				table: "jira_projects",
				rowId,
				clientId: this.clientId,
				hlc: this.hlc.now(),
			});

			if (delta) {
				deltas.push(delta);
			}
		}

		// Detect deletes: rows in previous snapshot missing from current
		for (const [rowId, previousRow] of previousMap) {
			if (!currentMap.has(rowId)) {
				const delta = await extractDelta(previousRow, null, {
					table: "jira_projects",
					rowId,
					clientId: this.clientId,
					hlc: this.hlc.now(),
				});

				if (delta) {
					deltas.push(delta);
				}
			}
		}

		// Replace snapshot
		this.projectSnapshot = currentMap;

		return deltas;
	}
}

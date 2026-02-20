/**
 * In-memory dev store for dashboard API routes.
 * Used when Clerk is not configured (dev mode) so the dashboard
 * is fully functional without an external control plane API.
 *
 * Attached to globalThis so state survives Next.js HMR reloads.
 * Uses atomic-swap pattern — the ReadonlyMap reference is replaced
 * on each mutation, never mutated in-place.
 */

export interface DevGateway {
	id: string;
	orgId: string;
	name: string;
	region?: string;
	status: string;
	createdAt: string;
}

// Survive HMR — Next.js dev mode re-imports modules on file changes
const g = globalThis as unknown as { __devGateways?: ReadonlyMap<string, DevGateway> };
if (!g.__devGateways) {
	g.__devGateways = new Map();
}

function getSnapshot(): ReadonlyMap<string, DevGateway> {
	return g.__devGateways!;
}

function setSnapshot(next: ReadonlyMap<string, DevGateway>): void {
	g.__devGateways = next;
}

export const devStore = {
	// --- Gateways ---

	listGateways(orgId: string): DevGateway[] {
		return [...getSnapshot().values()].filter((gw) => gw.orgId === orgId);
	},

	getGateway(id: string): DevGateway | undefined {
		return getSnapshot().get(id);
	},

	createGateway(input: { orgId: string; name: string; region?: string }): DevGateway {
		const gw: DevGateway = {
			id: `gw-${crypto.randomUUID().slice(0, 8)}`,
			orgId: input.orgId,
			name: input.name,
			region: input.region,
			status: "active",
			createdAt: new Date().toISOString(),
		};
		setSnapshot(new Map([...getSnapshot(), [gw.id, gw]]));
		return gw;
	},

	updateGateway(
		id: string,
		patch: Partial<Pick<DevGateway, "name" | "status">>,
	): DevGateway | undefined {
		const gw = getSnapshot().get(id);
		if (!gw) return undefined;
		const updated = { ...gw, ...patch };
		setSnapshot(new Map([...getSnapshot(), [id, updated]]));
		return updated;
	},

	deleteGateway(id: string): boolean {
		const snap = getSnapshot();
		if (!snap.has(id)) return false;
		const next = new Map(snap);
		next.delete(id);
		setSnapshot(next);
		return true;
	},

	// --- Usage (mock) ---

	getUsage(_orgId: string, from: string, to: string) {
		const days: Array<{
			date: string;
			pushDeltas: number;
			pullDeltas: number;
			apiCalls: number;
			storageBytes: number;
		}> = [];

		const start = new Date(from);
		const end = new Date(to);
		const cursor = new Date(start);

		while (cursor <= end) {
			days.push({
				date: cursor.toISOString().slice(0, 10),
				pushDeltas: Math.floor(Math.random() * 500),
				pullDeltas: Math.floor(Math.random() * 300),
				apiCalls: Math.floor(Math.random() * 100),
				storageBytes: Math.floor(Math.random() * 1024 * 1024),
			});
			cursor.setDate(cursor.getDate() + 1);
		}

		const totals = days.reduce(
			(acc, d) => ({
				pushDeltas: acc.pushDeltas + d.pushDeltas,
				pullDeltas: acc.pullDeltas + d.pullDeltas,
				apiCalls: acc.apiCalls + d.apiCalls,
				storageBytes: acc.storageBytes + d.storageBytes,
			}),
			{ pushDeltas: 0, pullDeltas: 0, apiCalls: 0, storageBytes: 0 },
		);

		return { data: days, totals };
	},

	// --- Billing (mock) ---

	getBilling(_orgId: string) {
		return {
			plan: "free",
			planName: "Free",
			price: 0,
			maxDeltasPerMonth: 10_000,
			maxStorageBytes: 100 * 1024 * 1024,
			usage: {
				deltasThisPeriod: 1_234,
				storageBytes: 12 * 1024 * 1024,
				apiCalls: 567,
			},
		};
	},
};

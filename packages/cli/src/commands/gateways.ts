import { loadConfig } from "../config";
import { request, requireToken, requireUrl } from "../http";
import { fatal, print, printTable } from "../output";

interface Gateway {
	id: string;
	orgId: string;
	name: string;
	region?: string;
	status: string;
	createdAt: string;
}

/**
 * Resolve the org ID from flags, config, or die.
 */
function requireOrgId(flags: Record<string, string>, orgId?: string): string {
	const id = flags.org ?? orgId;
	if (!id) {
		fatal("--org is required (or set orgId in ~/.lakesync/config.json)");
	}
	return id;
}

/**
 * `lakesync gateways list` — List all gateways for the organisation.
 */
export async function gatewaysList(flags: Record<string, string>): Promise<void> {
	const config = loadConfig();
	const url = requireUrl(config);
	const token = requireToken(config);
	const orgId = requireOrgId(flags, config.orgId);

	const response = await request(url, token, `/v1/orgs/${encodeURIComponent(orgId)}/gateways`);

	if (!response.ok) {
		const text = await response.text().catch(() => "Unknown error");
		fatal(`Failed to list gateways (${response.status}): ${text}`);
	}

	const gateways = (await response.json()) as Gateway[];

	if (gateways.length === 0) {
		print("No gateways found.");
		return;
	}

	printTable(
		gateways.map((gw) => ({
			id: gw.id,
			name: gw.name,
			status: gw.status,
			region: gw.region ?? "-",
		})),
	);
}

/**
 * `lakesync gateways create` — Create a new gateway.
 */
export async function gatewaysCreate(flags: Record<string, string>): Promise<void> {
	const config = loadConfig();
	const url = requireUrl(config);
	const token = requireToken(config);
	const orgId = requireOrgId(flags, config.orgId);

	const name = flags.name;
	if (!name) {
		fatal("--name is required");
	}

	const body: { orgId: string; name: string; region?: string } = { orgId, name };
	if (flags.region) {
		body.region = flags.region;
	}

	const response = await request(url, token, "/v1/gateways", {
		method: "POST",
		body,
	});

	if (!response.ok) {
		const text = await response.text().catch(() => "Unknown error");
		fatal(`Failed to create gateway (${response.status}): ${text}`);
	}

	const gateway = (await response.json()) as Gateway;
	print(`Created gateway: ${gateway.id}`);
	print(`  Name:   ${gateway.name}`);
	print(`  Status: ${gateway.status}`);
	if (gateway.region) print(`  Region: ${gateway.region}`);
}

/**
 * `lakesync gateways delete` — Delete a gateway (soft-delete).
 */
export async function gatewaysDelete(flags: Record<string, string>): Promise<void> {
	const config = loadConfig();
	const url = requireUrl(config);
	const token = requireToken(config);

	const gatewayId = flags.id ?? flags.gateway;
	if (!gatewayId) {
		fatal("--id is required (gateway ID to delete)");
	}

	const response = await request(url, token, `/v1/gateways/${encodeURIComponent(gatewayId)}`, {
		method: "DELETE",
	});

	if (!response.ok) {
		const text = await response.text().catch(() => "Unknown error");
		fatal(`Failed to delete gateway (${response.status}): ${text}`);
	}

	print(`Deleted gateway: ${gatewayId}`);
}

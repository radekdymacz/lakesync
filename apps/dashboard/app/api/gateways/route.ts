import { type NextRequest, NextResponse } from "next/server";
import { DEV_ORG_ID } from "@/lib/auth-config";
import { createBackend, resolveOrgId } from "@/lib/backend";

const backend = createBackend();

export async function GET(request: NextRequest) {
	const orgId = resolveOrgId(request.nextUrl.searchParams.get("orgId"));
	if (!orgId) {
		return NextResponse.json({ error: "Missing orgId" }, { status: 400 });
	}

	const data = await backend.gateways.list(orgId);
	return NextResponse.json(data);
}

export async function POST(request: NextRequest) {
	const body = await request.json();
	const gw = await backend.gateways.create({
		orgId: body.orgId ?? DEV_ORG_ID,
		name: body.name,
		region: body.region,
	});
	return NextResponse.json(gw, { status: 201 });
}

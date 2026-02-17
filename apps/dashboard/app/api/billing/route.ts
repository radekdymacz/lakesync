import { type NextRequest, NextResponse } from "next/server";
import { createBackend, resolveOrgId } from "@/lib/backend";

const backend = createBackend();

export async function GET(request: NextRequest) {
	const orgId = resolveOrgId(request.nextUrl.searchParams.get("orgId"));
	if (!orgId) {
		return NextResponse.json({ error: "Missing orgId" }, { status: 400 });
	}

	const data = await backend.billing.get(orgId);
	return NextResponse.json(data);
}

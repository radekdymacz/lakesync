import { type NextRequest, NextResponse } from "next/server";
import { createBackend, resolveOrgId } from "@/lib/backend";

const backend = createBackend();

export async function GET(request: NextRequest) {
	const orgId = resolveOrgId(request.nextUrl.searchParams.get("orgId"));
	if (!orgId) {
		return NextResponse.json({ error: "Missing orgId" }, { status: 400 });
	}

	const now = new Date();
	const from =
		request.nextUrl.searchParams.get("from") ??
		new Date(now.getTime() - 30 * 86_400_000).toISOString();
	const to = request.nextUrl.searchParams.get("to") ?? now.toISOString();

	const data = await backend.usage.get(orgId, from, to);
	return NextResponse.json(data);
}

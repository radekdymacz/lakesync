import { type NextRequest, NextResponse } from "next/server";
import { createBackend } from "@/lib/backend";

const backend = createBackend();

export async function POST(request: NextRequest) {
	const { orgId } = (await request.json()) as { orgId: string };

	if (!orgId) {
		return NextResponse.json({ error: "Missing orgId" }, { status: 400 });
	}

	const data = await backend.billing.portal(orgId);
	if (data.message && !data.url) {
		return NextResponse.json(data, { status: 400 });
	}
	return NextResponse.json(data);
}

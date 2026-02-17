import { type NextRequest, NextResponse } from "next/server";
import { createBackend } from "@/lib/backend";

const backend = createBackend();

export async function POST(request: NextRequest) {
	const { orgId, planId } = (await request.json()) as {
		orgId: string;
		planId: string;
	};

	if (!orgId || !planId) {
		return NextResponse.json({ error: "Missing orgId or planId" }, { status: 400 });
	}

	const data = await backend.billing.checkout(orgId, planId);
	if (data.message && !data.url) {
		return NextResponse.json(data, { status: 400 });
	}
	return NextResponse.json(data);
}

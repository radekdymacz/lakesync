import { NextResponse } from "next/server";
import { authedHandler, backend } from "@/lib/api-handler";

const VALID_PLAN_IDS = ["free", "starter", "pro", "enterprise"] as const;

export const POST = authedHandler(async (orgId, request) => {
	const body = (await request.json()) as Record<string, unknown>;
	const planId = body.planId;

	if (
		typeof planId !== "string" ||
		!VALID_PLAN_IDS.includes(planId as (typeof VALID_PLAN_IDS)[number])
	) {
		return NextResponse.json({ error: "Invalid planId" }, { status: 400 });
	}

	const data = await backend.billing.checkout(orgId, planId);
	if (data.message && !data.url) {
		return NextResponse.json(data, { status: 400 });
	}
	return NextResponse.json(data);
});

import { NextResponse } from "next/server";
import { authedHandler, backend } from "@/lib/api-handler";

export const POST = authedHandler(async (orgId) => {
	const data = await backend.billing.portal(orgId);
	if (data.message && !data.url) {
		return NextResponse.json(data, { status: 400 });
	}
	return NextResponse.json(data);
});

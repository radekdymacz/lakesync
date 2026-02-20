import { NextResponse } from "next/server";
import { authedHandler, backend } from "@/lib/api-handler";

export const GET = authedHandler(async (orgId) => {
	const data = await backend.billing.get(orgId);
	return NextResponse.json(data);
});

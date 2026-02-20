import { NextResponse } from "next/server";
import { authedHandler, backend } from "@/lib/api-handler";

export const GET = authedHandler(async (orgId, request) => {
	const now = new Date();
	const from =
		request.nextUrl.searchParams.get("from") ??
		new Date(now.getTime() - 30 * 86_400_000).toISOString();
	const to = request.nextUrl.searchParams.get("to") ?? now.toISOString();

	const data = await backend.usage.get(orgId, from, to);
	return NextResponse.json(data);
});

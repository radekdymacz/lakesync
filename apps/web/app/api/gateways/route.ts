import { NextResponse } from "next/server";
import { authedHandler, backend } from "@/lib/api-handler";

export const GET = authedHandler(async (orgId) => {
	const data = await backend.gateways.list(orgId);
	return NextResponse.json(data);
});

export const POST = authedHandler(async (orgId, request) => {
	const body = await request.json();
	const gw = await backend.gateways.create({
		orgId,
		name: body.name,
		region: body.region,
	});
	return NextResponse.json(gw, { status: 201 });
});

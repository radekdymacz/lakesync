import { NextResponse } from "next/server";
import { authedHandler, backend } from "@/lib/api-handler";

export const GET = authedHandler<{ id: string }>(async (_orgId, _request, context) => {
	const { id } = await context.params;
	const gw = await backend.gateways.get(id);
	if (!gw) return NextResponse.json({ error: "Not found" }, { status: 404 });
	return NextResponse.json(gw);
});

export const PATCH = authedHandler<{ id: string }>(async (_orgId, request, context) => {
	const { id } = await context.params;
	const body = (await request.json()) as Record<string, unknown>;

	// Validate — only name (string) and status (string) are accepted
	const name = body.name;
	const status = body.status;
	if (name !== undefined && typeof name !== "string") {
		return NextResponse.json({ error: "name must be a string" }, { status: 400 });
	}
	if (status !== undefined && typeof status !== "string") {
		return NextResponse.json({ error: "status must be a string" }, { status: 400 });
	}
	if (name === undefined && status === undefined) {
		return NextResponse.json({ error: "No valid fields to update" }, { status: 400 });
	}

	const patch: { name?: string; status?: string } = {};
	if (name !== undefined) patch.name = name as string;
	if (status !== undefined) patch.status = status as string;

	const gw = await backend.gateways.update(id, patch);
	if (!gw) return NextResponse.json({ error: "Not found" }, { status: 404 });
	return NextResponse.json(gw);
});

export const DELETE = authedHandler<{ id: string }>(async (_orgId, _request, context) => {
	const { id } = await context.params;
	await backend.gateways.delete(id);
	return new Response(null, { status: 204 });
});

import { type NextRequest, NextResponse } from "next/server";
import { createBackend } from "@/lib/backend";

const backend = createBackend();

export async function GET(_request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
	const { id } = await params;
	const gw = await backend.gateways.get(id);
	if (!gw) {
		return NextResponse.json({ error: "Not found" }, { status: 404 });
	}
	return NextResponse.json(gw);
}

export async function PATCH(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
	const { id } = await params;
	const body = await request.json();
	const gw = await backend.gateways.update(id, body);
	if (!gw) {
		return NextResponse.json({ error: "Not found" }, { status: 404 });
	}
	return NextResponse.json(gw);
}

export async function DELETE(
	_request: NextRequest,
	{ params }: { params: Promise<{ id: string }> },
) {
	const { id } = await params;
	await backend.gateways.delete(id);
	return new NextResponse(null, { status: 204 });
}

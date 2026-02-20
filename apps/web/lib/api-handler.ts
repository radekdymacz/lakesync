import { type NextRequest, NextResponse } from "next/server";
import { serverAuth } from "@/lib/auth-server";
import { createBackend } from "@/lib/backend";

/** Shared backend singleton — created once at module load. */
export const backend = createBackend();

type RouteContext<P = Record<string, string>> = { params: Promise<P> };

type HandlerFn<P = Record<string, string>> = (
	orgId: string,
	request: NextRequest,
	context: RouteContext<P>,
) => Promise<Response>;

/**
 * Wraps a route handler with auth check.
 * Returns 401 JSON if no orgId, otherwise calls fn with the authenticated orgId.
 */
export function authedHandler<P = Record<string, string>>(fn: HandlerFn<P>) {
	return async (request: NextRequest, context: RouteContext<P>): Promise<Response> => {
		const { orgId } = await serverAuth();
		if (!orgId) {
			return NextResponse.json({ error: "Unauthorised" }, { status: 401 });
		}
		return fn(orgId, request, context);
	};
}

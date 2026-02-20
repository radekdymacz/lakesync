import { type NextRequest, NextResponse } from "next/server";
import { CLERK_ENABLED } from "@/lib/auth-config";

async function handleRequest(request: NextRequest): Promise<NextResponse> {
	const { pathname } = request.nextUrl;

	// Public routes — no auth required
	if (pathname === "/" || pathname.startsWith("/docs")) {
		return NextResponse.next();
	}

	if (!CLERK_ENABLED) {
		// Dev mode — no auth, redirect sign-in/sign-up to dashboard
		if (pathname.startsWith("/sign-in") || pathname.startsWith("/sign-up")) {
			return NextResponse.redirect(new URL("/dashboard", request.url));
		}
		return NextResponse.next();
	}

	// Production — delegate to Clerk middleware
	const { clerkMiddleware, createRouteMatcher } = await import("@clerk/nextjs/server");

	const isPublicRoute = createRouteMatcher(["/", "/docs(.*)", "/sign-in(.*)", "/sign-up(.*)"]);

	const middleware = clerkMiddleware(async (auth, req) => {
		if (!isPublicRoute(req)) {
			await auth.protect();
		}
	});

	return middleware(request, {} as never) as unknown as Promise<NextResponse>;
}

export default handleRequest;

export const config = {
	matcher: [
		"/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)",
		"/(api|trpc)(.*)",
	],
};

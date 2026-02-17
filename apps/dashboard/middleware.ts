import { type NextRequest, NextResponse } from "next/server";

const clerkEnabled = !!process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY;

async function handleRequest(request: NextRequest): Promise<NextResponse> {
	if (!clerkEnabled) {
		// Dev mode — no auth, redirect sign-in/sign-up to dashboard
		const { pathname } = request.nextUrl;
		if (pathname.startsWith("/sign-in") || pathname.startsWith("/sign-up")) {
			return NextResponse.redirect(new URL("/dashboard", request.url));
		}
		return NextResponse.next();
	}

	// Production — delegate to Clerk middleware
	const { clerkMiddleware, createRouteMatcher } = await import("@clerk/nextjs/server");

	const isPublicRoute = createRouteMatcher(["/sign-in(.*)", "/sign-up(.*)"]);

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

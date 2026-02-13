// ---------------------------------------------------------------------------
// Middleware Pipeline — composable request processing
// ---------------------------------------------------------------------------

import type { IncomingMessage, ServerResponse } from "node:http";
import type { AuthClaims } from "./auth";
import type { Logger } from "./logger";
import type { RouteMatch } from "./router";

// ---------------------------------------------------------------------------
// Request context — accumulated state passed through the pipeline
// ---------------------------------------------------------------------------

/** Mutable context accumulated as middleware runs. */
export interface RequestContext {
	readonly req: IncomingMessage;
	readonly res: ServerResponse;
	readonly method: string;
	readonly url: URL;
	readonly pathname: string;
	readonly requestId: string;
	readonly logger: Logger;
	/** CORS headers to include on every response. */
	corsHeaders: Record<string, string>;
	/** Matched route (set by route-matching middleware). */
	route?: RouteMatch;
	/** Authenticated claims (set by auth middleware, undefined when auth disabled). */
	auth?: AuthClaims;
}

// ---------------------------------------------------------------------------
// Middleware type
// ---------------------------------------------------------------------------

/** A middleware function. Call `next()` to continue the pipeline. */
export type Middleware = (ctx: RequestContext, next: () => Promise<void>) => Promise<void>;

// ---------------------------------------------------------------------------
// Pipeline runner
// ---------------------------------------------------------------------------

/**
 * Execute a middleware pipeline.
 *
 * Each middleware receives the shared context and a `next` function.
 * Calling `next()` invokes the subsequent middleware. If a middleware
 * does not call `next()`, the pipeline stops (short-circuit).
 */
export function runPipeline(middlewares: Middleware[], ctx: RequestContext): Promise<void> {
	let index = 0;
	const next = async (): Promise<void> => {
		if (index >= middlewares.length) return;
		const mw = middlewares[index]!;
		index++;
		await mw(ctx, next);
	};
	return next();
}

// ---------------------------------------------------------------------------
// Route handler type (used by the data-driven dispatch map)
// ---------------------------------------------------------------------------

/** A route handler receives the request context after auth + rate limiting. */
export type RouteHandler = (ctx: RequestContext) => Promise<void> | void;

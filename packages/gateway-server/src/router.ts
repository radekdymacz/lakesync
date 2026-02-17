// ---------------------------------------------------------------------------
// Router — URL pattern matching and route dispatch
// ---------------------------------------------------------------------------

/** Matched route information. */
export interface RouteMatch {
	gatewayId: string;
	action: string;
	/** Extra route parameters (e.g. connector name from DELETE path). */
	connectorName?: string;
}

/** Route definition: [method, pattern, action, captureConnectorName?] */
type RouteEntry = [string, RegExp, string, boolean?];

/** Route definitions for the gateway server. */
const ROUTES: ReadonlyArray<RouteEntry> = [
	["POST", /^\/v1\/sync\/([^/]+)\/push$/, "push"],
	["GET", /^\/v1\/sync\/([^/]+)\/pull$/, "pull"],
	["POST", /^\/v1\/sync\/([^/]+)\/action$/, "action"],
	["GET", /^\/v1\/sync\/([^/]+)\/actions$/, "describe-actions"],
	["GET", /^\/v1\/sync\/([^/]+)\/ws$/, "ws"],
	["POST", /^\/v1\/admin\/flush\/([^/]+)$/, "flush"],
	["POST", /^\/v1\/admin\/schema\/([^/]+)$/, "schema"],
	["POST", /^\/v1\/admin\/sync-rules\/([^/]+)$/, "sync-rules"],
	["POST", /^\/v1\/admin\/connectors\/([^/]+)$/, "register-connector"],
	["GET", /^\/v1\/admin\/connectors\/([^/]+)$/, "list-connectors"],
	["DELETE", /^\/v1\/admin\/connectors\/([^/]+)\/([^/]+)$/, "unregister-connector", true],
	["GET", /^\/v1\/admin\/metrics\/([^/]+)$/, "metrics"],
];

/**
 * Legacy route patterns — match unversioned paths for 301 redirect.
 * Returns the equivalent `/v1/` path if the path is a legacy route, or null.
 */
export function matchLegacyRoute(pathname: string): string | null {
	if (
		pathname.startsWith("/sync/") ||
		pathname.startsWith("/admin/") ||
		pathname === "/connectors/types"
	) {
		return `/v1${pathname}`;
	}
	return null;
}

/**
 * Match a request pathname and method against the route table.
 *
 * Returns the matched route or null if no route matches.
 */
export function matchRoute(pathname: string, method: string): RouteMatch | null {
	for (const [m, pattern, action, hasConnector] of ROUTES) {
		if (method !== m) continue;
		const match = pathname.match(pattern);
		if (!match) continue;
		return {
			gatewayId: match[1]!,
			action,
			...(hasConnector ? { connectorName: match[2]! } : {}),
		};
	}
	return null;
}

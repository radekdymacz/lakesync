import { onRequestOptions as __api_waitlist_ts_onRequestOptions } from "/Users/radek/Documents/Github/lakesync/apps/landing/functions/api/waitlist.ts"
import { onRequestPost as __api_waitlist_ts_onRequestPost } from "/Users/radek/Documents/Github/lakesync/apps/landing/functions/api/waitlist.ts"

export const routes = [
    {
      routePath: "/api/waitlist",
      mountPath: "/api",
      method: "OPTIONS",
      middlewares: [],
      modules: [__api_waitlist_ts_onRequestOptions],
    },
  {
      routePath: "/api/waitlist",
      mountPath: "/api",
      method: "POST",
      middlewares: [],
      modules: [__api_waitlist_ts_onRequestPost],
    },
  ]
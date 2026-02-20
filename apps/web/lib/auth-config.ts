/** Whether Clerk is configured. When false, dev stubs are used instead. */
export const CLERK_ENABLED = !!process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY;

export const DEV_ORG_ID = "dev-org-1";
export const DEV_USER_ID = "dev-user-1";

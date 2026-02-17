// ---------------------------------------------------------------------------
// Stripe SDK type interfaces — injectable, never a real dependency
// ---------------------------------------------------------------------------
// These mirror the Stripe SDK's API surface so the actual stripe package
// is injected at runtime rather than being a hard dependency.

/** Stripe customer object (subset of Stripe.Customer) */
export interface StripeCustomer {
	readonly id: string;
	readonly email: string | null;
	readonly name: string | null;
	readonly metadata: Record<string, string>;
}

/** Stripe subscription status */
export type StripeSubscriptionStatus =
	| "active"
	| "canceled"
	| "incomplete"
	| "incomplete_expired"
	| "past_due"
	| "paused"
	| "trialing"
	| "unpaid";

/** Stripe subscription object (subset of Stripe.Subscription) */
export interface StripeSubscription {
	readonly id: string;
	readonly customer: string;
	readonly status: StripeSubscriptionStatus;
	readonly items: {
		readonly data: ReadonlyArray<{
			readonly id: string;
			readonly price: { readonly id: string };
		}>;
	};
	readonly current_period_end: number;
	readonly cancel_at_period_end: boolean;
}

/** Stripe checkout session (subset of Stripe.Checkout.Session) */
export interface StripeCheckoutSession {
	readonly id: string;
	readonly url: string | null;
}

/** Stripe billing portal session (subset of Stripe.BillingPortal.Session) */
export interface StripeBillingPortalSession {
	readonly id: string;
	readonly url: string;
}

/** Stripe invoice (subset of Stripe.Invoice) */
export interface StripeInvoice {
	readonly id: string;
	readonly customer: string;
	readonly subscription: string | null;
	readonly status: string | null;
	readonly amount_due: number;
	readonly amount_paid: number;
	readonly period_start: number;
	readonly period_end: number;
}

/** Stripe usage record (subset of Stripe.UsageRecord) */
export interface StripeUsageRecord {
	readonly id: string;
	readonly quantity: number;
	readonly timestamp: number;
	readonly subscription_item: string;
}

/**
 * Injectable Stripe client interface.
 *
 * Mirrors the subset of the Stripe SDK that LakeSync uses. The actual
 * `stripe` package is injected at runtime — this interface allows full
 * testing without Stripe dependencies.
 */
export interface StripeClient {
	customers: {
		create(params: {
			email?: string;
			name?: string;
			metadata?: Record<string, string>;
		}): Promise<StripeCustomer>;
		retrieve(id: string): Promise<StripeCustomer>;
	};

	subscriptions: {
		create(params: {
			customer: string;
			items: Array<{ price: string }>;
			metadata?: Record<string, string>;
		}): Promise<StripeSubscription>;
		retrieve(id: string): Promise<StripeSubscription>;
		update(
			id: string,
			params: {
				items?: Array<{ id: string; price: string }>;
				proration_behavior?: "create_prorations" | "none" | "always_invoice";
				cancel_at_period_end?: boolean;
				metadata?: Record<string, string>;
			},
		): Promise<StripeSubscription>;
		cancel(id: string): Promise<StripeSubscription>;
	};

	checkout: {
		sessions: {
			create(params: {
				customer: string;
				mode: "subscription" | "payment";
				line_items: Array<{ price: string; quantity: number }>;
				success_url: string;
				cancel_url: string;
				metadata?: Record<string, string>;
			}): Promise<StripeCheckoutSession>;
		};
	};

	billingPortal: {
		sessions: {
			create(params: { customer: string; return_url: string }): Promise<StripeBillingPortalSession>;
		};
	};

	subscriptionItems: {
		createUsageRecord(
			subscriptionItemId: string,
			params: {
				quantity: number;
				timestamp: number;
				action: "set" | "increment";
			},
		): Promise<StripeUsageRecord>;
	};

	webhooks: {
		constructEvent(payload: string, signature: string, secret: string): StripeWebhookEvent;
	};
}

/** Stripe webhook event (subset of Stripe.Event) */
export interface StripeWebhookEvent {
	readonly id: string;
	readonly type: string;
	readonly data: {
		readonly object: Record<string, unknown>;
	};
}

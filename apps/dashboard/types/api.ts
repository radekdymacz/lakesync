import type { PlanId } from "@lakesync/control-plane";

export interface UsageDayData {
	date: string;
	pushDeltas: number;
	pullDeltas: number;
	apiCalls: number;
	storageBytes: number;
}

export interface UsageData {
	data: UsageDayData[];
	totals: {
		pushDeltas: number;
		pullDeltas: number;
		apiCalls: number;
		storageBytes: number;
	};
}

export interface BillingData {
	plan: PlanId;
	planName: string;
	price: number;
	maxDeltasPerMonth: number;
	maxStorageBytes: number;
	currentPeriodEnd?: number;
	cancelAtPeriodEnd?: boolean;
	usage: {
		deltasThisPeriod: number;
		storageBytes: number;
		apiCalls: number;
	};
}

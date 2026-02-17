"use client";

/** A simple progress bar showing current vs limit */
export function UsageBar({
	label,
	current,
	limit,
	formatter,
}: {
	label: string;
	current: number;
	limit: number;
	formatter?: (n: number) => string;
}) {
	const fmt = formatter ?? ((n: number) => n.toLocaleString());
	const isUnlimited = limit === -1;
	const percentage = isUnlimited ? 0 : limit === 0 ? 100 : Math.min((current / limit) * 100, 100);
	const isOverLimit = !isUnlimited && current > limit;

	return (
		<div>
			<div className="flex items-center justify-between text-sm">
				<span className="font-medium text-gray-700">{label}</span>
				<span className="text-gray-500">
					{fmt(current)} / {isUnlimited ? "Unlimited" : fmt(limit)}
				</span>
			</div>
			<div className="mt-1.5 h-2 w-full rounded-full bg-gray-100">
				<div
					className={`h-2 rounded-full transition-all ${
						isOverLimit ? "bg-red-500" : percentage > 80 ? "bg-amber-500" : "bg-green-500"
					}`}
					style={{ width: isUnlimited ? "0%" : `${percentage}%` }}
				/>
			</div>
		</div>
	);
}

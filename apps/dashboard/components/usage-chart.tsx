"use client";

/** A simple bar chart using CSS (no charting library dependency) */
export function UsageChart({
	title,
	data,
	colour,
}: {
	title: string;
	data: Array<{ label: string; value: number }>;
	colour?: string;
}) {
	const maxVal = Math.max(...data.map((d) => d.value), 1);
	const barColour = colour ?? "bg-blue-500";

	return (
		<div className="rounded-lg border border-gray-200 bg-white p-6">
			<h3 className="mb-4 text-sm font-medium text-gray-500">{title}</h3>
			{data.length === 0 ? (
				<div className="flex h-48 items-center justify-center text-gray-400">
					No data for this period
				</div>
			) : (
				<div className="flex h-48 items-end gap-1">
					{data.map((d) => {
						const height = (d.value / maxVal) * 100;
						return (
							<div key={d.label} className="group relative flex flex-1 flex-col items-center">
								<div className="absolute bottom-6 left-1/2 z-10 hidden -translate-x-1/2 rounded bg-gray-900 px-2 py-1 text-xs text-white group-hover:block">
									{d.value.toLocaleString()}
								</div>
								<div
									className={`w-full rounded-t ${barColour} min-h-[2px]`}
									style={{ height: `${height}%` }}
								/>
								<span className="mt-1 text-[10px] text-gray-400">{d.label}</span>
							</div>
						);
					})}
				</div>
			)}
		</div>
	);
}

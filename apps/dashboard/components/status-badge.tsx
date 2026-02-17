import type { GatewayStatus } from "@lakesync/control-plane";

const styles: Record<GatewayStatus, string> = {
	active: "bg-green-100 text-green-800",
	suspended: "bg-yellow-100 text-yellow-800",
	deleted: "bg-red-100 text-red-800",
};

export function StatusBadge({ status }: { status: GatewayStatus }) {
	return (
		<span
			className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${styles[status]}`}
		>
			{status}
		</span>
	);
}

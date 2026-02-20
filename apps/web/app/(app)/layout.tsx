import { OrgGuard } from "@/components/org-guard";
import { Sidebar } from "@/components/sidebar";

export default function AppLayout({ children }: { children: React.ReactNode }) {
	return (
		<div className="flex h-screen bg-gray-50 text-gray-900">
			<Sidebar />
			<main className="flex-1 overflow-y-auto p-8">
				<OrgGuard>{children}</OrgGuard>
			</main>
		</div>
	);
}

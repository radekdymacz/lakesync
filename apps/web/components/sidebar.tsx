"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { getIdentityWidgets } from "@/lib/identity-widgets";

const navItems = [
	{ href: "/dashboard", label: "Overview" },
	{ href: "/gateways", label: "Gateways" },
	{ href: "/api-keys", label: "API Keys" },
	{ href: "/usage", label: "Usage" },
];

const { OrgSwitcher, UserButton } = getIdentityWidgets();

export function Sidebar() {
	const pathname = usePathname();

	return (
		<aside className="flex h-screen w-64 flex-col border-r border-gray-200 bg-white">
			<div className="border-b border-gray-200 p-4">
				<h1 className="mb-3 text-lg font-semibold">LakeSync</h1>
				<OrgSwitcher />
			</div>

			<nav className="flex-1 space-y-1 p-3">
				{navItems.map((item) => {
					const isActive = pathname.startsWith(item.href);
					return (
						<Link
							key={item.href}
							href={item.href}
							className={`block rounded-md px-3 py-2 text-sm font-medium ${
								isActive
									? "bg-gray-100 text-gray-900"
									: "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
							}`}
						>
							{item.label}
						</Link>
					);
				})}
				<Link
					href="/docs"
					className="mt-2 block rounded-md px-3 py-2 text-sm font-medium text-gray-600 hover:bg-gray-50 hover:text-gray-900"
				>
					Docs
				</Link>
			</nav>
			<div className="border-t border-gray-200 p-4">
				<UserButton />
			</div>
		</aside>
	);
}

"use client";

import { useTheme } from "next-themes";
import { useEffect, useState } from "react";

export function TopBar() {
	const { resolvedTheme, setTheme } = useTheme();
	const [mounted, setMounted] = useState(false);

	useEffect(() => setMounted(true), []);

	return (
		<header className="sticky top-0 z-[60] border-b border-fd-border bg-fd-background/85 backdrop-blur-md">
			<div className="mx-auto flex h-14 max-w-5xl items-center justify-between px-6">
				{/* Logo */}
				<a href="/" className="flex items-center gap-2.5">
					<div className="flex h-7 w-7 items-center justify-center rounded-md bg-fd-foreground">
						<span className="text-xs font-bold tracking-tight text-fd-background">
							LS
						</span>
					</div>
					<span className="text-[15px] font-semibold">LakeSync</span>
				</a>

				{/* Links */}
				<div className="hidden items-center gap-6 text-sm text-fd-muted-foreground sm:flex">
					<a
						href="/#features"
						className="transition-colors hover:text-fd-foreground"
					>
						Features
					</a>
					<a
						href="/#use-cases"
						className="transition-colors hover:text-fd-foreground"
					>
						Use Cases
					</a>
					<a
						href="/docs"
						className="text-fd-foreground transition-colors"
					>
						Docs
					</a>
				</div>

				{/* Right */}
				<div className="flex items-center gap-3">
					{mounted && (
						<button
							type="button"
							onClick={() =>
								setTheme(
									resolvedTheme === "dark" ? "light" : "dark",
								)
							}
							className="flex h-8 w-8 items-center justify-center rounded-md text-fd-muted-foreground transition-colors hover:bg-fd-accent hover:text-fd-foreground"
						>
							{resolvedTheme === "dark" ? (
								<svg
									className="h-4 w-4"
									fill="none"
									stroke="currentColor"
									strokeWidth="2"
									viewBox="0 0 24 24"
								>
									<circle cx="12" cy="12" r="5" />
									<path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42" />
								</svg>
							) : (
								<svg
									className="h-4 w-4"
									fill="none"
									stroke="currentColor"
									strokeWidth="2"
									viewBox="0 0 24 24"
								>
									<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
								</svg>
							)}
						</button>
					)}
					<a
						href="/#waitlist"
						className="hidden h-8 items-center rounded-md bg-fd-foreground px-3.5 text-sm font-medium text-fd-background transition-opacity hover:opacity-90 sm:inline-flex"
					>
						Get Started
					</a>
				</div>
			</div>
		</header>
	);
}

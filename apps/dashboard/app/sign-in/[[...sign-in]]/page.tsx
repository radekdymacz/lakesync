"use client";

function ClerkSignIn() {
	// eslint-disable-next-line @typescript-eslint/no-require-imports
	const { SignIn } = require("@clerk/nextjs") as typeof import("@clerk/nextjs");

	return (
		<div className="flex min-h-screen items-center justify-center">
			<SignIn afterSignInUrl="/dashboard" />
		</div>
	);
}

export default function SignInPage() {
	return <ClerkSignIn />;
}

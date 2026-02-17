"use client";

function ClerkSignUp() {
	// eslint-disable-next-line @typescript-eslint/no-require-imports
	const { SignUp } = require("@clerk/nextjs") as typeof import("@clerk/nextjs");

	return (
		<div className="flex min-h-screen items-center justify-center">
			<SignUp afterSignUpUrl="/dashboard" />
		</div>
	);
}

export default function SignUpPage() {
	return <ClerkSignUp />;
}

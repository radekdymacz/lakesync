import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";

export const baseOptions: BaseLayoutProps = {
	nav: {
		enabled: true,
		title: "LakeSync",
	},
	links: [
		{
			text: "Dashboard",
			url: "/dashboard",
		},
	],
	githubUrl: "https://github.com/radekdymacz/lakesync",
};

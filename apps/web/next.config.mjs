import { createMDX } from "fumadocs-mdx/next";

const withMDX = createMDX();

/** @type {import('next').NextConfig} */
const config = {
	transpilePackages: ["@lakesync/control-plane", "@lakesync/core"],
	images: { unoptimized: true },
};

export default withMDX(config);

const nextConfig = {
  output: "export",
  trailingSlash: true,
  basePath: process.env.NEXT_PUBLIC_BASE_PATH || "",
  turbopack: { root: process.cwd() },
  images: { unoptimized: true },
  poweredByHeader: false,
  devIndicators: false,
};

export default nextConfig;

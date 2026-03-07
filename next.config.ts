import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: ["@databricks/sql", "lz4"],
};

export default nextConfig;

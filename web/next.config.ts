import type { NextConfig } from "next";
import path from "node:path";

// Security headers that do not depend on the request. The Content-Security-Policy needs a
// per-request nonce, so it is set in src/middleware.ts.
const securityHeaders = [
  { key: "Strict-Transport-Security", value: "max-age=63072000; includeSubDomains; preload" },
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
  { key: "X-Frame-Options", value: "DENY" },
  {
    key: "Permissions-Policy",
    value: "camera=(), microphone=(), geolocation=(), payment=(), usb=(), interest-cohort=()",
  },
  { key: "Cross-Origin-Opener-Policy", value: "same-origin" },
];

const nextConfig: NextConfig = {
  // S17: never ship browser source maps.
  productionBrowserSourceMaps: false,
  poweredByHeader: false,
  // The site is its own workspace; a lockfile further up the disk is not its root.
  outputFileTracingRoot: path.resolve(__dirname),
  reactStrictMode: true,
  async headers() {
    return [{ source: "/:path*", headers: securityHeaders }];
  },
};

export default nextConfig;

import type { Metadata, Viewport } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import { MotionProvider } from "@/components/MotionProvider";
import "./globals.css";

const sans = Geist({ subsets: ["latin"], variable: "--font-geist-sans", display: "swap" });
const mono = Geist_Mono({ subsets: ["latin"], variable: "--font-geist-mono", display: "swap" });

// Every page is rendered per request: the CSP in middleware.ts allows only scripts carrying that
// request's nonce, which a page prerendered at build time could never have.
export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "LogForge: a distributed commit log, built from first principles",
  description:
    "A Kafka-style distributed append-only log written from scratch in Java 21, with a live cluster view, a recorded crash replay and measured benchmarks against Apache Kafka.",
  robots: { index: true, follow: true },
};

export const viewport: Viewport = {
  themeColor: [
    { media: "(prefers-color-scheme: dark)", color: "#0a0d10" },
    { media: "(prefers-color-scheme: light)", color: "#f6f7f8" },
  ],
  width: "device-width",
  initialScale: 1,
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${sans.variable} ${mono.variable}`}>
      <body>
        <a href="#main" className="skip-link">
          Skip to content
        </a>
        <MotionProvider>{children}</MotionProvider>
      </body>
    </html>
  );
}

import type { Metadata, Viewport } from "next";
import type { ReactNode } from "react";
import { GeistSans } from "geist/font/sans";
import { GeistMono } from "geist/font/mono";
import "./globals.css";

export const metadata: Metadata = {
  title: "IQ ANALYZER & CRYPTO STREAMING SUITE",
  description:
    "Consola AI text-to-SQL y suite de streaming cripto con arquitectura Lambda: Qwen2.5, Kafka, Spark, MinIO y FastAPI.",
  applicationName: "IQ ANALYZER",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  themeColor: "#000000",
  colorScheme: "dark",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="es" className={`${GeistSans.variable} ${GeistMono.variable} dark`}>
      <body className="min-h-screen bg-black font-sans text-zinc-100 antialiased">{children}</body>
    </html>
  );
}

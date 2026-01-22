import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { ThemeProvider } from "@/components/theme-context";

const inter = Inter({ subsets: ["latin"] });

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  metadataBase: new URL(process.env.NEXTAUTH_URL || "http://localhost:3000"),
  title: "Quality Classifier - Sub ID Reclassification Tool",
  description: "Automated classification tool for traffic source quality analysis. Compare sub_ids against 2026 quality targets and generate reclassification recommendations.",
  icons: {
    icon: "/favicon.svg",
    shortcut: "/favicon.svg",
  },
  openGraph: {
    title: "Quality Classifier",
    description: "Automated Premium/Standard reclassification tool",
    images: ["/og-image.png"],
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script src="https://apps.abacus.ai/chatllm/appllm-lib.js" />
      </head>
      <body className={inter.className} suppressHydrationWarning>
        <ThemeProvider>
          {children}
        </ThemeProvider>
      </body>
    </html>
  );
}

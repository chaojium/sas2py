import type { Metadata } from "next";
import { Space_Grotesk, JetBrains_Mono } from "next/font/google";
import Providers from "@/app/providers";
import Header from "@/components/Header";
import "./globals.css";

const spaceGrotesk = Space_Grotesk({
  variable: "--font-display",
  subsets: ["latin"],
});

const jetBrainsMono = JetBrains_Mono({
  variable: "--font-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "SAS2Py Studio",
  description: "Convert SAS code to Python or R with GPT-5.2, review, and collaborate.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${spaceGrotesk.variable} ${jetBrainsMono.variable} antialiased`}
      >
        <Providers>
          <Header />
          {children}
          <footer className="border-t border-[var(--border)] bg-white/50 px-6 py-5 text-sm text-[var(--muted)] md:px-12">
            <div className="mx-auto flex w-full max-w-6xl flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
              <p>Created by Chao Ji, PhD</p>
              <a
                href="https://www.linkedin.com/in/chaojoyceji/"
                target="_blank"
                rel="noreferrer"
                className="font-medium text-[var(--foreground)] underline underline-offset-4 transition hover:text-[var(--secondary)]"
              >
                LinkedIn
              </a>
            </div>
          </footer>
        </Providers>
      </body>
    </html>
  );
}

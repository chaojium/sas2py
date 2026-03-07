"use client";

import { useSession } from "next-auth/react";
import AuthButton from "@/components/AuthButton";

export default function Hero() {
  const { status } = useSession();
  if (status === "authenticated") {
    return null;
  }

  return (
    <header className="fade-up flex flex-col gap-8 rounded-[32px] border border-[var(--border)] bg-white/70 p-8 shadow-[0_20px_80px_-55px_rgba(15,118,110,0.65)] backdrop-blur md:p-12">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="inline-flex items-center gap-3 rounded-full border border-[var(--border)] bg-white/80 px-4 py-2 text-xs uppercase tracking-[0.3em] text-[var(--muted)]">
          SAS2Py Studio
          <span className="h-1 w-8 rounded-full bg-[var(--primary)]" />
        </div>
        <AuthButton />
      </div>
      <div className="max-w-3xl space-y-4">
        <h1 className="text-4xl font-semibold leading-tight md:text-6xl">
          Transform SAS logic into clean Python, then review with context.
        </h1>
        {/* <p className="text-lg text-[var(--muted)]">
          GPT-5.2 handles the translation. You control the output, capture
          feedback, and keep everything stored in Postgres for traceability.
        </p> */}
      </div>
      <div className="flex flex-wrap gap-4 text-sm text-[var(--muted)]">
        <span className="rounded-full border border-[var(--border)] px-4 py-2">
          GPT-5.2-powered conversion
        </span>
        <span className="rounded-full border border-[var(--border)] px-4 py-2">
          Review notes + ratings
        </span>
        <span className="rounded-full border border-[var(--border)] px-4 py-2">
          Enhance with user instructions
        </span>
      </div>
    </header>
  );
}

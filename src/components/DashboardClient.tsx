"use client";

import { useEffect, useState } from "react";
import AuthButton from "@/components/AuthButton";
import { useAuth } from "@/components/AuthProvider";
import { authFetch } from "@/lib/firebase/auth-fetch";

type Summary = {
  conversions: number;
  reviews: number;
};

export default function DashboardClient() {
  const { status } = useAuth();
  const isAuthed = status === "authenticated";
  const [summary, setSummary] = useState<Summary | null>(null);

  useEffect(() => {
    if (!isAuthed) return;
    authFetch("/api/summary")
      .then((res) => res.json())
      .then((data) => setSummary(data))
      .catch(() => setSummary(null));
  }, [isAuthed]);

  if (!isAuthed) {
    return (
      <section className="glass-card fade-up rounded-3xl p-8 md:p-12">
        <div className="flex flex-col gap-6">
          <h2 className="text-2xl font-semibold">Sign in to view dashboard.</h2>
          <p className="text-[var(--muted)]">
            Track conversion volume and review notes here.
          </p>
          <AuthButton variant="primary" />
        </div>
      </section>
    );
  }

  return (
    <section className="glass-card fade-up rounded-3xl p-8 md:p-12">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs uppercase tracking-[0.3em] text-[var(--muted)]">
            Dashboard
          </p>
          <h1 className="text-3xl font-semibold">Overview</h1>
        </div>
      </div>
      <div className="mt-8 grid gap-6 md:grid-cols-2">
        <div className="rounded-2xl border border-[var(--border)] bg-white/80 p-6">
          <p className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Conversions
          </p>
          <p className="mt-3 text-4xl font-semibold">
            {summary ? summary.conversions : "--"}
          </p>
          <p className="mt-2 text-sm text-[var(--muted)]">
            Total SAS to Python or R conversions stored.
          </p>
        </div>
        <div className="rounded-2xl border border-[var(--border)] bg-white/80 p-6">
          <p className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Reviews
          </p>
          <p className="mt-3 text-4xl font-semibold">
            {summary ? summary.reviews : "--"}
          </p>
          <p className="mt-2 text-sm text-[var(--muted)]">
            Notes left on conversions.
          </p>
        </div>
      </div>
    </section>
  );
}

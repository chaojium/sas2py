"use client";

import AuthButton from "@/components/AuthButton";
import { useAuth } from "@/components/AuthProvider";

export default function SettingsClient() {
  const { user, status } = useAuth();
  const isAuthed = status === "authenticated";

  if (!isAuthed) {
    return (
      <section className="glass-card fade-up rounded-3xl p-8 md:p-12">
        <div className="flex flex-col gap-6">
          <h2 className="text-2xl font-semibold">Sign in to manage settings.</h2>
          <p className="text-[var(--muted)]">
            Update profile settings once you are authenticated.
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
            Settings
          </p>
          <h1 className="text-3xl font-semibold">Profile</h1>
        </div>
        <AuthButton />
      </div>
      <div className="mt-8 grid gap-6 md:grid-cols-2">
        <div className="rounded-2xl border border-[var(--border)] bg-white/80 p-6">
          <p className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Name
          </p>
          <p className="mt-3 text-lg font-semibold">
            {user?.displayName || "Unnamed"}
          </p>
        </div>
        <div className="rounded-2xl border border-[var(--border)] bg-white/80 p-6">
          <p className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
            Email
          </p>
          <p className="mt-3 text-lg font-semibold">
            {user?.email || "No email"}
          </p>
        </div>
      </div>
    </section>
  );
}

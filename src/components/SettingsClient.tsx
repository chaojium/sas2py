"use client";

import { updateProfile } from "firebase/auth";
import { useEffect, useState, type FormEvent } from "react";
import AuthButton from "@/components/AuthButton";
import { useAuth } from "@/components/AuthProvider";

export default function SettingsClient() {
  const { user, status } = useAuth();
  const [displayName, setDisplayName] = useState("");
  const [savedName, setSavedName] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const isAuthed = status === "authenticated";

  useEffect(() => {
    const nextName = user?.displayName || "";
    setDisplayName(nextName);
    setSavedName(nextName);
  }, [user]);

  async function handleProfileSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!user) {
      return;
    }

    const nextName = displayName.trim();
    if (!nextName) {
      setError("Name is required.");
      setMessage(null);
      return;
    }

    setSaving(true);
    setError(null);
    setMessage(null);

    try {
      await updateProfile(user, { displayName: nextName });
      await user.getIdToken(true);
      setDisplayName(nextName);
      setSavedName(nextName);
      setMessage("Profile name updated.");
    } catch (profileError) {
      setError(
        profileError instanceof Error
          ? profileError.message
          : "Unable to update profile.",
      );
    } finally {
      setSaving(false);
    }
  }

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
            {savedName || "Unnamed"}
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
      <form
        className="mt-6 rounded-2xl border border-[var(--border)] bg-white/80 p-6"
        onSubmit={handleProfileSubmit}
      >
        <label
          htmlFor="display-name"
          className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
        >
          Edit name
        </label>
        <div className="mt-3 flex flex-col gap-3 sm:flex-row">
          <input
            id="display-name"
            type="text"
            value={displayName}
            onChange={(event) => setDisplayName(event.target.value)}
            placeholder="Enter your name"
            className="min-w-0 flex-1 rounded-2xl border border-[var(--border)] bg-white px-4 py-3 text-sm shadow-inner focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
          />
          <button
            type="submit"
            disabled={saving}
            className="inline-flex items-center justify-center rounded-full bg-[var(--foreground)] px-5 py-2.5 text-sm font-semibold text-[var(--background)] transition hover:scale-[1.02] disabled:cursor-not-allowed disabled:opacity-60"
          >
            {saving ? "Saving..." : "Save profile"}
          </button>
        </div>
        {message ? (
          <p className="mt-3 text-sm text-emerald-700">{message}</p>
        ) : null}
        {error ? <p className="mt-3 text-sm text-red-600">{error}</p> : null}
      </form>
    </section>
  );
}

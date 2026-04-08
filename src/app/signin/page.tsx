"use client";

import Link from "next/link";
import {
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
  signInWithPopup,
} from "firebase/auth";
import { useRouter } from "next/navigation";
import { useEffect, useState, type FormEvent } from "react";
import { useAuth } from "@/components/AuthProvider";
import { firebaseAuth, googleProvider } from "@/lib/firebase/client";

export default function SignInPage() {
  const router = useRouter();
  const { status } = useAuth();
  const [mode, setMode] = useState<"signin" | "signup">("signin");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (status === "authenticated") {
      router.replace("/");
    }
  }, [router, status]);

  async function handleEmailAuth(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);

    try {
      if (mode === "signup") {
        await createUserWithEmailAndPassword(firebaseAuth, email, password);
      } else {
        await signInWithEmailAndPassword(firebaseAuth, email, password);
      }
      router.replace("/");
    } catch (authError) {
      setError(
        authError instanceof Error ? authError.message : "Authentication failed.",
      );
    } finally {
      setLoading(false);
    }
  }

  async function handleGoogleSignIn() {
    setLoading(true);
    setError(null);

    try {
      googleProvider.setCustomParameters({ prompt: "select_account" });
      await signInWithPopup(firebaseAuth, googleProvider);
      router.replace("/");
    } catch (authError) {
      if (
        authError instanceof Error &&
        !authError.message.includes("auth/popup-closed-by-user")
      ) {
        setError(authError.message);
      }
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="grain min-h-screen px-6 py-12 md:px-12">
      <div className="mx-auto flex w-full max-w-5xl flex-col gap-10">
        <header className="fade-up space-y-6">
          <Link
            href="/"
            className="inline-flex items-center gap-2 text-sm uppercase tracking-[0.3em] text-[var(--muted)]"
          >
            SAS2Py Studio
          </Link>
          <h1 className="text-4xl font-semibold leading-tight md:text-6xl">
            Secure access to AI-assisted SAS conversion.
          </h1>
          <p className="max-w-2xl text-lg text-[var(--muted)]">
            Sign in to keep your conversions, reviews, and notes organized in
            your private workspace.
          </p>
        </header>
        <section className="glass-card fade-up rounded-3xl p-8 md:p-12">
          <div className="flex flex-col gap-8 md:flex-row md:items-start md:justify-between">
            <div>
              <h2 className="text-2xl font-semibold">
                Continue with your team identity
              </h2>
              <p className="mt-2 text-sm text-[var(--muted)]">
                Sign in with email and password or use Google.
              </p>
            </div>
            <div className="flex w-full max-w-md flex-col gap-4">
              <div className="inline-flex rounded-full border border-[var(--border)] bg-white/70 p-1">
                <button
                  type="button"
                  onClick={() => setMode("signin")}
                  className={`rounded-full px-4 py-2 text-sm ${
                    mode === "signin"
                      ? "bg-[var(--foreground)] text-[var(--background)]"
                      : "text-[var(--muted)]"
                  }`}
                >
                  Sign in
                </button>
                <button
                  type="button"
                  onClick={() => setMode("signup")}
                  className={`rounded-full px-4 py-2 text-sm ${
                    mode === "signup"
                      ? "bg-[var(--foreground)] text-[var(--background)]"
                      : "text-[var(--muted)]"
                  }`}
                >
                  Create account
                </button>
              </div>
              <form className="flex flex-col gap-3" onSubmit={handleEmailAuth}>
                <input
                  type="email"
                  value={email}
                  onChange={(event) => setEmail(event.target.value)}
                  placeholder="you@gmail.com"
                  required
                  className="rounded-2xl border border-[var(--border)] bg-white/80 px-4 py-3 text-sm shadow-inner focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
                />
                <input
                  type="password"
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                  placeholder="Password"
                  minLength={6}
                  required
                  className="rounded-2xl border border-[var(--border)] bg-white/80 px-4 py-3 text-sm shadow-inner focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
                />
                <button
                  type="submit"
                  disabled={loading}
                  className="inline-flex items-center justify-center rounded-full bg-[var(--foreground)] px-5 py-2.5 text-sm font-semibold text-[var(--background)] transition hover:scale-[1.02] disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {loading
                    ? "Working..."
                    : mode === "signup"
                      ? "Create account"
                      : "Sign in with email"}
                </button>
              </form>
              <button
                type="button"
                onClick={() => void handleGoogleSignIn()}
                disabled={loading}
                className="inline-flex items-center justify-center rounded-full border border-[var(--border)] px-5 py-2.5 text-sm font-semibold transition hover:bg-white/60 disabled:cursor-not-allowed disabled:opacity-60"
              >
                Continue with Google
              </button>
              {error ? <p className="text-sm text-red-600">{error}</p> : null}
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}

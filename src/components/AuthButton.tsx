"use client";

import Link from "next/link";
import { useAuth } from "@/components/AuthProvider";

type AuthButtonProps = {
  variant?: "primary" | "ghost";
};

export default function AuthButton({ variant = "ghost" }: AuthButtonProps) {
  const { status, signOutUser } = useAuth();
  const isAuthed = status === "authenticated";

  const base =
    "inline-flex items-center justify-center rounded-full px-5 py-2.5 text-sm font-semibold transition";
  const variants = {
    primary:
      "bg-[var(--foreground)] text-[var(--background)] hover:scale-[1.02]",
    ghost:
      "border border-[var(--border)] text-[var(--foreground)] hover:bg-white/60",
  };

  if (status === "loading") {
    return (
      <button className={`${base} ${variants[variant]}`} disabled>
        Loading...
      </button>
    );
  }

  return isAuthed ? (
    <button
      onClick={() => void signOutUser()}
      className={`${base} ${variants[variant]}`}
    >
      Sign out
    </button>
  ) : (
    <Link href="/signin" className={`${base} ${variants[variant]}`}>
      Sign in
    </Link>
  );
}

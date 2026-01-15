import Link from "next/link";
import AuthButton from "@/components/AuthButton";

export default function SignInPage() {
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
          <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="text-2xl font-semibold">
                Continue with your team identity
              </h2>
              <p className="mt-2 text-sm text-[var(--muted)]">
                We use OAuth providers configured in NextAuth.
              </p>
            </div>
            <AuthButton variant="primary" />
          </div>
        </section>
      </div>
    </main>
  );
}

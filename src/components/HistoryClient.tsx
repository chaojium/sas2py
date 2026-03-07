"use client";

import { useCallback, useEffect, useState } from "react";
import { useSession } from "next-auth/react";
import Link from "next/link";
import AuthButton from "@/components/AuthButton";
import CodeBlock from "@/components/CodeBlock";

type Entry = {
  id: string;
  name: string;
  language: "PYTHON" | "R";
  sasCode: string;
  pythonCode: string;
  createdAt: string;
  runs: {
    id: string;
    exitCode: number | null;
    timedOut: boolean;
    durationMs: number;
    detectedPackages: string[];
    policyMode: string;
    createdAt: string;
  }[];
};

export default function HistoryClient() {
  const { status } = useSession();
  const isAuthed = status === "authenticated";
  const [entries, setEntries] = useState<Entry[]>([]);
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [expandedIds, setExpandedIds] = useState<Record<string, boolean>>({});

  const fetchEntries = useCallback(async (search?: string, showLoading = true) => {
    if (!isAuthed) return;
    if (showLoading) {
      setLoading(true);
    }
    const url = search
      ? `/api/conversions?search=${encodeURIComponent(search)}`
      : "/api/conversions";
    const response = await fetch(url);
    const data = await response.json();
    setEntries(response.ok ? data.entries || [] : []);
    if (showLoading) {
      setLoading(false);
    }
  }, [isAuthed]);

  useEffect(() => {
    if (!isAuthed) return;
    fetch("/api/conversions")
      .then((response) => response.json())
      .then((data) => setEntries(data.entries || []))
      .catch(() => setEntries([]));
  }, [isAuthed]);

  if (!isAuthed) {
    return (
      <section className="glass-card fade-up rounded-3xl p-8 md:p-12">
        <div className="flex flex-col gap-6">
          <h2 className="text-2xl font-semibold">Sign in to see history.</h2>
          <p className="text-[var(--muted)]">
            Your conversion history is tied to your account.
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
            Conversion history
          </p>
          <h1 className="text-3xl font-semibold">All conversions</h1>
        </div>
        <div className="flex items-center gap-3">
          <Link
            href="/"
            className="rounded-full border border-[var(--border)] px-4 py-2 text-sm text-[var(--muted)] transition hover:bg-white/70"
          >
            Back to studio
          </Link>
          <AuthButton />
        </div>
      </div>
      <div className="mt-6 flex flex-wrap gap-3">
        <input
          className="min-w-[240px] flex-1 rounded-full border border-[var(--border)] bg-white/80 px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
          placeholder="Search by name or code..."
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <button
          onClick={() => fetchEntries(query)}
          className="rounded-full bg-[var(--secondary)] px-5 py-2 text-sm font-semibold text-white transition hover:translate-y-[-1px]"
        >
          Search
        </button>
        <button
          onClick={() => {
            setQuery("");
            void fetchEntries("");
          }}
          className="rounded-full border border-[var(--border)] px-4 py-2 text-sm text-[var(--muted)] transition hover:bg-white/70"
        >
          Reset
        </button>
      </div>
      <div className="mt-8 space-y-5">
        {loading ? (
          <p className="text-sm text-[var(--muted)]">Loading...</p>
        ) : entries.length === 0 ? (
          <p className="text-sm text-[var(--muted)]">
            No conversions found.
          </p>
        ) : (
          entries.map((entry) => (
            <article
              key={entry.id}
              className="rounded-2xl border border-[var(--border)] bg-white/80 p-5"
            >
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <button
                    className="text-left text-lg font-semibold hover:text-[var(--secondary)]"
                    onClick={() =>
                      setExpandedIds((prev) => ({
                        ...prev,
                        [entry.id]: !prev[entry.id],
                      }))
                    }
                  >
                    {entry.name}
                  </button>
                  <span className="text-xs text-[var(--muted)]">
                    {new Date(entry.createdAt).toLocaleString()}
                  </span>
                </div>
                {expandedIds[entry.id] ? (
                  <div className="mt-4 space-y-4">
                    <div className="grid gap-4 md:grid-cols-2">
                      <div>
                        <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                          SAS Source
                        </h4>
                        <div className="mt-2">
                          <CodeBlock
                            code={entry.sasCode}
                            language="sas"
                            maxHeight={256}
                          />
                        </div>
                      </div>
                      <div>
                        <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                          {entry.language === "R" ? "R Output" : "Python Output"}
                        </h4>
                        <div className="mt-2">
                          <CodeBlock
                            code={entry.pythonCode}
                            language={entry.language === "R" ? "r" : "python"}
                            maxHeight={256}
                          />
                        </div>
                      </div>
                    </div>
                    <div>
                      <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                        Executions
                      </h4>
                      <div className="mt-2 space-y-2">
                        {entry.runs.length === 0 ? (
                          <p className="text-sm text-[var(--muted)]">
                            No executions recorded.
                          </p>
                        ) : (
                          entry.runs.slice(0, 5).map((run) => (
                            <div
                              key={run.id}
                              className="rounded-xl border border-[var(--border)] bg-white/70 p-3 text-sm"
                            >
                              <p className="text-xs text-[var(--muted)]">
                                {new Date(run.createdAt).toLocaleString()} | exit{" "}
                                {run.exitCode ?? "unknown"} | {run.durationMs}ms
                                {run.timedOut ? " | timed out" : ""} | policy:{" "}
                                {run.policyMode}
                              </p>
                              <p className="mt-1 text-xs text-[var(--muted)]">
                                packages:{" "}
                                {run.detectedPackages.length > 0
                                  ? run.detectedPackages.join(", ")
                                  : "none detected"}
                              </p>
                            </div>
                          ))
                        )}
                      </div>
                    </div>
                  </div>
                ) : null}
            </article>
          ))
        )}
      </div>
    </section>
  );
}

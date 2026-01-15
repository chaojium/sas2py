"use client";

import { useEffect, useMemo, useState } from "react";
import { useSession } from "next-auth/react";
import AuthButton from "@/components/AuthButton";
import CodeBlock from "@/components/CodeBlock";

type Review = {
  id: string;
  rating: number | null;
  summary: string | null;
  comments: string;
  createdAt: string;
  reviewer: {
    name: string | null;
    email: string | null;
  };
};

type Entry = {
  id: string;
  name: string;
  language: "PYTHON" | "R";
  sasCode: string;
  pythonCode: string;
  createdAt: string;
  reviews: Review[];
};

type Draft = {
  summary: string;
  comments: string;
  rating: string;
};

export default function Converter() {
  const { status } = useSession();
  const isAuthed = status === "authenticated";
  const [sasCode, setSasCode] = useState("");
  const [name, setName] = useState("");
  const [language, setLanguage] = useState<"PYTHON" | "R">("PYTHON");
  const [pythonCode, setPythonCode] = useState("");
  const [currentEntryId, setCurrentEntryId] = useState<string | null>(null);
  const [entries, setEntries] = useState<Entry[]>([]);
  const [expandedIds, setExpandedIds] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reviewDrafts, setReviewDrafts] = useState<Record<string, Draft>>({});
  const [enhancePrompt, setEnhancePrompt] = useState("");
  const [fullScreen, setFullScreen] = useState(false);

  useEffect(() => {
    if (fullScreen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => {
      document.body.style.overflow = "";
    };
  }, [fullScreen]);

  const canConvert = isAuthed && sasCode.trim().length > 0 && !loading;

  const fetchEntries = async () => {
    if (!isAuthed) return;
    const response = await fetch("/api/conversions");
    if (!response.ok) return;
    const data = await response.json();
    setEntries(data.entries || []);
  };

  useEffect(() => {
    fetchEntries();
  }, [isAuthed]);

  const handleConvert = async () => {
    if (!name.trim()) {
      setError("Name is required before converting.");
      return;
    }
    setLoading(true);
    setError(null);
    setPythonCode("");
    setCurrentEntryId(null);
    try {
      const response = await fetch("/api/conversions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sasCode, name, language }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || "Conversion failed.");
      }
      setPythonCode(data.entry.pythonCode);
      setCurrentEntryId(data.entry.id);
      setLanguage(data.entry.language || "PYTHON");
      setName("");
      setEnhancePrompt("");
      await fetchEntries();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Conversion failed.");
    } finally {
      setLoading(false);
    }
  };

  const handleReviewChange = (
    entryId: string,
    field: keyof Draft,
    value: string,
  ) => {
    setReviewDrafts((prev) => ({
      ...prev,
      [entryId]: {
        summary: prev[entryId]?.summary || "",
        comments: prev[entryId]?.comments || "",
        rating: prev[entryId]?.rating || "",
        [field]: value,
      },
    }));
  };

  const handleReviewSubmit = async (entryId: string) => {
    const draft = reviewDrafts[entryId];
    if (!draft?.comments?.trim()) {
      setError("Add comments before submitting a review.");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetch("/api/reviews", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          codeEntryId: entryId,
          summary: draft.summary,
          comments: draft.comments,
          rating: draft.rating ? Number(draft.rating) : null,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || "Review failed.");
      }
      setReviewDrafts((prev) => ({ ...prev, [entryId]: { summary: "", comments: "", rating: "" } }));
      await fetchEntries();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Review failed.");
    } finally {
      setLoading(false);
    }
  };

  const handleEnhance = async () => {
    if (!currentEntryId || !enhancePrompt.trim()) {
      setError("Add a refinement prompt before running enhancement.");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetch("/api/conversions", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          entryId: currentEntryId,
          instruction: enhancePrompt,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || "Enhancement failed.");
      }
      setPythonCode(data.entry.pythonCode);
      setEnhancePrompt("");
      await fetchEntries();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Enhancement failed.");
    } finally {
      setLoading(false);
    }
  };

  const currentEntry = useMemo(
    () => entries.find((entry) => entry.id === currentEntryId) || null,
    [entries, currentEntryId],
  );

  if (!isAuthed) {
    return (
      <section className="glass-card fade-up rounded-3xl p-8 md:p-12">
        <div className="flex flex-col gap-6">
          <h2 className="text-2xl font-semibold">
            Sign in to start converting SAS to Python.
          </h2>
          {/* <p className="text-[var(--muted)]">
            Authentication keeps your code history and review notes private to
            your account.
          </p> */}
          <AuthButton variant="primary" />
        </div>
      </section>
    );
  }

  return (
    <section
      className={`fade-up ${
        fullScreen
          ? "fixed inset-0 z-30 overflow-auto bg-[var(--background)] px-6 py-8 md:px-12"
          : ""
      }`}
    >
      <div className="grid gap-8 lg:grid-cols-[1.05fr_0.95fr]">
        <div className="glass-card rounded-3xl p-6 md:p-10">
          <div className="flex items-center justify-between gap-4">
            <h2 className="text-2xl font-semibold">Conversion workspace</h2>
            <button
              onClick={() => setFullScreen((prev) => !prev)}
              className="rounded-full border border-[var(--border)] px-4 py-2 text-xs uppercase tracking-[0.2em] text-[var(--muted)] transition hover:bg-white/70"
            >
              {fullScreen ? "Exit full screen" : "Full screen"}
            </button>
          </div>
          <p className="mt-3 text-sm text-[var(--muted)]">
            Paste SAS code, run the conversion, then annotate the Python output
            with review notes.
          </p>
          <div className="mt-6 space-y-4">
            <input
              className="w-full rounded-2xl border border-[var(--border)] bg-white/80 px-4 py-3 text-sm shadow-inner focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
              placeholder="Name this conversion (required)"
              value={name}
              onChange={(event) => setName(event.target.value)}
            />
            <div className="flex flex-wrap items-center gap-3">
              <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                Output language
              </span>
              <button
                type="button"
                onClick={() => setLanguage("PYTHON")}
                className={`rounded-full border px-4 py-2 text-xs uppercase tracking-[0.2em] transition ${
                  language === "PYTHON"
                    ? "border-[var(--foreground)] bg-[var(--foreground)] text-[var(--background)]"
                    : "border-[var(--border)] text-[var(--foreground)] hover:bg-white/70"
                }`}
              >
                Python
              </button>
              <button
                type="button"
                onClick={() => setLanguage("R")}
                className={`rounded-full border px-4 py-2 text-xs uppercase tracking-[0.2em] transition ${
                  language === "R"
                    ? "border-[var(--foreground)] bg-[var(--foreground)] text-[var(--background)]"
                    : "border-[var(--border)] text-[var(--foreground)] hover:bg-white/70"
                }`}
              >
                R
              </button>
            </div>
            <textarea
              className="min-h-[220px] w-full rounded-2xl border border-[var(--border)] bg-white/80 p-4 font-mono text-sm shadow-inner focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
              placeholder="Paste SAS code here..."
              value={sasCode}
              onChange={(event) => setSasCode(event.target.value)}
            />
            <div className="flex flex-wrap items-center gap-3">
              <button
                onClick={handleConvert}
                disabled={!canConvert}
                className="rounded-full bg-[var(--primary)] px-6 py-2.5 text-sm font-semibold text-white transition hover:translate-y-[-1px] disabled:cursor-not-allowed disabled:opacity-50"
              >
              {loading ? "Running Codex..." : "Convert"}
            </button>
              <button
                onClick={() => setSasCode("")}
                className="rounded-full border border-[var(--border)] px-4 py-2 text-sm text-[var(--muted)] transition hover:bg-white/70"
              >
                Clear
              </button>
              {error ? (
                <span className="text-sm text-red-600">{error}</span>
              ) : null}
            </div>
          </div>
          <div className="mt-8">
            <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold">
              {language === "R" ? "R output" : "Python output"}
            </h3>
              {pythonCode ? (
                <button
                  onClick={() => navigator.clipboard.writeText(pythonCode)}
                  className="text-xs uppercase tracking-[0.2em] text-[var(--muted)] hover:text-[var(--foreground)]"
                >
                  Copy
                </button>
              ) : null}
            </div>
            <div className="mt-3">
            <CodeBlock
              code={
                pythonCode ||
                `Your converted ${language === "R" ? "R" : "Python"} will appear here once Codex finishes the translation.`
              }
              language={language === "R" ? "r" : "python"}
              maxHeight={320}
            />
            </div>
            {currentEntryId && pythonCode ? (
              <div className="mt-4 rounded-2xl border border-[var(--border)] bg-white/70 p-4">
                <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                  Enhance conversion
                </h4>
                <div className="mt-3 grid gap-3">
                  <input
                    className="w-full rounded-xl border border-[var(--border)] bg-white/90 px-3 py-2 text-sm"
                    placeholder="e.g. optimize with vectorized pandas, add typing, improve performance"
                    value={enhancePrompt}
                    onChange={(event) => setEnhancePrompt(event.target.value)}
                  />
                  <div className="flex flex-wrap items-center gap-3">
                    <button
                      onClick={handleEnhance}
                      className="rounded-full bg-[var(--foreground)] px-5 py-2 text-sm font-semibold text-[var(--background)] transition hover:translate-y-[-1px]"
                    >
                      Apply enhancement
                    </button>
                  </div>
                </div>
              </div>
            ) : null}
          </div>
          {currentEntry ? (
            <div className="mt-8 rounded-2xl border border-[var(--border)] bg-white/70 p-5">
              <h4 className="text-sm font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                Review this conversion
              </h4>
              <div className="mt-3 grid gap-3">
                <input
                  className="w-full rounded-xl border border-[var(--border)] bg-white/90 px-3 py-2 text-sm"
                  placeholder="Short summary (optional)"
                  value={reviewDrafts[currentEntry.id]?.summary || ""}
                  onChange={(event) =>
                    handleReviewChange(
                      currentEntry.id,
                      "summary",
                      event.target.value,
                    )
                  }
                />
                <textarea
                  className="min-h-[120px] w-full rounded-xl border border-[var(--border)] bg-white/90 px-3 py-2 text-sm"
                  placeholder="Comments and feedback"
                  value={reviewDrafts[currentEntry.id]?.comments || ""}
                  onChange={(event) =>
                    handleReviewChange(
                      currentEntry.id,
                      "comments",
                      event.target.value,
                    )
                  }
                />
                <div className="flex flex-wrap items-center gap-3">
                  <select
                    className="rounded-full border border-[var(--border)] bg-white/90 px-4 py-2 text-sm"
                    value={reviewDrafts[currentEntry.id]?.rating || ""}
                    onChange={(event) =>
                      handleReviewChange(
                        currentEntry.id,
                        "rating",
                        event.target.value,
                      )
                    }
                  >
                    <option value="">Rating</option>
                    <option value="1">1 - Needs work</option>
                    <option value="2">2 - Rough</option>
                    <option value="3">3 - Solid</option>
                    <option value="4">4 - Great</option>
                    <option value="5">5 - Excellent</option>
                  </select>
                  <button
                    onClick={() => handleReviewSubmit(currentEntry.id)}
                    className="rounded-full bg-[var(--secondary)] px-5 py-2 text-sm font-semibold text-white transition hover:translate-y-[-1px]"
                  >
                    Save review
                  </button>
                </div>
              </div>
            </div>
          ) : null}
        </div>
        <div className="glass-card rounded-3xl p-6 md:p-10">
          <div className="flex items-center justify-between gap-3">
            <h2 className="text-2xl font-semibold">Recent conversions</h2>
            <span className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
              {entries.length} stored
            </span>
          </div>
          <p className="mt-3 text-sm text-[var(--muted)]">
            Each conversion is stored in Postgres with its review history.
          </p>
          <div className="mt-3">
            <a
              href="/history"
              className="text-xs uppercase tracking-[0.2em] text-[var(--secondary)]"
            >
              View full history
            </a>
          </div>
          <div className="mt-6 space-y-6">
            {entries.length === 0 ? (
              <div className="rounded-2xl border border-dashed border-[var(--border)] p-6 text-sm text-[var(--muted)]">
                No conversions yet. Paste SAS code to create your first entry.
              </div>
            ) : null}
            {entries.map((entry) => (
              <div
                key={entry.id}
                className="rounded-2xl border border-[var(--border)] bg-white/80 p-5"
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
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
                    <p className="text-xs text-[var(--muted)]">
                      {new Date(entry.createdAt).toLocaleString()}
                    </p>
                  </div>
                  <button
                    className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                    onClick={() => {
                      setPythonCode(entry.pythonCode);
                      setCurrentEntryId(entry.id);
                      setLanguage(entry.language);
                    }}
                  >
                    View
                  </button>
                </div>
                {expandedIds[entry.id] ? (
                  <div className="mt-4 space-y-4">
                    <div>
                      <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                        SAS Source
                      </h4>
                      <div className="mt-2">
                        <CodeBlock
                          code={entry.sasCode}
                          language="sas"
                          maxHeight={144}
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
                          maxHeight={144}
                        />
                      </div>
                    </div>
                  </div>
                ) : null}
                <div className="mt-4 space-y-3">
                  <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                    Reviews
                  </h4>
                  {entry.reviews.length === 0 ? (
                    <p className="text-sm text-[var(--muted)]">
                      No reviews yet. Add feedback from the workspace.
                    </p>
                  ) : (
                    entry.reviews.map((review) => (
                      <div
                        key={review.id}
                        className="rounded-xl border border-[var(--border)] bg-white/70 p-3 text-sm"
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-[var(--muted)]">
                          <span>
                            {review.reviewer?.name ||
                              review.reviewer?.email ||
                              "Reviewer"}
                          </span>
                          <span>
                            {new Date(review.createdAt).toLocaleString()}
                          </span>
                        </div>
                        {review.summary ? (
                          <p className="mt-2 font-semibold">
                            {review.summary}
                          </p>
                        ) : null}
                        <p className="mt-1">{review.comments}</p>
                        {review.rating ? (
                          <p className="mt-2 text-xs text-[var(--muted)]">
                            Rating: {review.rating}/5
                          </p>
                        ) : null}
                      </div>
                    ))
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

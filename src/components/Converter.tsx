"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useSession } from "next-auth/react";
import Image from "next/image";
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

type Run = {
  id: string;
  language: "PYTHON" | "R";
  stdout: string;
  stderr: string;
  exitCode: number | null;
  timedOut: boolean;
  durationMs: number;
  detectedPackages: string[];
  policyMode: "off" | "blocklist" | "allowlist" | string;
  createdAt: string;
};

type Entry = {
  id: string;
  name: string;
  language: "PYTHON" | "R";
  sasCode: string;
  pythonCode: string;
  createdAt: string;
  reviews: Review[];
  runs: Run[];
};

type Draft = {
  summary: string;
  comments: string;
  rating: string;
};

type ExecutionResult = {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  timedOut: boolean;
  durationMs: number;
  images?: string[];
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
  const [executeLoading, setExecuteLoading] = useState(false);
  const [executeError, setExecuteError] = useState<string | null>(null);
  const [executeResult, setExecuteResult] = useState<ExecutionResult | null>(
    null,
  );
  const [uploadedSasFileName, setUploadedSasFileName] = useState<string | null>(
    null,
  );

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

  const fetchEntries = useCallback(async () => {
    if (!isAuthed) return;
    const response = await fetch("/api/conversions");
    if (!response.ok) return;
    const data = await response.json();
    setEntries(data.entries || []);
  }, [isAuthed]);

  useEffect(() => {
    void fetchEntries();
  }, [fetchEntries]);

  const handleConvert = async () => {
    if (!name.trim()) {
      setError("Name is required before converting.");
      return;
    }
    setLoading(true);
    setError(null);
    setExecuteError(null);
    setExecuteResult(null);
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
      setExecuteResult(null);
      setExecuteError(null);
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

  const handleSasFileUpload = async (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const isSasFile =
      file.name.toLowerCase().endsWith(".sas") ||
      file.type === "text/plain" ||
      file.type === "application/octet-stream";
    if (!isSasFile) {
      setError("Please upload a .sas file.");
      event.target.value = "";
      return;
    }

    try {
      const text = await file.text();
      setSasCode(text);
      setUploadedSasFileName(file.name);
      setError(null);
      if (!name.trim()) {
        const baseName = file.name.replace(/\.sas$/i, "");
        setName(baseName);
      }
    } catch {
      setError("Failed to read the SAS file.");
    } finally {
      event.target.value = "";
    }
  };

  const handleDownloadConvertedFile = () => {
    if (!pythonCode.trim()) return;
    const extension = language === "R" ? "R" : "py";
    const rawName =
      currentEntry?.name?.trim() ||
      name.trim() ||
      uploadedSasFileName?.replace(/\.sas$/i, "").trim() ||
      "converted_code";
    const safeName = rawName.replace(/[^\w.-]+/g, "_");
    const blob = new Blob([pythonCode], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${safeName}.${extension}`;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  };

  const handleExecute = async () => {
    if (!pythonCode.trim()) {
      setExecuteError("No generated code to run.");
      return;
    }
    setExecuteLoading(true);
    setExecuteError(null);
    setExecuteResult(null);
    try {
      const response = await fetch("/api/execute", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          code: pythonCode,
          language,
          codeEntryId: currentEntryId,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || "Execution failed.");
      }
      setExecuteResult(data.result || null);
      await fetchEntries();
    } catch (err) {
      setExecuteError(err instanceof Error ? err.message : "Execution failed.");
    } finally {
      setExecuteLoading(false);
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
              <label className="cursor-pointer rounded-full border border-[var(--border)] px-4 py-2 text-sm text-[var(--muted)] transition hover:bg-white/70">
                Upload .sas file
                <input
                  type="file"
                  accept=".sas,text/plain"
                  className="hidden"
                  onChange={handleSasFileUpload}
                />
              </label>
              {uploadedSasFileName ? (
                <span className="text-xs text-[var(--muted)]">
                  Loaded: {uploadedSasFileName}
                </span>
              ) : null}
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <button
                onClick={handleConvert}
                disabled={!canConvert}
                className="rounded-full bg-[var(--primary)] px-6 py-2.5 text-sm font-semibold text-white transition hover:translate-y-[-1px] disabled:cursor-not-allowed disabled:opacity-50"
              >
              {loading ? "Running GPT-5.2..." : "Convert"}
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
                <div className="flex items-center gap-3">
                  <button
                    onClick={handleDownloadConvertedFile}
                    className="text-xs uppercase tracking-[0.2em] text-[var(--muted)] hover:text-[var(--foreground)]"
                  >
                    Download
                  </button>
                  <button
                    onClick={() => navigator.clipboard.writeText(pythonCode)}
                    className="text-xs uppercase tracking-[0.2em] text-[var(--muted)] hover:text-[var(--foreground)]"
                  >
                    Copy
                  </button>
                </div>
              ) : null}
            </div>
            <div className="mt-3">
            <CodeBlock
              code={
                pythonCode ||
                `Your converted ${language === "R" ? "R" : "Python"} will appear here once GPT-5.2 finishes the translation.`
              }
              language={language === "R" ? "r" : "python"}
              maxHeight={320}
            />
            </div>
            <div className="mt-4 flex flex-wrap items-center gap-3">
              <button
                onClick={handleExecute}
                disabled={!pythonCode || executeLoading}
                className="rounded-full bg-[var(--secondary)] px-5 py-2 text-sm font-semibold text-white transition hover:translate-y-[-1px] disabled:cursor-not-allowed disabled:opacity-50"
              >
                {executeLoading
                  ? `Running ${language === "R" ? "R" : "Python"}...`
                  : `Run ${language === "R" ? "R" : "Python"} code`}
              </button>
              {executeError ? (
                <span className="text-sm text-red-600">{executeError}</span>
              ) : null}
            </div>
            {executeResult ? (
              <div className="mt-4 rounded-2xl border border-[var(--border)] bg-white/70 p-4">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                    Execution output
                  </h4>
                  <p className="text-xs text-[var(--muted)]">
                    Exit code {executeResult.exitCode ?? "unknown"} in{" "}
                    {executeResult.durationMs}ms
                    {executeResult.timedOut ? " (timed out)" : ""}
                  </p>
                </div>
                <div className="mt-3 space-y-3">
                  <div>
                    <p className="mb-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                      Stdout
                    </p>
                    <CodeBlock
                      code={executeResult.stdout || "(no output)"}
                      language="text"
                      maxHeight={160}
                    />
                  </div>
                  <div>
                    <p className="mb-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                      Stderr
                    </p>
                    <CodeBlock
                      code={executeResult.stderr || "(no output)"}
                      language="text"
                      maxHeight={160}
                    />
                  </div>
                  {executeResult.images && executeResult.images.length > 0 ? (
                    <div>
                      <p className="mb-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                        Plots
                      </p>
                      <div className="grid gap-3 md:grid-cols-2">
                        {executeResult.images.map((image, index) => (
                          <Image
                            key={`${index}-${image.length}`}
                            src={`data:image/png;base64,${image}`}
                            alt={`Execution plot ${index + 1}`}
                            width={1200}
                            height={800}
                            unoptimized
                            className="h-auto w-full rounded-xl border border-[var(--border)] bg-white p-2"
                          />
                        ))}
                      </div>
                    </div>
                  ) : null}
                </div>
              </div>
            ) : null}
            {currentEntry?.runs?.length ? (
              <div className="mt-4 rounded-2xl border border-[var(--border)] bg-white/70 p-4">
                <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                  Recent executions
                </h4>
                <div className="mt-3 space-y-3">
                  {currentEntry.runs.slice(0, 3).map((run) => (
                    <div
                      key={run.id}
                      className="rounded-xl border border-[var(--border)] bg-white/80 p-3 text-sm"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-[var(--muted)]">
                        <span>
                          {new Date(run.createdAt).toLocaleString()} | exit{" "}
                          {run.exitCode ?? "unknown"} | {run.durationMs}ms
                          {run.timedOut ? " | timed out" : ""}
                        </span>
                        <span>policy: {run.policyMode}</span>
                      </div>
                      <p className="mt-2 text-xs text-[var(--muted)]">
                        packages:{" "}
                        {run.detectedPackages.length > 0
                          ? run.detectedPackages.join(", ")
                          : "none detected"}
                      </p>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
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
                      setExecuteResult(null);
                      setExecuteError(null);
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

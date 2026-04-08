"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import AuthButton from "@/components/AuthButton";
import { useAuth } from "@/components/AuthProvider";
import CodeBlock from "@/components/CodeBlock";
import { authFetch } from "@/lib/firebase/auth-fetch";

type Entry = {
  id: string;
  projectId: string | null;
  projectName: string | null;
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

type Project = {
  id: string;
  name: string;
  description: string | null;
  createdAt: string;
  entryCount: number;
};

export default function HistoryClient() {
  const { status } = useAuth();
  const isAuthed = status === "authenticated";
  const [entries, setEntries] = useState<Entry[]>([]);
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [expandedIds, setExpandedIds] = useState<Record<string, boolean>>({});
  const [projectName, setProjectName] = useState("");
  const [projectDescription, setProjectDescription] = useState("");
  const [savingProject, setSavingProject] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [activeSearch, setActiveSearch] = useState("");

  const fetchEntries = useCallback(
    async (showLoading = true) => {
      if (!isAuthed) return;
      if (showLoading) setLoading(true);
      try {
        const response = await authFetch("/api/conversions");
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data?.error || "Failed to load history.");
        }
        setEntries(data.entries || []);
      } catch (error) {
        setEntries([]);
        setMessage(error instanceof Error ? error.message : "Failed to load history.");
      } finally {
        if (showLoading) setLoading(false);
      }
    },
    [isAuthed],
  );

  const fetchProjects = useCallback(async () => {
    if (!isAuthed) return;
    const response = await authFetch("/api/projects");
    const data = await response.json();
    setProjects(response.ok ? data.projects || [] : []);
  }, [isAuthed]);

  useEffect(() => {
    if (!isAuthed) return;
    void Promise.all([fetchEntries(false), fetchProjects()]);
  }, [fetchEntries, fetchProjects, isAuthed]);

  const filteredEntries = useMemo(() => {
    const normalized = activeSearch.trim().toLowerCase();
    if (!normalized) return entries;
    return entries.filter((entry) =>
      [
        entry.name,
        entry.projectName || "",
        entry.sasCode,
        entry.pythonCode,
      ].some((value) => value.toLowerCase().includes(normalized)),
    );
  }, [activeSearch, entries]);

  const groupedEntries = useMemo(() => {
    const byProject = new Map<string, Entry[]>();
    for (const entry of filteredEntries) {
      const key = entry.projectId || "unassigned";
      const group = byProject.get(key) || [];
      group.push(entry);
      byProject.set(key, group);
    }
    return byProject;
  }, [filteredEntries]);

  const handleCreateProject = async () => {
    if (!projectName.trim()) {
      setMessage("Project name is required.");
      return;
    }
    setSavingProject(true);
    setMessage(null);
    try {
      const response = await authFetch("/api/projects", {
        method: "POST",
        body: JSON.stringify({
          name: projectName,
          description: projectDescription,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || "Project creation failed.");
      }
      setProjectName("");
      setProjectDescription("");
      setMessage("Project created.");
      await fetchProjects();
    } catch (error) {
      setMessage(
        error instanceof Error ? error.message : "Project creation failed.",
      );
    } finally {
      setSavingProject(false);
    }
  };

  const handleMoveEntry = async (entryId: string, nextProjectId: string) => {
    setMessage(null);
    const response = await authFetch("/api/conversions", {
      method: "PATCH",
      body: JSON.stringify({
        entryId,
        projectId: nextProjectId || null,
      }),
    });
    const data = await response.json();
    if (!response.ok) {
      setMessage(data?.error || "Failed to move conversion.");
      return;
    }
    const project = projects.find((item) => item.id === nextProjectId);
    setEntries((prev) =>
      prev.map((entry) =>
        entry.id === entryId
          ? {
              ...entry,
              projectId: nextProjectId || null,
              projectName: nextProjectId ? project?.name || null : null,
            }
          : entry,
      ),
    );
    await fetchProjects();
    setMessage(nextProjectId ? "Conversion moved to project." : "Conversion unassigned.");
  };

  const handleDeleteEntry = async (entryId: string) => {
    setMessage(null);
    const response = await authFetch("/api/conversions", {
      method: "DELETE",
      body: JSON.stringify({ entryId }),
    });
    const data = await response.json();
    if (!response.ok) {
      setMessage(data?.error || "Failed to delete conversion.");
      return;
    }
    setEntries((prev) => prev.filter((entry) => entry.id !== entryId));
    await fetchProjects();
    setExpandedIds((prev) => {
      const next = { ...prev };
      delete next[entryId];
      return next;
    });
    setMessage("Conversion deleted.");
  };

  const handleDeleteProject = async (projectId: string) => {
    setMessage(null);
    const response = await authFetch(`/api/projects/${projectId}`, {
      method: "DELETE",
    });
    const data = await response.json();
    if (!response.ok) {
      setMessage(data?.error || "Failed to delete project.");
      return;
    }
    await Promise.all([fetchEntries(false), fetchProjects()]);
  };

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
          <h1 className="text-3xl font-semibold">Projects</h1>
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

      <div className="mt-6 grid gap-8 lg:grid-cols-[320px_1fr]">
        <aside className="space-y-6">
          <div className="rounded-2xl border border-[var(--border)] bg-white/80 p-5">
            <h2 className="text-sm font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
              New project
            </h2>
            <div className="mt-4 grid gap-3">
              <input
                className="rounded-xl border border-[var(--border)] bg-white/90 px-3 py-2 text-sm"
                placeholder="Project name"
                value={projectName}
                onChange={(event) => setProjectName(event.target.value)}
              />
              <textarea
                className="min-h-[96px] rounded-xl border border-[var(--border)] bg-white/90 px-3 py-2 text-sm"
                placeholder="Description (optional)"
                value={projectDescription}
                onChange={(event) => setProjectDescription(event.target.value)}
              />
              <button
                onClick={handleCreateProject}
                disabled={savingProject}
                className="rounded-full bg-[var(--foreground)] px-5 py-2 text-sm font-semibold text-[var(--background)] transition hover:translate-y-[-1px] disabled:cursor-not-allowed disabled:opacity-50"
              >
                {savingProject ? "Creating..." : "Create project"}
              </button>
            </div>
          </div>

          <div className="rounded-2xl border border-[var(--border)] bg-white/80 p-5">
            <h2 className="text-sm font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
              Collections
            </h2>
            <div className="mt-4 space-y-3">
              <div className="rounded-xl border border-[var(--border)] bg-white/70 p-3">
                <p className="text-sm font-semibold">Unassigned</p>
                <p className="mt-1 text-xs text-[var(--muted)]">
                  {groupedEntries.get("unassigned")?.length || 0} conversions
                </p>
              </div>
              {projects.map((project) => (
                <div
                  key={project.id}
                  className="rounded-xl border border-[var(--border)] bg-white/70 p-3"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold">{project.name}</p>
                      <p className="mt-1 text-xs text-[var(--muted)]">
                        {project.entryCount} conversions
                      </p>
                      {project.description ? (
                        <p className="mt-2 text-xs text-[var(--muted)]">
                          {project.description}
                        </p>
                      ) : null}
                    </div>
                    <button
                      onClick={() => handleDeleteProject(project.id)}
                      className="text-xs uppercase tracking-[0.2em] text-red-600"
                    >
                      Delete
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </aside>

        <div>
          <div
            className="flex flex-wrap gap-3"
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                setMessage(null);
                setActiveSearch(query.trim());
              }
            }}
          >
            <input
              className="min-w-[240px] flex-1 rounded-full border border-[var(--border)] bg-white/80 px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
              placeholder="Search by name or code..."
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
            <button
              onClick={() => {
                setMessage(null);
                setActiveSearch(query.trim());
              }}
              className="rounded-full bg-[var(--secondary)] px-5 py-2 text-sm font-semibold text-white transition hover:translate-y-[-1px]"
            >
              Search
            </button>
            <button
              onClick={() => {
                setMessage(null);
                setQuery("");
                setActiveSearch("");
              }}
              className="rounded-full border border-[var(--border)] px-4 py-2 text-sm text-[var(--muted)] transition hover:bg-white/70"
            >
              Reset
            </button>
          </div>
          {message ? (
            <p className="mt-3 text-sm text-[var(--muted)]">{message}</p>
          ) : null}
          {!message && activeSearch ? (
            <p className="mt-3 text-sm text-[var(--muted)]">
              Showing results for &quot;{activeSearch}&quot;.
            </p>
          ) : null}

          <div className="mt-8 space-y-8">
            {[{ id: "unassigned", name: "Unassigned" }, ...projects].map((group) => {
              const groupId = group.id;
              const groupEntries = groupedEntries.get(groupId) || [];
              if (groupEntries.length === 0) return null;

              return (
                <section key={groupId} className="space-y-4">
                  <div>
                    <h2 className="text-xl font-semibold">{group.name}</h2>
                    {"description" in group && group.description ? (
                      <p className="mt-1 text-sm text-[var(--muted)]">
                        {group.description}
                      </p>
                    ) : null}
                  </div>

                  <div className="space-y-5">
                    {groupEntries.map((entry) => (
                      <article
                        key={entry.id}
                        className="rounded-2xl border border-[var(--border)] bg-white/80 p-5"
                      >
                        <div className="flex flex-wrap items-start justify-between gap-4">
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
                            <p className="mt-1 text-xs text-[var(--muted)]">
                              {new Date(entry.createdAt).toLocaleString()}
                            </p>
                          </div>
                          <div className="flex flex-wrap items-center gap-3">
                            <select
                              className="rounded-full border border-[var(--border)] bg-white/90 px-4 py-2 text-sm"
                              value={entry.projectId || ""}
                              onChange={(event) =>
                                void handleMoveEntry(entry.id, event.target.value)
                              }
                            >
                              <option value="">Unassigned</option>
                              {projects.map((project) => (
                                <option key={project.id} value={project.id}>
                                  {project.name}
                                </option>
                              ))}
                            </select>
                            <button
                              onClick={() => void handleDeleteEntry(entry.id)}
                              className="rounded-full border border-red-200 px-4 py-2 text-sm text-red-600 transition hover:bg-red-50"
                            >
                              Delete
                            </button>
                          </div>
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
                    ))}
                  </div>
                </section>
              );
            })}

            {!loading && filteredEntries.length === 0 ? (
              <p className="text-sm text-[var(--muted)]">No conversions found.</p>
            ) : null}
          </div>
        </div>
      </div>
    </section>
  );
}

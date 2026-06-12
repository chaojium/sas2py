"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Image from "next/image";
import { useSearchParams } from "next/navigation";
import AuthButton from "@/components/AuthButton";
import { useAuth } from "@/components/AuthProvider";
import CodeBlock from "@/components/CodeBlock";
import { authFetch } from "@/lib/firebase/auth-fetch";

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
  artifacts?: {
    name: string;
    contentType: string;
    sizeBytes: number;
    downloadUrl?: string;
    contentBase64?: string;
  }[];
  createdAt: string;
};

type SasAnalysis = {
  interpretation: string;
  expectedOutput: string;
  validationChecks: string[];
};

type ConversationMessage = {
  id: string;
  role: "user" | "assistant" | string;
  content: string;
  messageType: string | null;
  createdAt: string;
};

type ConversationThread = {
  id: string;
  codeEntryId: string;
  title: string | null;
  createdAt: string;
  updatedAt: string | null;
  messages: ConversationMessage[];
};

type Enhancement = {
  id: string;
  language: "PYTHON" | "R" | string;
  instruction: string;
  previousCode: string;
  updatedCode: string;
  createdAt: string;
};

type Entry = {
  id: string;
  name: string;
  language: "PYTHON" | "R";
  sasCode: string;
  pythonCode: string;
  additionalGuidance?: string | null;
  referenceUrl?: string | null;
  createdAt: string;
  enhancements: Enhancement[];
  reviews: Review[];
  runs: Run[];
};

type EntryGroup = {
  id: string;
  name: string;
  sasCode: string;
  createdAt: string;
  entries: Entry[];
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
  artifacts?: {
    name: string;
    contentType: string;
    sizeBytes: number;
    downloadUrl?: string;
    contentBase64?: string;
  }[];
  backend?: "databricks" | "docker";
};

type ExecuteApiResponse = {
  pending?: boolean;
  token?: string;
  result?: ExecutionResult;
  error?: string;
  statusMessage?: string;
};

type ConvertApiResponse = {
  entry: Entry & {
    userId?: string;
    sasAnalysis?: SasAnalysis;
  };
  reusedExisting?: boolean;
  error?: string;
};

async function parseApiResponse<T>(
  response: Response,
): Promise<{ data: T | null; text: string }> {
  const text = await response.text();
  if (!text) {
    return { data: null, text: "" };
  }

  try {
    return { data: JSON.parse(text) as T, text };
  } catch {
    return { data: null, text };
  }
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildPreview(text: string, maxLength = 280) {
  const normalized = text.trim();
  if (normalized.length <= maxLength) {
    return { text: normalized, truncated: false };
  }
  return {
    text: `${normalized.slice(0, maxLength).trimEnd()}...`,
    truncated: true,
  };
}

function runToExecutionResult(run: Run): ExecutionResult {
  return {
    stdout: run.stdout,
    stderr: run.stderr,
    exitCode: run.exitCode,
    timedOut: run.timedOut,
    durationMs: run.durationMs,
    images: [],
    artifacts: run.artifacts || [],
  };
}

function formatArtifactSize(sizeBytes: number) {
  if (sizeBytes < 1024) return `${sizeBytes} B`;
  if (sizeBytes < 1024 * 1024) return `${(sizeBytes / 1024).toFixed(1)} KB`;
  return `${(sizeBytes / (1024 * 1024)).toFixed(1)} MB`;
}

function buildEntryGroupKey(entry: Entry) {
  return `${entry.name.trim().toLowerCase()}::${entry.sasCode.trim()}`;
}

function getChangedLineNumbers(previousCode: string, nextCode: string) {
  const previousLines = previousCode.split("\n");
  const nextLines = nextCode.split("\n");
  const rows = previousLines.length;
  const cols = nextLines.length;
  const lcs = Array.from({ length: rows + 1 }, () =>
    Array<number>(cols + 1).fill(0),
  );

  for (let row = rows - 1; row >= 0; row -= 1) {
    for (let col = cols - 1; col >= 0; col -= 1) {
      lcs[row][col] =
        previousLines[row] === nextLines[col]
          ? lcs[row + 1][col + 1] + 1
          : Math.max(lcs[row + 1][col], lcs[row][col + 1]);
    }
  }

  const changed: number[] = [];
  let row = 0;
  let col = 0;

  while (row < rows && col < cols) {
    if (previousLines[row] === nextLines[col]) {
      row += 1;
      col += 1;
      continue;
    }

    if (lcs[row + 1][col] >= lcs[row][col + 1]) {
      row += 1;
    } else {
      changed.push(col + 1);
      col += 1;
    }
  }

  while (col < cols) {
    changed.push(col + 1);
    col += 1;
  }

  return changed;
}

export default function Converter() {
  const { status } = useAuth();
  const searchParams = useSearchParams();
  const isAuthed = status === "authenticated";
  const [sasCode, setSasCode] = useState("");
  const [name, setName] = useState("");
  const [language, setLanguage] = useState<"PYTHON" | "R">("PYTHON");
  const [additionalGuidance, setAdditionalGuidance] = useState("");
  const [referenceUrl, setReferenceUrl] = useState("");
  const [pythonCode, setPythonCode] = useState("");
  const [savedPythonCode, setSavedPythonCode] = useState("");
  const [currentEntryId, setCurrentEntryId] = useState<string | null>(null);
  const [entries, setEntries] = useState<Entry[]>([]);
  const [expandedIds, setExpandedIds] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState(false);
  const [saveCodeLoading, setSaveCodeLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reviewDrafts, setReviewDrafts] = useState<Record<string, Draft>>({});
  const [enhancePrompt, setEnhancePrompt] = useState("");
  const [enhanceLoading, setEnhanceLoading] = useState(false);
  const [fullScreen, setFullScreen] = useState(false);
  const [isEditingCode, setIsEditingCode] = useState(false);
  const [executeLoading, setExecuteLoading] = useState(false);
  const [executeError, setExecuteError] = useState<string | null>(null);
  const [executeResult, setExecuteResult] = useState<ExecutionResult | null>(
    null,
  );
  const [executionInputFiles, setExecutionInputFiles] = useState<File[]>([]);
  const [confirmClearExecutionFiles, setConfirmClearExecutionFiles] =
    useState(false);
  const [sasAnalysisByEntry, setSasAnalysisByEntry] = useState<
    Record<string, SasAnalysis>
  >({});
  const [draftSasAnalysis, setDraftSasAnalysis] = useState<SasAnalysis | null>(
    null,
  );
  const [sasAnalysisLoading, setSasAnalysisLoading] = useState(false);
  const [sasAnalysisError, setSasAnalysisError] = useState<string | null>(null);
  const [uploadedSasFileName, setUploadedSasFileName] = useState<string | null>(
    null,
  );
  const [conversationThreads, setConversationThreads] = useState<
    Record<string, ConversationThread[]>
  >({});
  const [conversationPrompt, setConversationPrompt] = useState("");
  const [conversationLoading, setConversationLoading] = useState(false);
  const [conversationError, setConversationError] = useState<string | null>(null);
  const [applySuggestionLoadingId, setApplySuggestionLoadingId] = useState<
    string | null
  >(null);
  const [showAllConversationMessages, setShowAllConversationMessages] =
    useState(false);
  const [showAllEnhancements, setShowAllEnhancements] = useState(false);
  const [expandedConversationMessages, setExpandedConversationMessages] =
    useState<Record<string, boolean>>({});
  const [expandedEnhancements, setExpandedEnhancements] = useState<
    Record<string, boolean>
  >({});
  const [highlightedCodeLines, setHighlightedCodeLines] = useState<number[]>(
    [],
  );
  const [forceRegenerate, setForceRegenerate] = useState(false);
  const sasLineNumberRef = useRef<HTMLDivElement | null>(null);

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
    const response = await authFetch("/api/conversions");
    if (!response.ok) return;
    const { data } = await parseApiResponse<{
      entries?: Entry[];
    }>(response);
    setEntries(data?.entries || []);
  }, [isAuthed]);

  useEffect(() => {
    void fetchEntries();
  }, [fetchEntries]);

  const fetchConversationThreads = useCallback(
    async (entryId: string) => {
      const response = await authFetch(
        `/api/conversations?codeEntryId=${encodeURIComponent(entryId)}`,
      );
      const { data, text } = await parseApiResponse<{
        conversations?: ConversationThread[];
        error?: string;
      }>(response);
      if (!response.ok) {
        throw new Error(
          data?.error || text || "Conversation history failed to load.",
        );
      }
      const conversations = (data?.conversations || []) as ConversationThread[];
      setConversationThreads((prev) => ({
        ...prev,
        [entryId]: conversations,
      }));
      return conversations;
    },
    [],
  );

  useEffect(() => {
    if (!isAuthed || !currentEntryId) {
      return;
    }
    void fetchConversationThreads(currentEntryId).catch((error) => {
      setConversationError(
        error instanceof Error
          ? error.message
          : "Conversation history failed to load.",
      );
    });
  }, [currentEntryId, fetchConversationThreads, isAuthed]);

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
    setSavedPythonCode("");
    setCurrentEntryId(null);
    setIsEditingCode(false);
    setDraftSasAnalysis(null);
    try {
      const response = await authFetch("/api/conversions", {
        method: "POST",
        body: JSON.stringify({
          sasCode,
          name,
          language,
          forceRegenerate,
          additionalGuidance,
          referenceUrl,
        }),
      });
      const { data, text } = await parseApiResponse<ConvertApiResponse>(
        response,
      );
      if (!response.ok) {
        throw new Error(
          data?.error ||
            text ||
            `Conversion failed with status ${response.status}.`,
        );
      }
      if (!data?.entry) {
        throw new Error(
          text || "Conversion returned an empty or invalid response.",
        );
      }
      setPythonCode(data.entry.pythonCode);
      setSavedPythonCode(data.entry.pythonCode);
      setHighlightedCodeLines([]);
      setExecuteResult(null);
      setExecuteError(null);
      setCurrentEntryId(data.entry.id);
      if (data.entry.sasAnalysis) {
        const sasAnalysis = data.entry.sasAnalysis;
        setSasAnalysisByEntry((prev) => ({
          ...prev,
          [data.entry.id]: sasAnalysis,
        }));
        setDraftSasAnalysis(sasAnalysis);
        setSasAnalysisError(null);
      }
      setLanguage(data.entry.language || "PYTHON");
      setName(data.entry.name || name);
      setAdditionalGuidance(data.entry.additionalGuidance || "");
      setReferenceUrl(data.entry.referenceUrl || "");
      setConversationPrompt("");
      setConversationError(null);
      setShowAllConversationMessages(false);
      setShowAllEnhancements(false);
      setExpandedConversationMessages({});
      setExpandedEnhancements({});
      if (data.reusedExisting) {
        setError(
          "Reused the latest saved translation for this same SAS code and language instead of generating a new one.",
        );
      } else if (forceRegenerate) {
        setError("Generated a fresh translation by bypassing saved-result reuse.");
      }
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
      const response = await authFetch("/api/reviews", {
        method: "POST",
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
    setEnhanceLoading(true);
    setError(null);
    try {
      const response = await authFetch("/api/conversions", {
        method: "PATCH",
        body: JSON.stringify({
          entryId: currentEntryId,
          instruction: enhancePrompt,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || "Enhancement failed.");
      }
      const nextCode = data.entry.pythonCode as string;
      setHighlightedCodeLines(getChangedLineNumbers(pythonCode, nextCode));
      setPythonCode(data.entry.pythonCode);
      setSavedPythonCode(data.entry.pythonCode);
      setIsEditingCode(false);
      if (data.entry.enhancements) {
        setEntries((prev) =>
          prev.map((entry) =>
            entry.id === currentEntryId
              ? { ...entry, enhancements: data.entry.enhancements }
              : entry,
          ),
        );
      }
      await fetchEntries();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Enhancement failed.");
    } finally {
      setEnhanceLoading(false);
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
      setDraftSasAnalysis(null);
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

  const handleSaveEditedCode = async () => {
    if (!currentEntryId || !pythonCode.trim()) {
      setError("No edited code to save.");
      return;
    }

    setSaveCodeLoading(true);
    setError(null);
    try {
      const response = await authFetch("/api/conversions", {
        method: "PATCH",
        body: JSON.stringify({
          entryId: currentEntryId,
          pythonCode,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || "Saving edited code failed.");
      }
      const nextCode = data.entry.pythonCode as string;
      setHighlightedCodeLines(getChangedLineNumbers(savedPythonCode, nextCode));
      setPythonCode(data.entry.pythonCode);
      setSavedPythonCode(data.entry.pythonCode);
      setIsEditingCode(false);
      await fetchEntries();
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Saving edited code failed.",
      );
    } finally {
      setSaveCodeLoading(false);
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

  const getExecutionOutputFileName = (kind: "stdout" | "stderr") => {
    const rawName =
      currentEntry?.name?.trim() ||
      name.trim() ||
      uploadedSasFileName?.replace(/\.sas$/i, "").trim() ||
      "execution_output";
    const safeName = rawName.replace(/[^\w.-]+/g, "_");
    return `${safeName}.${kind}.txt`;
  };

  const handleCopyExecutionOutput = (text: string) => {
    navigator.clipboard.writeText(text || "(no output)");
  };

  const handlePasteExecutionOutputToPrompt = (
    kind: "stdout" | "stderr",
    text: string,
  ) => {
    const label = kind === "stdout" ? "STDOUT" : "STDERR";
    const content = text || "(no output)";
    setConversationPrompt((previous) => {
      const prefix = previous.trim() ? `${previous.trim()}\n\n` : "";
      return `${prefix}${label}:\n${content}`;
    });
  };

  const handleDownloadExecutionOutput = (
    kind: "stdout" | "stderr",
    text: string,
  ) => {
    const blob = new Blob([text || "(no output)"], {
      type: "text/plain;charset=utf-8",
    });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = getExecutionOutputFileName(kind);
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
      const response = executionInputFiles.length > 0
        ? await (() => {
            const formData = new FormData();
            formData.set("code", pythonCode);
            formData.set("language", language);
            if (currentEntryId) {
              formData.set("codeEntryId", currentEntryId);
            }
            for (const file of executionInputFiles) {
              formData.append("inputFiles", file);
            }
            return authFetch("/api/execute", {
              method: "POST",
              body: formData,
            });
          })()
        : await authFetch("/api/execute", {
            method: "POST",
            body: JSON.stringify({
              code: pythonCode,
              language,
              codeEntryId: currentEntryId,
            }),
          });
      const { data, text } = await parseApiResponse<ExecuteApiResponse>(
        response,
      );
      if (!response.ok) {
        throw new Error(
          data?.error ||
            text ||
            "Execution failed.",
        );
      }
      if (!data) {
        throw new Error("Execution returned an invalid response.");
      }
      if (data.pending && data.token) {
        let pollResult: ExecutionResult | null = null;
        for (let attempt = 0; attempt < 180; attempt += 1) {
          await sleep(2000);
          const pollResponse = await authFetch(
            `/api/execute?token=${encodeURIComponent(data.token)}`,
          );
          const poll = await parseApiResponse<ExecuteApiResponse>(pollResponse);
          if (!pollResponse.ok) {
            throw new Error(
              poll.data?.error ||
                poll.text ||
                "Execution polling failed.",
            );
          }
          if (!poll.data) {
            throw new Error("Execution polling returned an invalid response.");
          }
          if (!poll.data.pending) {
            pollResult = poll.data.result || null;
            break;
          }
        }
        if (!pollResult) {
          throw new Error("Execution is still running. Please try again shortly.");
        }
        setExecuteResult(pollResult);
      } else {
        setExecuteResult(data.result || null);
      }
      await fetchEntries();
    } catch (err) {
      setExecuteError(err instanceof Error ? err.message : "Execution failed.");
    } finally {
      setExecuteLoading(false);
    }
  };

  const handleDownloadArtifact = (artifact: NonNullable<ExecutionResult["artifacts"]>[number]) => {
    if (artifact.downloadUrl) {
      window.open(artifact.downloadUrl, "_blank", "noopener,noreferrer");
      return;
    }
    if (!artifact.contentBase64) {
      return;
    }

    const binary = atob(artifact.contentBase64);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    const blob = new Blob([bytes], {
      type: artifact.contentType || "application/octet-stream",
    });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = artifact.name.split("/").pop() || artifact.name;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  };

  const fetchSasAnalysis = async (
    sourceSasCode: string,
    entryId?: string | null,
  ) => {
    const response = await authFetch("/api/sas-analysis", {
      method: "POST",
      body: JSON.stringify({
        entryId: entryId || undefined,
        sasCode: sourceSasCode,
      }),
    });
    const { data, text } = await parseApiResponse<{
      analysis?: SasAnalysis;
      error?: string;
    }>(response);
    if (!response.ok) {
      throw new Error(data?.error || text || "SAS analysis failed.");
    }
    if (!data?.analysis) {
      throw new Error(text || "SAS analysis returned an invalid response.");
    }
    return data.analysis as SasAnalysis;
  };

  const handleExecutionFileUpload = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const files = Array.from(event.target.files || []).filter(
      (file) => file.size > 0,
    );
    if (files.length === 0) {
      event.target.value = "";
      return;
    }
    setExecutionInputFiles((prev) => [...prev, ...files]);
    setExecuteError(null);
    setConfirmClearExecutionFiles(false);
    event.target.value = "";
  };

  const handleRemoveExecutionInputFile = (targetIndex: number) => {
    setExecutionInputFiles((prev) =>
      prev.filter((_, index) => index !== targetIndex),
    );
    setConfirmClearExecutionFiles(false);
  };

  const handleClearExecutionInputFiles = () => {
    if (!confirmClearExecutionFiles) {
      setConfirmClearExecutionFiles(true);
      return;
    }
    setExecutionInputFiles([]);
    setConfirmClearExecutionFiles(false);
  };

  const handleGenerateSasAnalysis = async () => {
    const sourceSasCode = currentEntry?.sasCode || sasCode;
    if (!sourceSasCode.trim()) {
      setSasAnalysisError("No SAS code available to analyze.");
      return;
    }

    setSasAnalysisLoading(true);
    setSasAnalysisError(null);
    try {
      const analysis = await fetchSasAnalysis(sourceSasCode, currentEntryId);
      if (currentEntryId) {
        setSasAnalysisByEntry((prev) => ({
          ...prev,
          [currentEntryId]: analysis,
        }));
      } else {
        setDraftSasAnalysis(analysis);
      }
    } catch (err) {
      setSasAnalysisError(
        err instanceof Error ? err.message : "SAS analysis failed.",
      );
    } finally {
      setSasAnalysisLoading(false);
    }
  };

  const handleConversationSend = async () => {
    if (!currentEntryId || !conversationPrompt.trim()) {
      setConversationError("Add a question or error message first.");
      return;
    }

    setConversationLoading(true);
    setConversationError(null);
    try {
      const latestConversation =
        conversationThreads[currentEntryId]?.[0] || null;
      const response = await authFetch("/api/conversations", {
        method: "POST",
        body: JSON.stringify({
          codeEntryId: currentEntryId,
          conversationId: latestConversation?.id,
          message: conversationPrompt,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || "Conversation request failed.");
      }
      const conversation = data.conversation as ConversationThread | undefined;
      if (conversation) {
        setConversationThreads((prev) => ({
          ...prev,
          [currentEntryId]: [
            conversation,
            ...(prev[currentEntryId] || []).filter(
              (thread) => thread.id !== conversation.id,
            ),
          ],
        }));
      } else {
        await fetchConversationThreads(currentEntryId);
      }
      setConversationPrompt("");
      setShowAllConversationMessages(false);
      setShowAllEnhancements(false);
      setExpandedConversationMessages({});
    } catch (err) {
      setConversationError(
        err instanceof Error ? err.message : "Conversation request failed.",
      );
    } finally {
      setConversationLoading(false);
    }
  };

  const handleApplyAssistantSuggestion = async (message: ConversationMessage) => {
    if (!currentEntryId || message.role !== "assistant") {
      return;
    }

    setApplySuggestionLoadingId(message.id);
    setError(null);
    try {
      const response = await authFetch("/api/conversions", {
        method: "PATCH",
        body: JSON.stringify({
          entryId: currentEntryId,
          instruction: message.content,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || "Applying assistant suggestion failed.");
      }
      const nextCode = data.entry.pythonCode as string;
      setHighlightedCodeLines(getChangedLineNumbers(pythonCode, nextCode));
      setPythonCode(data.entry.pythonCode);
      setSavedPythonCode(data.entry.pythonCode);
      setIsEditingCode(false);
      if (data.entry.enhancements) {
        setEntries((prev) =>
          prev.map((entry) =>
            entry.id === currentEntryId
              ? { ...entry, pythonCode: data.entry.pythonCode, enhancements: data.entry.enhancements }
              : entry,
          ),
        );
      }
      await fetchEntries();
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Applying assistant suggestion failed.",
      );
    } finally {
      setApplySuggestionLoadingId(null);
    }
  };

  const currentEntry = useMemo(
    () => entries.find((entry) => entry.id === currentEntryId) || null,
    [entries, currentEntryId],
  );
  const canApplyEnhancement =
    Boolean(currentEntryId) && Boolean(enhancePrompt.trim()) && !enhanceLoading;
  const currentSasAnalysis = currentEntryId
    ? sasAnalysisByEntry[currentEntryId] || null
    : draftSasAnalysis;
  const currentConversation =
    currentEntryId && conversationThreads[currentEntryId]?.length
      ? conversationThreads[currentEntryId][0]
      : null;
  const groupedEntries = useMemo<EntryGroup[]>(() => {
    const groups = new Map<string, EntryGroup>();

    for (const entry of entries) {
      const key = buildEntryGroupKey(entry);
      const existing = groups.get(key);
      if (existing) {
        existing.entries.push(entry);
        if (
          new Date(entry.createdAt).getTime() >
          new Date(existing.createdAt).getTime()
        ) {
          existing.createdAt = entry.createdAt;
        }
      } else {
        groups.set(key, {
          id: key,
          name: entry.name,
          sasCode: entry.sasCode,
          createdAt: entry.createdAt,
          entries: [entry],
        });
      }
    }

    return Array.from(groups.values())
      .map((group) => ({
        ...group,
        entries: [...group.entries].sort(
          (a, b) =>
            new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime(),
        ),
      }))
      .sort(
        (a, b) =>
          new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime(),
      );
  }, [entries]);
  const visibleConversationMessages = showAllConversationMessages
    ? currentConversation?.messages || []
    : (currentConversation?.messages || []).slice(-4);
  const visibleEnhancements = showAllEnhancements
    ? currentEntry?.enhancements || []
    : (currentEntry?.enhancements || []).slice(0, 4);
  const sasLineNumbers = useMemo(() => {
    const lineCount = Math.max(1, sasCode.split("\n").length);
    return Array.from({ length: lineCount }, (_, index) => index + 1);
  }, [sasCode]);

  const handleViewEntry = useCallback(async (entry: Entry) => {
    setSasCode(entry.sasCode);
    setPythonCode(entry.pythonCode);
    setSavedPythonCode(entry.pythonCode);
    setExecuteResult(
      entry.runs.length > 0 ? runToExecutionResult(entry.runs[0]) : null,
    );
    setExecuteError(null);
    setExecutionInputFiles([]);
    setUploadedSasFileName(null);
    setCurrentEntryId(entry.id);
    setConversationPrompt("");
    setConversationError(null);
    setShowAllConversationMessages(false);
    setShowAllEnhancements(false);
    setExpandedConversationMessages({});
    setExpandedEnhancements({});
    setName(entry.name);
    setLanguage(entry.language);
    setAdditionalGuidance(entry.additionalGuidance || "");
    setReferenceUrl(entry.referenceUrl || "");
    setIsEditingCode(false);
    setSasAnalysisError(null);
    setHighlightedCodeLines([]);
  }, []);

  useEffect(() => {
    const requestedEntryId = searchParams.get("entryId");
    if (!requestedEntryId || entries.length === 0) return;
    if (currentEntryId === requestedEntryId) return;
    const entry = entries.find((item) => item.id === requestedEntryId);
    if (!entry) return;
    void handleViewEntry(entry);
  }, [currentEntryId, entries, handleViewEntry, searchParams]);

  if (!isAuthed) {
    return (
      <section className="glass-card fade-up rounded-3xl p-8 md:p-12">
        <div className="flex flex-col gap-6">
          <h2 className="text-2xl font-semibold">
            Sign in to start converting SAS to Python or R.
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
      <div className="grid gap-8 lg:grid-cols-[1.35fr_0.65fr]">
        <div className="glass-card min-w-0 rounded-3xl p-6 md:p-10">
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
            Paste SAS code, run the conversion, then annotate the output with
            review notes.
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
              className="min-h-[110px] w-full rounded-2xl border border-[var(--border)] bg-white/80 p-4 text-sm shadow-inner focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
              placeholder="Additional guidance for translation, such as method notes, constraints, or expected statistical approach..."
              value={additionalGuidance}
              onChange={(event) => setAdditionalGuidance(event.target.value)}
            />
            <input
              className="w-full rounded-2xl border border-[var(--border)] bg-white/80 px-4 py-3 text-sm shadow-inner focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
              placeholder="Reference URL (optional), e.g. CDC method page"
              value={referenceUrl}
              onChange={(event) => setReferenceUrl(event.target.value)}
            />
            <div className="overflow-hidden rounded-2xl border border-[var(--border)] bg-white/80 shadow-inner focus-within:ring-2 focus-within:ring-[var(--secondary)]">
              <div className="flex min-w-0">
                <div
                  ref={sasLineNumberRef}
                  className="max-h-[420px] min-h-[220px] shrink-0 overflow-hidden border-r border-[var(--border)] bg-[color:color-mix(in_oklab,var(--foreground)_4%,white)] px-3 py-4 font-mono text-xs leading-6 text-[var(--muted)]"
                >
                  {sasLineNumbers.map((lineNumber) => (
                    <div key={lineNumber} className="text-right">
                      {lineNumber}
                    </div>
                  ))}
                </div>
                <textarea
                  className="max-h-[420px] min-h-[220px] min-w-0 w-full resize-y border-0 bg-transparent p-4 font-mono text-sm leading-6 focus:outline-none"
                  placeholder="Paste SAS code here..."
                  value={sasCode}
                  onChange={(event) => {
                    setSasCode(event.target.value);
                    if (!currentEntryId) {
                      setDraftSasAnalysis(null);
                    }
                  }}
                  onScroll={(event) => {
                    if (sasLineNumberRef.current) {
                      sasLineNumberRef.current.scrollTop =
                        event.currentTarget.scrollTop;
                    }
                  }}
                  spellCheck={false}
                />
              </div>
            </div>
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
            {(currentEntry || sasCode.trim()) && (
              <div className="rounded-2xl border border-[var(--border)] bg-white/70 p-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                    SAS interpretation
                  </h4>
                  <button
                    onClick={handleGenerateSasAnalysis}
                    disabled={sasAnalysisLoading}
                    className="rounded-full border border-[var(--border)] px-4 py-2 text-xs uppercase tracking-[0.2em] text-[var(--muted)] transition hover:bg-white/70 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {sasAnalysisLoading ? "Analyzing..." : "Analyze SAS"}
                  </button>
                </div>
                {sasAnalysisError ? (
                  <p className="mt-3 text-sm text-red-600">{sasAnalysisError}</p>
                ) : null}
                {currentSasAnalysis ? (
                  <div className="mt-3 space-y-4">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                        Interpretation
                      </p>
                      <p className="mt-2 text-sm leading-6">
                        {currentSasAnalysis.interpretation}
                      </p>
                    </div>
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                        Expected output
                      </p>
                      <p className="mt-2 text-sm leading-6">
                        {currentSasAnalysis.expectedOutput}
                      </p>
                    </div>
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                        Validation checks
                      </p>
                      <ul className="mt-2 space-y-2 text-sm leading-6 text-[var(--foreground)]">
                        {currentSasAnalysis.validationChecks.map((check, index) => (
                          <li key={`${index}-${check}`}>- {check}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                ) : (
                  <p className="mt-3 text-sm text-[var(--muted)]">
                    Generate a plain-language interpretation of the SAS logic and
                    a summary of the expected output for validation.
                  </p>
                )}
              </div>
            )}
            <div className="flex flex-wrap items-center gap-3">
              <button
                onClick={handleConvert}
                disabled={!canConvert}
                className="rounded-full bg-[var(--primary)] px-6 py-2.5 text-sm font-semibold text-white transition hover:translate-y-[-1px] disabled:cursor-not-allowed disabled:opacity-50"
              >
              {loading ? "Running GPT-5.5..." : "Convert"}
            </button>
              <button
                onClick={() => {
                  setSasCode("");
                  setDraftSasAnalysis(null);
                }}
                className="rounded-full border border-[var(--border)] px-4 py-2 text-sm text-[var(--muted)] transition hover:bg-white/70"
              >
                Clear
              </button>
              {error ? (
                <span className="text-sm text-red-600">{error}</span>
              ) : null}
            </div>
            <label className="flex items-center gap-2 text-sm text-[var(--muted)]">
              <input
                type="checkbox"
                checked={forceRegenerate}
                onChange={(event) => setForceRegenerate(event.target.checked)}
                className="h-4 w-4 rounded border-[var(--border)]"
              />
              Force regenerate instead of reusing the latest saved translation
            </label>
          </div>
            <div className="mt-8 min-w-0">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">
                {language === "R" ? "R output" : "Python output"}
              </h3>
              {pythonCode ? (
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => {
                      if (isEditingCode) {
                        setPythonCode(savedPythonCode);
                        setIsEditingCode(false);
                        return;
                      }
                      setIsEditingCode(true);
                    }}
                    className="text-xs uppercase tracking-[0.2em] text-[var(--muted)] hover:text-[var(--foreground)]"
                  >
                    {isEditingCode ? "Cancel edit" : "Edit"}
                  </button>
                  {currentEntryId ? (
                    <button
                      onClick={handleSaveEditedCode}
                      disabled={
                        saveCodeLoading ||
                        !isEditingCode ||
                        pythonCode === savedPythonCode
                      }
                      className="text-xs uppercase tracking-[0.2em] text-[var(--muted)] hover:text-[var(--foreground)] disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {saveCodeLoading ? "Saving..." : "Save edits"}
                    </button>
                  ) : null}
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
              {isEditingCode ? (
                <textarea
                  className="min-h-[320px] w-full rounded-2xl border border-[var(--border)] bg-white/80 p-4 font-mono text-sm shadow-inner focus:outline-none focus:ring-2 focus:ring-[var(--secondary)]"
                  value={pythonCode}
                  onChange={(event) => setPythonCode(event.target.value)}
                  spellCheck={false}
                />
              ) : (
                <CodeBlock
                  code={
                    pythonCode ||
                    `Your converted ${language === "R" ? "R" : "Python"} will appear here once GPT-5.5 finishes the translation.`
                  }
                  language={language === "R" ? "r" : "python"}
                  maxHeight={320}
                  showLineNumbers
                  highlightedLines={highlightedCodeLines}
                />
              )}
            </div>
            <div className="mt-4 flex flex-wrap items-center gap-3">
              <label className="cursor-pointer rounded-full border border-[var(--border)] px-4 py-2 text-sm text-[var(--muted)] transition hover:bg-white/70">
                Upload input file
                <input
                  type="file"
                  multiple
                  className="hidden"
                  onChange={handleExecutionFileUpload}
                />
              </label>
              {executionInputFiles.length > 0 ? (
                <>
                  <span className="text-xs text-[var(--muted)]">
                    {executionInputFiles.length} input file
                    {executionInputFiles.length === 1 ? "" : "s"} attached
                    {" "}for execution
                  </span>
                  <button
                    onClick={handleClearExecutionInputFiles}
                    className="text-xs uppercase tracking-[0.2em] text-[var(--muted)] hover:text-[var(--foreground)]"
                  >
                    {confirmClearExecutionFiles ? "Confirm clear" : "Clear files"}
                  </button>
                </>
              ) : null}
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
            {executionInputFiles.length > 0 ? (
              <div className="mt-3 rounded-2xl border border-[var(--border)] bg-white/70 p-4">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                  Execution input files
                </p>
                <p className="mt-2 text-sm text-[var(--muted)]">
                  Uploaded files are exposed to the execution runtime through
                  the <code>SAS2PY_INPUT_DIR</code> folder. Docker runs use
                  <code>/workspace/input</code>. Databricks runs use
                  <code>/tmp/sas2py-input</code>.
                </p>
                <div className="mt-3 space-y-2">
                  {executionInputFiles.map((file, index) => (
                    <div
                      key={`${file.name}-${file.size}-${index}`}
                      className="flex items-center justify-between gap-3 rounded-xl border border-[var(--border)] bg-white/80 px-3 py-2 text-sm"
                    >
                      <span className="min-w-0 flex-1 truncate">{file.name}</span>
                      <button
                        onClick={() => handleRemoveExecutionInputFile(index)}
                        className="text-xs uppercase tracking-[0.2em] text-[var(--muted)] hover:text-red-600"
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
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
                    {executeResult.backend
                      ? ` | backend: ${executeResult.backend}`
                      : ""}
                  </p>
                </div>
                <div className="mt-3 space-y-3">
                  <div>
                    <div className="mb-1 flex flex-wrap items-center justify-between gap-2">
                      <p className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                        Stdout
                      </p>
                      <div className="flex flex-wrap items-center gap-2">
                        <button
                          onClick={() =>
                            handleCopyExecutionOutput(executeResult.stdout)
                          }
                          className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                        >
                          Copy
                        </button>
                        <button
                          onClick={() =>
                            handlePasteExecutionOutputToPrompt(
                              "stdout",
                              executeResult.stdout,
                            )
                          }
                          className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                        >
                          Paste to prompt
                        </button>
                        <button
                          onClick={() =>
                            handleDownloadExecutionOutput(
                              "stdout",
                              executeResult.stdout,
                            )
                          }
                          className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                        >
                          Download
                        </button>
                      </div>
                    </div>
                    <CodeBlock
                      code={executeResult.stdout || "(no output)"}
                      language="text"
                      maxHeight={160}
                      wrapLongLines
                    />
                  </div>
                  <div>
                    <div className="mb-1 flex flex-wrap items-center justify-between gap-2">
                      <p className="text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                        Stderr
                      </p>
                      <div className="flex flex-wrap items-center gap-2">
                        <button
                          onClick={() =>
                            handleCopyExecutionOutput(executeResult.stderr)
                          }
                          className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                        >
                          Copy
                        </button>
                        <button
                          onClick={() =>
                            handlePasteExecutionOutputToPrompt(
                              "stderr",
                              executeResult.stderr,
                            )
                          }
                          className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                        >
                          Paste to prompt
                        </button>
                        <button
                          onClick={() =>
                            handleDownloadExecutionOutput(
                              "stderr",
                              executeResult.stderr,
                            )
                          }
                          className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                        >
                          Download
                        </button>
                      </div>
                    </div>
                    <CodeBlock
                      code={executeResult.stderr || "(no output)"}
                      language="text"
                      maxHeight={160}
                      wrapLongLines
                    />
                  </div>
                  {executeResult.artifacts && executeResult.artifacts.length > 0 ? (
                    <div>
                      <p className="mb-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                        Downloadable outputs
                      </p>
                      <div className="space-y-2">
                        {executeResult.artifacts.map((artifact) => (
                          <div
                            key={`${artifact.name}-${artifact.sizeBytes}`}
                            className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-[var(--border)] bg-white/80 px-3 py-2 text-sm"
                          >
                            <div className="min-w-0 flex-1">
                              <p className="truncate font-medium">{artifact.name}</p>
                              <p className="text-xs text-[var(--muted)]">
                                {formatArtifactSize(artifact.sizeBytes)} | {artifact.contentType}
                              </p>
                            </div>
                            <button
                              onClick={() => handleDownloadArtifact(artifact)}
                              className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                            >
                              Download
                            </button>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : null}
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
                      disabled={!canApplyEnhancement}
                      className="rounded-full bg-[var(--foreground)] px-5 py-2 text-sm font-semibold text-[var(--background)] transition hover:translate-y-[-1px] disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {enhanceLoading ? "Applying enhancement..." : "Apply enhancement"}
                    </button>
                  </div>
                  <p className="text-xs text-[var(--muted)]">
                    Your instruction stays here after each run, so you can refine it and apply additional enhancements.
                  </p>
                </div>
              </div>
            ) : null}
            {currentEntryId ? (
              <div className="mt-4 rounded-2xl border border-[var(--border)] bg-white/70 p-4">
                <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                  Ask about this code
                </h4>
                <div className="mt-3 space-y-3">
                  <textarea
                    className="min-h-[100px] w-full rounded-xl border border-[var(--border)] bg-white/90 px-3 py-2 text-sm"
                    placeholder="Paste an error message, traceback, or ask what to change..."
                    value={conversationPrompt}
                    onChange={(event) => setConversationPrompt(event.target.value)}
                  />
                  <div className="flex flex-wrap items-center gap-3">
                    <button
                      onClick={handleConversationSend}
                      disabled={!conversationPrompt.trim() || conversationLoading}
                      className="rounded-full bg-[var(--secondary)] px-5 py-2 text-sm font-semibold text-white transition hover:translate-y-[-1px] disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {conversationLoading ? "Thinking..." : "Ask assistant"}
                    </button>
                    {conversationError ? (
                      <span className="text-sm text-red-600">{conversationError}</span>
                    ) : null}
                  </div>
                  <div className="space-y-3">
                    {currentConversation?.messages?.length ? (
                      <>
                        {visibleConversationMessages.map((message) => (
                          (() => {
                            const preview = buildPreview(message.content);
                            const expanded = expandedConversationMessages[message.id];
                            const displayText =
                              expanded || !preview.truncated
                                ? message.content
                                : preview.text;

                            return (
                              <div
                                key={message.id}
                                className={`rounded-xl border p-3 text-sm ${
                                  message.role === "assistant"
                                    ? "border-[var(--border)] bg-white/85"
                                    : "border-[color:color-mix(in_oklab,var(--secondary)_35%,white)] bg-[color:color-mix(in_oklab,var(--secondary)_10%,white)]"
                                }`}
                              >
                                <div className="mb-2 flex flex-wrap items-center justify-between gap-2 text-xs text-[var(--muted)]">
                                  <span>
                                    {message.role === "assistant" ? "Assistant" : "You"}
                                  </span>
                                  <span>{new Date(message.createdAt).toLocaleString()}</span>
                                </div>
                                <p className="whitespace-pre-wrap">{displayText}</p>
                                {preview.truncated ? (
                                  <button
                                    onClick={() =>
                                      setExpandedConversationMessages((prev) => ({
                                        ...prev,
                                        [message.id]: !prev[message.id],
                                      }))
                                    }
                                    className="mt-2 text-xs uppercase tracking-[0.2em] text-[var(--secondary)]"
                                  >
                                    {expanded ? "View less" : "View more"}
                                  </button>
                                ) : null}
                                {message.role === "assistant" ? (
                                  <div className="mt-3">
                                    <button
                                      onClick={() => handleApplyAssistantSuggestion(message)}
                                      disabled={applySuggestionLoadingId === message.id}
                                      className="rounded-full border border-[var(--border)] px-4 py-2 text-xs uppercase tracking-[0.2em] text-[var(--foreground)] transition hover:bg-white/70 disabled:cursor-not-allowed disabled:opacity-50"
                                    >
                                      {applySuggestionLoadingId === message.id
                                        ? "Applying suggestion..."
                                        : "Apply suggestion"}
                                    </button>
                                  </div>
                                ) : null}
                              </div>
                            );
                          })()
                        ))}
                        {currentConversation.messages.length > 4 ? (
                          <button
                            onClick={() =>
                              setShowAllConversationMessages((prev) => !prev)
                            }
                            className="text-xs uppercase tracking-[0.2em] text-[var(--secondary)]"
                          >
                            {showAllConversationMessages ? "Show less" : "Show more"}
                          </button>
                        ) : null}
                      </>
                    ) : (
                      <p className="text-sm text-[var(--muted)]">
                        No conversation yet. Ask about an error, warning, or desired improvement.
                      </p>
                    )}
                  </div>
                </div>
              </div>
            ) : null}
            {currentEntry?.enhancements?.length ? (
              <div className="mt-4 rounded-2xl border border-[var(--border)] bg-white/70 p-4">
                <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                  Enhancement history
                </h4>
                <div className="mt-3 space-y-3">
                  {visibleEnhancements.map((enhancement) => (
                    (() => {
                      const preview = buildPreview(enhancement.instruction, 180);
                      const expanded = expandedEnhancements[enhancement.id];
                      const displayText =
                        expanded || !preview.truncated
                          ? enhancement.instruction
                          : preview.text;

                      return (
                        <div
                          key={enhancement.id}
                          className="rounded-xl border border-[var(--border)] bg-white/80 p-3 text-sm"
                        >
                          <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-[var(--muted)]">
                            <span>{new Date(enhancement.createdAt).toLocaleString()}</span>
                            <span>{enhancement.language}</span>
                          </div>
                          <p className="mt-2 font-medium whitespace-pre-wrap">{displayText}</p>
                          {preview.truncated ? (
                            <button
                              onClick={() =>
                                setExpandedEnhancements((prev) => ({
                                  ...prev,
                                  [enhancement.id]: !prev[enhancement.id],
                                }))
                              }
                              className="mt-2 text-xs uppercase tracking-[0.2em] text-[var(--secondary)]"
                            >
                              {expanded ? "View less" : "View more"}
                            </button>
                          ) : null}
                        </div>
                      );
                    })()
                  ))}
                  {currentEntry.enhancements.length > 4 ? (
                    <button
                      onClick={() => setShowAllEnhancements((prev) => !prev)}
                      className="text-xs uppercase tracking-[0.2em] text-[var(--secondary)]"
                    >
                      {showAllEnhancements ? "Show less" : "Show more"}
                    </button>
                  ) : null}
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
        <div className="glass-card min-w-0 rounded-3xl p-6 md:p-10">
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
            {groupedEntries.length === 0 ? (
              <div className="rounded-2xl border border-dashed border-[var(--border)] p-6 text-sm text-[var(--muted)]">
                No conversions yet. Paste SAS code to create your first entry.
              </div>
            ) : null}
            {groupedEntries.map((group) => (
              <div
                key={group.id}
                className="rounded-2xl border border-[var(--border)] bg-white/80 p-5"
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <button
                      className="text-left text-lg font-semibold hover:text-[var(--secondary)]"
                      onClick={() =>
                        setExpandedIds((prev) => ({
                          ...prev,
                          [group.id]: !prev[group.id],
                        }))
                      }
                    >
                      {group.name}
                    </button>
                    <p className="text-xs text-[var(--muted)]">
                      {new Date(group.createdAt).toLocaleString()}
                    </p>
                    <p className="mt-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                      {group.entries.length} version{group.entries.length === 1 ? "" : "s"}
                    </p>
                  </div>
                  <button
                    className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                    onClick={() => void handleViewEntry(group.entries[0])}
                  >
                    View latest
                  </button>
                </div>
                {expandedIds[group.id] ? (
                  <div className="mt-4 space-y-4">
                    <div>
                      <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                        SAS Source
                      </h4>
                      <div className="mt-2">
                        <CodeBlock
                          code={group.sasCode}
                          language="sas"
                          maxHeight={144}
                        />
                      </div>
                    </div>
                    <div className="space-y-4">
                      {group.entries.map((entry) => (
                        <div
                          key={entry.id}
                          className="rounded-xl border border-[var(--border)] bg-white/70 p-4"
                        >
                          <div className="flex flex-wrap items-center justify-between gap-3">
                            <div>
                              <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                                {entry.language === "R" ? "R version" : "Python version"}
                              </h4>
                              <p className="mt-1 text-xs text-[var(--muted)]">
                                {new Date(entry.createdAt).toLocaleString()}
                              </p>
                            </div>
                            <button
                              className="rounded-full border border-[var(--border)] px-3 py-1 text-xs uppercase tracking-[0.2em] text-[var(--muted)]"
                              onClick={() => void handleViewEntry(entry)}
                            >
                              View
                            </button>
                          </div>
                          <div className="mt-3">
                            <CodeBlock
                              code={entry.pythonCode}
                              language={entry.language === "R" ? "r" : "python"}
                              maxHeight={144}
                            />
                          </div>
                          <div className="mt-4 space-y-3">
                            <h4 className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--muted)]">
                              Latest run
                            </h4>
                            {entry.runs.length === 0 ? (
                              <p className="text-sm text-[var(--muted)]">
                                No execution results saved yet.
                              </p>
                            ) : (
                              <div className="rounded-xl border border-[var(--border)] bg-white/70 p-3 text-sm">
                                <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-[var(--muted)]">
                                  <span>{new Date(entry.runs[0].createdAt).toLocaleString()}</span>
                                  <span>
                                    Exit code {entry.runs[0].exitCode ?? "unknown"} in{" "}
                                    {entry.runs[0].durationMs}ms
                                    {entry.runs[0].timedOut ? " (timed out)" : ""}
                                  </span>
                                </div>
                                <p className="mt-2 text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                                  Stdout
                                </p>
                                <div className="mt-2">
                                  <CodeBlock
                                    code={entry.runs[0].stdout || "(no output)"}
                                    language="text"
                                    maxHeight={144}
                                    wrapLongLines
                                  />
                                </div>
                                <p className="mt-3 text-xs uppercase tracking-[0.2em] text-[var(--muted)]">
                                  Stderr
                                </p>
                                <div className="mt-2">
                                  <CodeBlock
                                    code={entry.runs[0].stderr || "(no output)"}
                                    language="text"
                                    maxHeight={144}
                                    wrapLongLines
                                  />
                                </div>
                              </div>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

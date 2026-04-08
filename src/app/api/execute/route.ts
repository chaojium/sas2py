import { NextResponse } from "next/server";
import {
  runCodeInContainer,
  type ExecutionBackend,
  type ExecutionLanguage,
} from "@/lib/codeRunner";
import { execute, table } from "@/lib/databricks";
import { getAuthUser } from "@/lib/firebase/server";
import { randomUUID } from "node:crypto";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const contentType = request.headers.get("content-type") || "";
  let code = "";
  let rawLanguage = "";
  let rawBackend = "";
  let codeEntryId = "";
  let inputFiles: { name: string; content: Buffer }[] = [];

  if (contentType.includes("multipart/form-data")) {
    const formData = await request.formData();
    code = typeof formData.get("code") === "string" ? String(formData.get("code")) : "";
    rawLanguage =
      typeof formData.get("language") === "string"
        ? String(formData.get("language")).trim().toUpperCase()
        : "";
    rawBackend =
      typeof formData.get("backend") === "string"
        ? String(formData.get("backend")).trim().toLowerCase()
        : "";
    codeEntryId =
      typeof formData.get("codeEntryId") === "string"
        ? String(formData.get("codeEntryId")).trim()
        : "";
    const uploaded = formData.getAll("inputFiles");
    inputFiles = await Promise.all(
      uploaded
        .filter((value): value is File => value instanceof File && value.size > 0)
        .map(async (file) => ({
          name: file.name,
          content: Buffer.from(await file.arrayBuffer()),
        })),
    );
  } else {
    const body = await request.json();
    code = typeof body?.code === "string" ? body.code : "";
    rawLanguage =
      typeof body?.language === "string" ? body.language.trim().toUpperCase() : "";
    rawBackend =
      typeof body?.backend === "string" ? body.backend.trim().toLowerCase() : "";
    codeEntryId =
      typeof body?.codeEntryId === "string" ? body.codeEntryId.trim() : "";
  }
  const language: ExecutionLanguage = rawLanguage === "R" ? "R" : "PYTHON";
  const backend = rawBackend
    ? (rawBackend as ExecutionBackend)
    : undefined;

  if (!code.trim()) {
    return NextResponse.json({ error: "Code is required." }, { status: 400 });
  }

  if (rawLanguage && rawLanguage !== "R" && rawLanguage !== "PYTHON") {
    return NextResponse.json(
      { error: "Language must be PYTHON or R." },
      { status: 400 },
    );
  }
  if (rawBackend && rawBackend !== "databricks" && rawBackend !== "docker") {
    return NextResponse.json(
      { error: "Backend must be databricks or docker." },
      { status: 400 },
    );
  }

  try {
    if (codeEntryId) {
      const entryRows = await execute<Record<string, unknown>>(
        `SELECT id FROM ${table("code_entries")} WHERE id = ? AND user_id = ? LIMIT 1`,
        [codeEntryId, user.appUserId],
      );
      if (!entryRows[0]) {
        return NextResponse.json(
          { error: "Code entry not found." },
          { status: 404 },
        );
      }
    }

    const result = await runCodeInContainer(code, language, backend, inputFiles);
    const runId = randomUUID();
    await execute(
      `INSERT INTO ${table(
        "code_runs",
      )} (id, code_entry_id, user_id, language, stdout, stderr, exit_code, timed_out, duration_ms, detected_packages, policy_mode, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp())`,
      [
        runId,
        codeEntryId || null,
        user.appUserId,
        language,
        result.stdout,
        result.stderr,
        result.exitCode,
        result.timedOut,
        result.durationMs,
        result.detectedPackages.join(","),
        result.policyMode,
      ],
    );

    return NextResponse.json({ result });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Code execution failed.";
    const status =
      message.includes("Blocked package(s) detected") ||
      message.includes("Package(s) not in allowlist") ||
      message.includes("allowlist is empty")
        ? 400
        : 500;
    return NextResponse.json(
      {
        error: message,
      },
      { status },
    );
  }
}

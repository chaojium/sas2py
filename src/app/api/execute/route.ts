import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import { runCodeInContainer, type ExecutionLanguage } from "@/lib/codeRunner";
import { execute, table } from "@/lib/databricks";
import { randomUUID } from "node:crypto";

export async function POST(request: Request) {
  const session = await getServerSession(authOptions);
  if (!session?.user?.id) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const code = typeof body?.code === "string" ? body.code : "";
  const rawLanguage =
    typeof body?.language === "string" ? body.language.trim().toUpperCase() : "";
  const codeEntryId =
    typeof body?.codeEntryId === "string" ? body.codeEntryId.trim() : "";
  const language: ExecutionLanguage = rawLanguage === "R" ? "R" : "PYTHON";

  if (!code.trim()) {
    return NextResponse.json({ error: "Code is required." }, { status: 400 });
  }

  if (rawLanguage && rawLanguage !== "R" && rawLanguage !== "PYTHON") {
    return NextResponse.json(
      { error: "Language must be PYTHON or R." },
      { status: 400 },
    );
  }

  try {
    if (codeEntryId) {
      const entryRows = await execute<Record<string, unknown>>(
        `SELECT id FROM ${table("code_entries")} WHERE id = ? AND user_id = ? LIMIT 1`,
        [codeEntryId, session.user.id],
      );
      if (!entryRows[0]) {
        return NextResponse.json(
          { error: "Code entry not found." },
          { status: 404 },
        );
      }
    }

    const result = await runCodeInContainer(code, language);
    const runId = randomUUID();
    await execute(
      `INSERT INTO ${table(
        "code_runs",
      )} (id, code_entry_id, user_id, language, stdout, stderr, exit_code, timed_out, duration_ms, detected_packages, policy_mode, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp())`,
      [
        runId,
        codeEntryId || null,
        session.user.id,
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

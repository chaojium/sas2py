import { NextResponse } from "next/server";
import {
  getDatabricksExecutionStatus,
  runCodeInContainer,
  startDatabricksExecution,
  type ExecutionBackend,
  type ExecutionInputFile,
  type ExecutionLanguage,
} from "@/lib/codeRunner";
import { execute, table } from "@/lib/databricks";
import { getAuthUser } from "@/lib/firebase/server";
import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";

export const runtime = "nodejs";
export const maxDuration = 300;

type ExecutionTokenPayload = {
  runId: number;
  language: ExecutionLanguage;
  codeEntryId: string;
  startedAt: number;
  detectedPackages: string[];
  policyMode: "off" | "blocklist" | "allowlist";
};

function getExecutionTokenSecret() {
  const secret =
    process.env.DATABRICKS_ACCESS_TOKEN ||
    process.env.CODE_RUNNER_DATABRICKS_PYTHON_TOKEN ||
    process.env.CODE_RUNNER_DATABRICKS_R_TOKEN;
  if (!secret) {
    throw new Error("Missing Databricks token secret for execution polling.");
  }
  return secret;
}

function encodeExecutionToken(payload: ExecutionTokenPayload) {
  const encoded = Buffer.from(JSON.stringify(payload), "utf8").toString(
    "base64url",
  );
  const signature = createHmac("sha256", getExecutionTokenSecret())
    .update(encoded)
    .digest("base64url");
  return `${encoded}.${signature}`;
}

function decodeExecutionToken(token: string) {
  const [encoded, signature] = token.split(".");
  if (!encoded || !signature) {
    throw new Error("Execution token is invalid.");
  }
  const expected = createHmac("sha256", getExecutionTokenSecret())
    .update(encoded)
    .digest("base64url");
  const actualBuffer = Buffer.from(signature, "utf8");
  const expectedBuffer = Buffer.from(expected, "utf8");
  if (
    actualBuffer.length !== expectedBuffer.length ||
    !timingSafeEqual(actualBuffer, expectedBuffer)
  ) {
    throw new Error("Execution token is invalid.");
  }
  const payload = JSON.parse(
    Buffer.from(encoded, "base64url").toString("utf8"),
  ) as ExecutionTokenPayload;
  if (
    !Number.isFinite(payload.runId) ||
    !Number.isFinite(payload.startedAt) ||
    (payload.language !== "PYTHON" && payload.language !== "R")
  ) {
    throw new Error("Execution token is invalid.");
  }
  return payload;
}

async function ensureCodeEntryOwnership(codeEntryId: string, userId: string) {
  const entryRows = await execute<Record<string, unknown>>(
    `SELECT id FROM ${table("code_entries")} WHERE id = ? AND user_id = ? LIMIT 1`,
    [codeEntryId, userId],
  );
  return Boolean(entryRows[0]);
}

async function persistExecutionResult(params: {
  userId: string;
  codeEntryId: string;
  language: ExecutionLanguage;
  runId: number;
  result: {
    stdout: string;
    stderr: string;
    exitCode: number | null;
    timedOut: boolean;
    durationMs: number;
    detectedPackages: string[];
    policyMode: string;
  };
}) {
  const recordId = `databricks-${params.runId}`;
  const existingRows = await execute<Record<string, unknown>>(
    `SELECT id FROM ${table("code_runs")} WHERE id = ? LIMIT 1`,
    [recordId],
  );
  if (existingRows[0]) {
    return;
  }
  await execute(
    `INSERT INTO ${table(
      "code_runs",
    )} (id, code_entry_id, user_id, language, stdout, stderr, exit_code, timed_out, duration_ms, detected_packages, policy_mode, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp())`,
    [
      recordId,
      params.codeEntryId || null,
      params.userId,
      params.language,
      params.result.stdout,
      params.result.stderr,
      params.result.exitCode,
      params.result.timedOut,
      params.result.durationMs,
      params.result.detectedPackages.join(","),
      params.result.policyMode,
    ],
  );
}

async function parseExecutionRequest(request: Request) {
  const contentType = request.headers.get("content-type") || "";
  let code = "";
  let rawLanguage = "";
  let rawBackend = "";
  let codeEntryId = "";
  let inputFiles: ExecutionInputFile[] = [];

  if (contentType.includes("multipart/form-data")) {
    const formData = await request.formData();
    code =
      typeof formData.get("code") === "string"
        ? String(formData.get("code"))
        : "";
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
      typeof body?.language === "string"
        ? body.language.trim().toUpperCase()
        : "";
    rawBackend =
      typeof body?.backend === "string"
        ? body.backend.trim().toLowerCase()
        : "";
    codeEntryId =
      typeof body?.codeEntryId === "string" ? body.codeEntryId.trim() : "";
  }

  return { code, rawLanguage, rawBackend, codeEntryId, inputFiles };
}

function getRequestStatusCode(message: string) {
  return message.includes("Blocked package(s) detected") ||
    message.includes("Package(s) not in allowlist") ||
    message.includes("allowlist is empty")
    ? 400
    : 500;
}

export async function POST(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { code, rawLanguage, rawBackend, codeEntryId, inputFiles } =
    await parseExecutionRequest(request);
  const language: ExecutionLanguage = rawLanguage === "R" ? "R" : "PYTHON";
  const backend = rawBackend ? (rawBackend as ExecutionBackend) : undefined;

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
    if (codeEntryId && !(await ensureCodeEntryOwnership(codeEntryId, user.appUserId))) {
      return NextResponse.json(
        { error: "Code entry not found." },
        { status: 404 },
      );
    }

    if (backend === "databricks" || (!backend && language === "PYTHON")) {
      const handle = await startDatabricksExecution(code, language, inputFiles);
      return NextResponse.json({
        pending: true,
        backend: "databricks",
        token: encodeExecutionToken({
          runId: handle.runId,
          language: handle.language,
          codeEntryId,
          startedAt: handle.startedAt,
          detectedPackages: handle.detectedPackages,
          policyMode: handle.policyMode,
        }),
      });
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

    return NextResponse.json({ pending: false, result });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Code execution failed.";
    return NextResponse.json(
      { error: message },
      { status: getRequestStatusCode(message) },
    );
  }
}

export async function GET(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const token = new URL(request.url).searchParams.get("token") || "";
  if (!token) {
    return NextResponse.json(
      { error: "Execution token is required." },
      { status: 400 },
    );
  }

  try {
    const payload = decodeExecutionToken(token);
    if (
      payload.codeEntryId &&
      !(await ensureCodeEntryOwnership(payload.codeEntryId, user.appUserId))
    ) {
      return NextResponse.json(
        { error: "Code entry not found." },
        { status: 404 },
      );
    }

    const status = await getDatabricksExecutionStatus({
      runId: payload.runId,
      language: payload.language,
      startedAt: payload.startedAt,
      detectedPackages: payload.detectedPackages,
      policyMode: payload.policyMode,
      backend: "databricks",
    });

    if (!status.completed) {
      return NextResponse.json({
        pending: true,
        backend: "databricks",
        statusMessage: status.statusMessage,
        lifeCycleState: status.lifeCycleState,
        resultState: status.resultState,
        durationMs: status.durationMs,
      });
    }

    await persistExecutionResult({
      userId: user.appUserId,
      codeEntryId: payload.codeEntryId,
      language: payload.language,
      runId: payload.runId,
      result: status.result,
    });

    return NextResponse.json({
      pending: false,
      result: status.result,
    });
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Code execution failed.";
    return NextResponse.json(
      { error: message },
      { status: getRequestStatusCode(message) },
    );
  }
}

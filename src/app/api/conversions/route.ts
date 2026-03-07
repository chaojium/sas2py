import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import { execute, table } from "@/lib/databricks";
import {
  convertSasToPython,
  convertSasToR,
  refineConversion,
} from "@/lib/codex";
import { randomUUID } from "node:crypto";

export const runtime = "nodejs";

export async function GET(request: Request) {
  const session = await getServerSession(authOptions);
  if (!session?.user?.id) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = new URL(request.url);
  const query = searchParams.get("search")?.trim();
  const filters: string[] = [`user_id = ?`];
  const params: unknown[] = [session.user.id];
  if (query) {
    filters.push(
      `(lower(name) LIKE ? OR lower(sas_code) LIKE ? OR lower(python_code) LIKE ?)`,
    );
    const like = `%${query.toLowerCase()}%`;
    params.push(like, like, like);
  }

  const entries = await execute<Record<string, unknown>>(
    `SELECT id, user_id, name, language, sas_code, python_code, created_at
     FROM ${table("code_entries")}
     WHERE ${filters.join(" AND ")}
     ORDER BY created_at DESC`,
    params,
  );

  const entryIds = entries.map((entry) => entry.id as string);
  const reviews =
    entryIds.length > 0
      ? await execute<Record<string, unknown>>(
          `SELECT id, code_entry_id, reviewer_id, rating, summary, comments, created_at
           FROM ${table("code_reviews")}
           WHERE code_entry_id IN (${entryIds.map(() => "?").join(",")})
           ORDER BY created_at DESC`,
          entryIds,
        )
      : [];

  const reviewsByEntry = reviews.reduce<Record<string, Record<string, unknown>[]>>(
    (acc, review) => {
      const entryId = review.code_entry_id as string;
      acc[entryId] = acc[entryId] || [];
      acc[entryId].push(review);
      return acc;
    },
    {},
  );

  let runs: Record<string, unknown>[] = [];
  if (entryIds.length > 0) {
    try {
      runs = await execute<Record<string, unknown>>(
        `SELECT id, code_entry_id, language, stdout, stderr, exit_code, timed_out, duration_ms, detected_packages, policy_mode, created_at
         FROM ${table("code_runs")}
         WHERE code_entry_id IN (${entryIds.map(() => "?").join(",")})
         ORDER BY created_at DESC`,
        entryIds,
      );
    } catch (error) {
      console.warn("Skipping code run history query:", error);
    }
  }

  const runsByEntry = runs.reduce<Record<string, Record<string, unknown>[]>>(
    (acc, run) => {
      const entryId = run.code_entry_id as string;
      acc[entryId] = acc[entryId] || [];
      acc[entryId].push(run);
      return acc;
    },
    {},
  );

  const formattedEntries = entries.map((entry) => ({
    id: entry.id,
    userId: entry.user_id,
    name: entry.name,
    language: entry.language,
    sasCode: entry.sas_code,
    pythonCode: entry.python_code,
    createdAt: entry.created_at,
    reviews: (reviewsByEntry[entry.id as string] || []).map((review) => ({
      id: review.id,
      rating: review.rating,
      summary: review.summary,
      comments: review.comments,
      createdAt: review.created_at,
      reviewer: {
        name: null,
        email: null,
      },
    })),
    runs: (runsByEntry[entry.id as string] || []).map((run) => ({
      id: run.id,
      language: run.language,
      stdout: run.stdout,
      stderr: run.stderr,
      exitCode: run.exit_code,
      timedOut: run.timed_out,
      durationMs: run.duration_ms,
      detectedPackages: String(run.detected_packages || "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean),
      policyMode: run.policy_mode,
      createdAt: run.created_at,
    })),
  }));

  return NextResponse.json({ entries: formattedEntries });
}

export async function POST(request: Request) {
  const session = await getServerSession(authOptions);
  if (!session?.user?.id) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const sasCode = typeof body?.sasCode === "string" ? body.sasCode.trim() : "";
  const name = typeof body?.name === "string" ? body.name.trim() : "";
  const rawLanguage =
    typeof body?.language === "string" ? body.language.trim() : "python";
  const language = rawLanguage.toLowerCase() === "r" ? "R" : "PYTHON";
  if (!sasCode) {
    return NextResponse.json(
      { error: "SAS code is required." },
      { status: 400 },
    );
  }
  if (!name) {
    return NextResponse.json({ error: "Name is required." }, { status: 400 });
  }

  try {
    const pythonCode =
      language === "R"
        ? await convertSasToR(sasCode)
        : await convertSasToPython(sasCode);
    const id = randomUUID();
    await execute(
      `INSERT INTO ${table(
        "code_entries",
      )} (id, user_id, name, language, sas_code, python_code, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, current_timestamp(), current_timestamp())`,
      [id, session.user.id, name, language, sasCode, pythonCode],
    );

    return NextResponse.json({
      entry: {
        id,
        userId: session.user.id,
        name,
        language,
        sasCode,
        pythonCode,
        createdAt: new Date().toISOString(),
        reviews: [],
      },
    });
  } catch (error) {
    console.error("OpenAI conversion failed:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "OpenAI conversion failed.",
        details:
          process.env.NODE_ENV === "development" && error instanceof Error
            ? error.stack
            : undefined,
      },
      { status: 500 },
    );
  }
}

export async function PATCH(request: Request) {
  const session = await getServerSession(authOptions);
  if (!session?.user?.id) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const entryId = typeof body?.entryId === "string" ? body.entryId : "";
  const instruction =
    typeof body?.instruction === "string" ? body.instruction.trim() : "";

  if (!entryId || !instruction) {
    return NextResponse.json(
      { error: "Entry and instruction are required." },
      { status: 400 },
    );
  }

  const entryRows = await execute<Record<string, unknown>>(
    `SELECT id, user_id, name, language, sas_code, python_code
     FROM ${table("code_entries")}
     WHERE id = ? AND user_id = ?
     LIMIT 1`,
    [entryId, session.user.id],
  );
  const entry = entryRows[0];

  if (!entry) {
    return NextResponse.json(
      { error: "Code entry not found." },
      { status: 404 },
    );
  }

  try {
    const pythonCode = await refineConversion(
      entry.sas_code as string,
      entry.python_code as string,
      instruction,
      entry.language as "PYTHON" | "R",
    );
    await execute(
      `UPDATE ${table(
        "code_entries",
      )} SET python_code = ?, updated_at = current_timestamp()
       WHERE id = ? AND user_id = ?`,
      [pythonCode, entry.id, session.user.id],
    );

    return NextResponse.json({
      entry: {
        id: entry.id,
        userId: entry.user_id,
        name: entry.name,
        language: entry.language,
        sasCode: entry.sas_code,
        pythonCode,
      },
    });
  } catch (error) {
    console.error("OpenAI refinement failed:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "OpenAI refinement failed.",
        details:
          process.env.NODE_ENV === "development" && error instanceof Error
            ? error.stack
            : undefined,
      },
      { status: 500 },
    );
  }
}

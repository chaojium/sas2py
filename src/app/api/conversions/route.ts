import { NextResponse } from "next/server";
import { execute, table } from "@/lib/databricks";
import {
  analyzeSasCode,
  convertSasToPythonWithContext,
  convertSasToRWithContext,
  refineConversion,
} from "@/lib/codex";
import { getAuthUser } from "@/lib/firebase/server";
import { randomUUID } from "node:crypto";

export const runtime = "nodejs";

type EnhancementRow = {
  id: string;
  code_entry_id: string;
  user_id: string;
  language: string;
  instruction: string;
  previous_code: string;
  updated_code: string;
  created_at: string;
};

export async function GET(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = new URL(request.url);
  const query = searchParams.get("search")?.trim();
  const filters: string[] = [`e.user_id = ?`];
  const params: unknown[] = [user.appUserId];
  if (query) {
    filters.push(
      `(lower(e.name) LIKE ? OR lower(e.sas_code) LIKE ? OR lower(e.python_code) LIKE ?)`,
    );
    const like = `%${query.toLowerCase()}%`;
    params.push(like, like, like);
  }

  const entries = await execute<Record<string, unknown>>(
    `SELECT e.id, e.user_id, e.project_id, e.name, e.language, e.sas_code, e.python_code, e.created_at,
            e.additional_guidance, e.reference_url,
            p.name AS project_name
     FROM ${table("code_entries")}
     e
     LEFT JOIN ${table("projects")} p ON p.id = e.project_id AND p.user_id = e.user_id
     WHERE ${filters.join(" AND ")}
     ORDER BY e.created_at DESC`,
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

  let enhancements: EnhancementRow[] = [];
  if (entryIds.length > 0) {
    try {
      enhancements = await execute<EnhancementRow>(
        `SELECT id, code_entry_id, user_id, language, instruction, previous_code, updated_code, created_at
         FROM ${table("code_enhancements")}
         WHERE code_entry_id IN (${entryIds.map(() => "?").join(",")})
         ORDER BY created_at DESC`,
        entryIds,
      );
    } catch (error) {
      console.warn("Skipping code enhancement history query:", error);
    }
  }

  const enhancementsByEntry = enhancements.reduce<Record<string, EnhancementRow[]>>(
    (acc, enhancement) => {
      const entryId = enhancement.code_entry_id;
      acc[entryId] = acc[entryId] || [];
      acc[entryId].push(enhancement);
      return acc;
    },
    {},
  );

  let runs: Record<string, unknown>[] = [];
  if (entryIds.length > 0) {
    try {
      runs = await execute<Record<string, unknown>>(
        `SELECT id, code_entry_id, language, stdout, stderr, exit_code, timed_out, duration_ms, detected_packages, policy_mode, artifacts_json, created_at
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
    projectId: entry.project_id,
    projectName: entry.project_name,
    name: entry.name,
    language: entry.language,
    sasCode: entry.sas_code,
    pythonCode: entry.python_code,
    additionalGuidance: entry.additional_guidance,
    referenceUrl: entry.reference_url,
    createdAt: entry.created_at,
    enhancements: (enhancementsByEntry[entry.id as string] || []).map(
      (enhancement) => ({
        id: enhancement.id,
        language: enhancement.language,
        instruction: enhancement.instruction,
        previousCode: enhancement.previous_code,
        updatedCode: enhancement.updated_code,
        createdAt: enhancement.created_at,
      }),
    ),
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
      artifacts: (() => {
        try {
          const parsed = JSON.parse(String(run.artifacts_json || "[]"));
          return Array.isArray(parsed) ? parsed : [];
        } catch {
          return [];
        }
      })(),
      createdAt: run.created_at,
    })),
  }));

  return NextResponse.json({ entries: formattedEntries });
}

export async function POST(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const sasCode = typeof body?.sasCode === "string" ? body.sasCode.trim() : "";
  const name = typeof body?.name === "string" ? body.name.trim() : "";
  const rawLanguage =
    typeof body?.language === "string" ? body.language.trim() : "python";
  const language = rawLanguage.toLowerCase() === "r" ? "R" : "PYTHON";
  const forceRegenerate = body?.forceRegenerate === true;
  const additionalGuidance =
    typeof body?.additionalGuidance === "string"
      ? body.additionalGuidance.trim()
      : "";
  const referenceUrl =
    typeof body?.referenceUrl === "string" ? body.referenceUrl.trim() : "";
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
    if (!forceRegenerate) {
      const existingRows = await execute<Record<string, unknown>>(
        `SELECT id, user_id, name, language, sas_code, python_code, created_at, additional_guidance, reference_url
         FROM ${table("code_entries")}
         WHERE user_id = ? AND language = ? AND sas_code = ?
           AND coalesce(additional_guidance, '') = ?
           AND coalesce(reference_url, '') = ?
         ORDER BY created_at DESC
         LIMIT 1`,
        [user.appUserId, language, sasCode, additionalGuidance, referenceUrl],
      );
      const existing = existingRows[0];
      if (existing) {
        const sasAnalysis = await analyzeSasCode(sasCode);
        return NextResponse.json({
          entry: {
            id: existing.id,
            userId: existing.user_id,
            name: existing.name,
            language: existing.language,
            sasCode: existing.sas_code,
            pythonCode: existing.python_code,
            additionalGuidance: existing.additional_guidance,
            referenceUrl: existing.reference_url,
            sasAnalysis,
            createdAt: existing.created_at,
            enhancements: [],
            reviews: [],
            runs: [],
          },
          reusedExisting: true,
        });
      }
    }

    const [pythonCode, sasAnalysis] = await Promise.all([
      language === "R"
        ? convertSasToRWithContext(sasCode, { additionalGuidance, referenceUrl })
        : convertSasToPythonWithContext(sasCode, {
            additionalGuidance,
            referenceUrl,
          }),
      analyzeSasCode(sasCode),
    ]);
    const id = randomUUID();
    await execute(
      `INSERT INTO ${table(
        "code_entries",
      )} (id, user_id, name, language, sas_code, python_code, additional_guidance, reference_url, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp(), current_timestamp())`,
      [
        id,
        user.appUserId,
        name,
        language,
        sasCode,
        pythonCode,
        additionalGuidance,
        referenceUrl,
      ],
    );

    return NextResponse.json({
      entry: {
        id,
        userId: user.appUserId,
        name,
        language,
        sasCode,
        pythonCode,
        additionalGuidance,
        referenceUrl,
        sasAnalysis,
        createdAt: new Date().toISOString(),
        enhancements: [],
        reviews: [],
        runs: [],
      },
      reusedExisting: false,
    });
  } catch (error) {
    console.error("Conversion request failed:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Conversion request failed.",
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
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const entryId = typeof body?.entryId === "string" ? body.entryId : "";
  const projectId =
    body?.projectId === null
      ? null
      : typeof body?.projectId === "string"
        ? body.projectId.trim()
        : undefined;
  const editedPythonCode =
    typeof body?.pythonCode === "string" ? body.pythonCode : "";
  const instruction =
    typeof body?.instruction === "string" ? body.instruction.trim() : "";

  if (
    !entryId ||
    (!instruction && !editedPythonCode.trim() && projectId === undefined)
  ) {
    return NextResponse.json(
      { error: "Entry and either instruction or code are required." },
      { status: 400 },
    );
  }

  const entryRows = await execute<Record<string, unknown>>(
    `SELECT id, user_id, project_id, name, language, sas_code, python_code
            , additional_guidance, reference_url
     FROM ${table("code_entries")}
     WHERE id = ? AND user_id = ?
     LIMIT 1`,
    [entryId, user.appUserId],
  );
  const entry = entryRows[0];

  if (!entry) {
    return NextResponse.json(
      { error: "Code entry not found." },
      { status: 404 },
    );
  }

  try {
    if (projectId !== undefined) {
      if (projectId) {
        const projectRows = await execute<Record<string, unknown>>(
          `SELECT id, name FROM ${table("projects")} WHERE id = ? AND user_id = ? LIMIT 1`,
          [projectId, user.appUserId],
        );
        if (!projectRows[0]) {
          return NextResponse.json(
            { error: "Project not found." },
            { status: 404 },
          );
        }
      }

      await execute(
        `UPDATE ${table(
          "code_entries",
        )} SET project_id = ?, updated_at = current_timestamp()
         WHERE id = ? AND user_id = ?`,
        [projectId, entry.id, user.appUserId],
      );

      return NextResponse.json({
        entry: {
          id: entry.id,
          userId: entry.user_id,
          projectId,
          name: entry.name,
          language: entry.language,
          sasCode: entry.sas_code,
          pythonCode: entry.python_code,
          additionalGuidance: entry.additional_guidance,
          referenceUrl: entry.reference_url,
        },
      });
    }

    if (editedPythonCode.trim()) {
      await execute(
        `UPDATE ${table(
          "code_entries",
        )} SET python_code = ?, updated_at = current_timestamp()
         WHERE id = ? AND user_id = ?`,
        [editedPythonCode, entry.id, user.appUserId],
      );

      return NextResponse.json({
        entry: {
          id: entry.id,
          userId: entry.user_id,
          name: entry.name,
          language: entry.language,
          sasCode: entry.sas_code,
          pythonCode: editedPythonCode,
          additionalGuidance: entry.additional_guidance,
          referenceUrl: entry.reference_url,
        },
      });
    }

    const pythonCode = await refineConversion(
      entry.sas_code as string,
      entry.python_code as string,
      instruction,
      entry.language as "PYTHON" | "R",
      {
        additionalGuidance: String(entry.additional_guidance || ""),
        referenceUrl: String(entry.reference_url || ""),
      },
    );
    const enhancementId = randomUUID();
    await execute(
      `INSERT INTO ${table(
        "code_enhancements",
      )} (id, code_entry_id, user_id, language, instruction, previous_code, updated_code, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp())`,
      [
        enhancementId,
        entry.id,
        user.appUserId,
        entry.language,
        instruction,
        entry.python_code,
        pythonCode,
      ],
    );
    await execute(
      `UPDATE ${table(
        "code_entries",
      )} SET python_code = ?, updated_at = current_timestamp()
       WHERE id = ? AND user_id = ?`,
      [pythonCode, entry.id, user.appUserId],
    );

    return NextResponse.json({
      entry: {
        id: entry.id,
        userId: entry.user_id,
        name: entry.name,
        language: entry.language,
        sasCode: entry.sas_code,
        pythonCode,
        additionalGuidance: entry.additional_guidance,
        referenceUrl: entry.reference_url,
        enhancements: [
          {
            id: enhancementId,
            language: entry.language,
            instruction,
            previousCode: entry.python_code,
            updatedCode: pythonCode,
            createdAt: new Date().toISOString(),
          },
        ],
      },
    });
  } catch (error) {
    console.error("Refinement request failed:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Refinement request failed.",
        details:
          process.env.NODE_ENV === "development" && error instanceof Error
            ? error.stack
            : undefined,
      },
      { status: 500 },
    );
  }
}

export async function DELETE(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = new URL(request.url);
  let entryId = searchParams.get("entryId")?.trim() || "";

  if (!entryId) {
    try {
      const body = await request.json();
      entryId = typeof body?.entryId === "string" ? body.entryId.trim() : "";
    } catch {
      // ignore body parse failure for empty-body deletes
    }
  }

  if (!entryId) {
    return NextResponse.json({ error: "entryId is required." }, { status: 400 });
  }

  let entryRows = await execute<Record<string, unknown>>(
    `SELECT id, user_id FROM ${table("code_entries")} WHERE id = ? AND user_id = ? LIMIT 1`,
    [entryId, user.appUserId],
  );

  if (!entryRows[0]) {
    entryRows = await execute<Record<string, unknown>>(
      `SELECT id, user_id FROM ${table("code_entries")} WHERE id = ? LIMIT 1`,
      [entryId],
    );
  }

  if (!entryRows[0]) {
    return NextResponse.json({ error: "Code entry not found." }, { status: 404 });
  }

  const entry = entryRows[0];
  const owner = String(entry.user_id || "").toLowerCase();
  if (owner && owner !== user.appUserId.toLowerCase()) {
    return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  }

  try {
    await execute(
      `DELETE FROM ${table("code_runs")} WHERE code_entry_id = ? AND user_id = ?`,
      [entryId, user.appUserId],
    );
  } catch (error) {
    console.warn("Skipping code run delete:", error);
  }

  await execute(
    `DELETE FROM ${table("code_reviews")} WHERE code_entry_id = ?`,
    [entryId],
  );
  await execute(
    `DELETE FROM ${table("code_entries")} WHERE id = ?`,
    [entryId],
  );

  const remainingRows = await execute<Record<string, unknown>>(
    `SELECT id FROM ${table("code_entries")} WHERE id = ? LIMIT 1`,
    [entryId],
  );
  if (remainingRows[0]) {
    return NextResponse.json(
      { error: "Deletion did not complete." },
      { status: 500 },
    );
  }

  return NextResponse.json({ success: true });
}

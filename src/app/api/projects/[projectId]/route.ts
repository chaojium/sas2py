import { NextResponse } from "next/server";
import { execute, table } from "@/lib/databricks";
import { getAuthUser } from "@/lib/firebase/server";

export const runtime = "nodejs";

type Context = {
  params: Promise<{ projectId: string }>;
};

export async function PATCH(request: Request, context: Context) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { projectId } = await context.params;
  const body = await request.json();
  const name = typeof body?.name === "string" ? body.name.trim() : "";
  const description =
    typeof body?.description === "string" ? body.description.trim() : "";

  if (!name) {
    return NextResponse.json({ error: "Project name is required." }, { status: 400 });
  }

  const rows = await execute<Record<string, unknown>>(
    `SELECT id FROM ${table("projects")} WHERE id = ? AND user_id = ? LIMIT 1`,
    [projectId, user.appUserId],
  );
  if (!rows[0]) {
    return NextResponse.json({ error: "Project not found." }, { status: 404 });
  }

  await execute(
    `UPDATE ${table(
      "projects",
    )} SET name = ?, description = ?, updated_at = current_timestamp()
     WHERE id = ? AND user_id = ?`,
    [name, description || null, projectId, user.appUserId],
  );

  return NextResponse.json({
    project: {
      id: projectId,
      name,
      description: description || null,
    },
  });
}

export async function DELETE(_request: Request, context: Context) {
  const user = await getAuthUser(_request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { projectId } = await context.params;
  const rows = await execute<Record<string, unknown>>(
    `SELECT id FROM ${table("projects")} WHERE id = ? AND user_id = ? LIMIT 1`,
    [projectId, user.appUserId],
  );
  if (!rows[0]) {
    return NextResponse.json({ error: "Project not found." }, { status: 404 });
  }

  await execute(
    `UPDATE ${table(
      "code_entries",
    )} SET project_id = NULL, updated_at = current_timestamp()
     WHERE project_id = ? AND user_id = ?`,
    [projectId, user.appUserId],
  );
  await execute(
    `DELETE FROM ${table("projects")} WHERE id = ? AND user_id = ?`,
    [projectId, user.appUserId],
  );

  return NextResponse.json({ success: true });
}

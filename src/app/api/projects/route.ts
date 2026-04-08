import { NextResponse } from "next/server";
import { randomUUID } from "node:crypto";
import { execute, table } from "@/lib/databricks";
import { getAuthUser } from "@/lib/firebase/server";

export const runtime = "nodejs";

export async function GET(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const projects = await execute<Record<string, unknown>>(
    `SELECT p.id, p.name, p.description, p.created_at,
            COUNT(e.id) AS entry_count
     FROM ${table("projects")} p
     LEFT JOIN ${table("code_entries")} e ON e.project_id = p.id
     WHERE p.user_id = ?
     GROUP BY p.id, p.name, p.description, p.created_at
     ORDER BY p.created_at DESC`,
    [user.appUserId],
  );

  return NextResponse.json({
    projects: projects.map((project) => ({
      id: project.id,
      name: project.name,
      description: project.description,
      createdAt: project.created_at,
      entryCount: Number(project.entry_count || 0),
    })),
  });
}

export async function POST(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const name = typeof body?.name === "string" ? body.name.trim() : "";
  const description =
    typeof body?.description === "string" ? body.description.trim() : "";

  if (!name) {
    return NextResponse.json({ error: "Project name is required." }, { status: 400 });
  }

  const id = randomUUID();
  await execute(
    `INSERT INTO ${table(
      "projects",
    )} (id, user_id, name, description, created_at, updated_at)
     VALUES (?, ?, ?, ?, current_timestamp(), current_timestamp())`,
    [id, user.appUserId, name, description || null],
  );

  return NextResponse.json({
    project: {
      id,
      name,
      description: description || null,
      createdAt: new Date().toISOString(),
      entryCount: 0,
    },
  });
}

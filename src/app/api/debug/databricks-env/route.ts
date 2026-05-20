import { NextResponse } from "next/server";

export const runtime = "nodejs";

function isDebugEnabled() {
  return process.env.DEBUG_ROUTES_ENABLED?.trim() === "true";
}

function maskValue(value: string | undefined, visible = 4) {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return "";
  }
  if (trimmed.length <= visible * 2) {
    return `${trimmed.slice(0, 1)}***${trimmed.slice(-1)}`;
  }
  return `${trimmed.slice(0, visible)}***${trimmed.slice(-visible)}`;
}

function summarizeValue(value: string | undefined) {
  if (value === undefined) {
    return { present: false, length: 0, masked: null, quoted: false };
  }

  const quoted =
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"));
  const trimmed = value.trim();
  return {
    present: trimmed.length > 0,
    length: value.length,
    trimmedLength: trimmed.length,
    startsWithWhitespace: /^\s/.test(value),
    endsWithWhitespace: /\s$/.test(value),
    quoted,
    masked: maskValue(trimmed),
  };
}

export async function GET() {
  if (!isDebugEnabled()) {
    return NextResponse.json({ error: "Not found." }, { status: 404 });
  }

  return NextResponse.json({
    nodeEnv: process.env.NODE_ENV || null,
    vercelEnv: process.env.VERCEL_ENV || null,
    databricks: {
      host: summarizeValue(process.env.DATABRICKS_SERVER_HOSTNAME),
      httpPath: summarizeValue(process.env.DATABRICKS_HTTP_PATH),
      accessToken: summarizeValue(process.env.DATABRICKS_ACCESS_TOKEN),
      catalog: summarizeValue(process.env.DATABRICKS_CATALOG),
      schema: summarizeValue(process.env.DATABRICKS_SCHEMA),
    },
  });
}

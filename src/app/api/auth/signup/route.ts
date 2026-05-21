import { NextResponse } from "next/server";
import { hashPassword } from "@/lib/auth/password";
import { getPrisma } from "@/lib/prisma";

const prisma = getPrisma();

function defaultNameFromEmail(email: string) {
  const localPart = email.split("@")[0] ?? "";
  return localPart.replace(/[._-]+/g, " ").trim() || email;
}

export async function POST(request: Request) {
  const body = await request.json();
  const email =
    typeof body?.email === "string" ? body.email.trim().toLowerCase() : "";
  const password = typeof body?.password === "string" ? body.password : "";
  const name =
    typeof body?.name === "string" && body.name.trim()
      ? body.name.trim()
      : defaultNameFromEmail(email);

  if (!email) {
    return NextResponse.json({ error: "Email is required." }, { status: 400 });
  }

  if (password.length < 8) {
    return NextResponse.json(
      { error: "Password must be at least 8 characters." },
      { status: 400 },
    );
  }

  const existingUser = await prisma.user.findUnique({
    where: { email },
    select: { id: true },
  });

  if (existingUser) {
    return NextResponse.json(
      { error: "An account with that email already exists." },
      { status: 409 },
    );
  }

  const user = await prisma.user.create({
    data: {
      email,
      name,
      passwordHash: hashPassword(password),
    },
    select: {
      id: true,
      email: true,
      name: true,
    },
  });

  return NextResponse.json({ user }, { status: 201 });
}

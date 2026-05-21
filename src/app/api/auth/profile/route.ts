import { NextResponse } from "next/server";
import { getPrisma } from "@/lib/prisma";
import { getAuthUser } from "@/lib/firebase/server";

const prisma = getPrisma();

export async function PATCH(request: Request) {
  const authUser = await getAuthUser(request);

  if (!authUser?.uid) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const name = typeof body?.name === "string" ? body.name.trim() : "";

  if (!name) {
    return NextResponse.json({ error: "Name is required." }, { status: 400 });
  }

  const user = await prisma.user.update({
    where: { id: authUser.uid },
    data: { name },
    select: {
      id: true,
      email: true,
      name: true,
      image: true,
    },
  });

  return NextResponse.json({ user });
}

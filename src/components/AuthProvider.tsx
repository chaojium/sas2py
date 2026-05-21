"use client";

import { createContext, useContext, type ReactNode } from "react";
import { SessionProvider, signOut, useSession } from "next-auth/react";

type AuthStatus = "loading" | "authenticated" | "unauthenticated";

type AuthUser = {
  id: string;
  displayName: string | null;
  email: string | null;
  image: string | null;
};

type AuthContextValue = {
  status: AuthStatus;
  user: AuthUser | null;
  signOutUser: () => Promise<void>;
  refreshUser: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

function AuthContextProvider({ children }: { children: ReactNode }) {
  const { data, status, update } = useSession();
  const user = data?.user
    ? {
        id: data.user.id,
        displayName: data.user.name ?? null,
        email: data.user.email ?? null,
        image: data.user.image ?? null,
      }
    : null;

  return (
    <AuthContext.Provider
      value={{
        user,
        status:
          status === "loading"
            ? "loading"
            : user
              ? "authenticated"
              : "unauthenticated",
        signOutUser: async () => {
          await signOut({ redirect: false });
        },
        refreshUser: async () => {
          await update();
        },
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export default function AuthProvider({ children }: { children: ReactNode }) {
  return (
    <SessionProvider>
      <AuthContextProvider>{children}</AuthContextProvider>
    </SessionProvider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider.");
  }
  return context;
}

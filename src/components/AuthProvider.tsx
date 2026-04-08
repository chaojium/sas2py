"use client";

import {
  type User as FirebaseUser,
  onIdTokenChanged,
  signOut,
} from "firebase/auth";
import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { firebaseAuth } from "@/lib/firebase/client";

type AuthStatus = "loading" | "authenticated" | "unauthenticated";

type AuthContextValue = {
  status: AuthStatus;
  user: FirebaseUser | null;
  signOutUser: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

export default function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<FirebaseUser | null>(null);
  const [status, setStatus] = useState<AuthStatus>("loading");

  useEffect(() => {
    return onIdTokenChanged(firebaseAuth, (nextUser) => {
      setUser(nextUser);
      setStatus(nextUser ? "authenticated" : "unauthenticated");
    });
  }, []);

  return (
    <AuthContext.Provider
      value={{
        status,
        user,
        signOutUser: () => signOut(firebaseAuth),
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider.");
  }
  return context;
}

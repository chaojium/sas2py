import "server-only";
import { spawn } from "node:child_process";

const DEFAULT_TIMEOUT_MS = 90_000;

function buildCommand() {
  const command = process.env.CODEX_CLI_COMMAND || "codex";
  const script = process.env.CODEX_CLI_SCRIPT;
  const rawArgs = process.env.CODEX_CLI_ARGS || "";
  const requireLogin = process.env.CODEX_REQUIRE_LOGIN === "true";
  const mode = process.env.CODEX_MODE || "exec";
  const args = rawArgs
    .split(" ")
    .map((part) => part.trim())
    .filter(Boolean);

  if (requireLogin && !args.includes("--login")) {
    args.unshift("--login");
  }

  const modeArgs = mode ? [mode, "-"] : [];
  const finalArgs = script ? [script, ...modeArgs, ...args] : [...modeArgs, ...args];
  return { command, args: finalArgs };
}

export async function convertSasToPython(sasCode: string) {
  const { command, args } = buildCommand();
  const prompt = [
    "Convert the following SAS code to idiomatic, production-ready Python.",
    "Use pandas where needed and preserve logic and comments.",
    "Return only Python code, no markdown or commentary.",
    "",
    sasCode,
  ].join("\n");

  return new Promise<string>((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
    });

    const timer = setTimeout(() => {
      child.kill();
      reject(new Error("Codex CLI timed out."));
    }, Number(process.env.CODEX_TIMEOUT_MS || DEFAULT_TIMEOUT_MS));

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code !== 0) {
        reject(
          new Error(
            stderr || `Codex CLI failed with exit code ${code ?? "unknown"}.`,
          ),
        );
        return;
      }
      const output = stdout.trim();
      if (!output) {
        reject(new Error("Codex CLI returned empty output."));
        return;
      }
      resolve(output);
    });

    child.stdin.write(prompt);
    child.stdin.end();
  });
}

export async function convertSasToR(sasCode: string) {
  const { command, args } = buildCommand();
  const prompt = [
    "Convert the following SAS code to idiomatic, production-ready R.",
    "Use tidyverse or data.table where appropriate and preserve logic and comments.",
    "Return only R code, no markdown or commentary.",
    "",
    sasCode,
  ].join("\n");

  return new Promise<string>((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
    });

    const timer = setTimeout(() => {
      child.kill();
      reject(new Error("Codex CLI timed out."));
    }, Number(process.env.CODEX_TIMEOUT_MS || DEFAULT_TIMEOUT_MS));

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code !== 0) {
        reject(
          new Error(
            stderr || `Codex CLI failed with exit code ${code ?? "unknown"}.`,
          ),
        );
        return;
      }
      const output = stdout.trim();
      if (!output) {
        reject(new Error("Codex CLI returned empty output."));
        return;
      }
      resolve(output);
    });

    child.stdin.write(prompt);
    child.stdin.end();
  });
}

export async function refineConversion(
  sasCode: string,
  convertedCode: string,
  instruction: string,
  language: "PYTHON" | "R",
) {
  const { command, args } = buildCommand();
  const target = language === "R" ? "R" : "Python";
  const prompt = [
    `You are improving an existing SAS to ${target} conversion.`,
    "Apply the user's instruction while preserving the SAS logic.",
    `Return only the updated ${target} code, no markdown or commentary.`,
    "",
    "User instruction:",
    instruction,
    "",
    "SAS source:",
    sasCode,
    "",
    `Current ${target} conversion:`,
    convertedCode,
  ].join("\n");

  return new Promise<string>((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
    });

    const timer = setTimeout(() => {
      child.kill();
      reject(new Error("Codex CLI timed out."));
    }, Number(process.env.CODEX_TIMEOUT_MS || DEFAULT_TIMEOUT_MS));

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code !== 0) {
        reject(
          new Error(
            stderr || `Codex CLI failed with exit code ${code ?? "unknown"}.`,
          ),
        );
        return;
      }
      const output = stdout.trim();
      if (!output) {
        reject(new Error("Codex CLI returned empty output."));
        return;
      }
      resolve(output);
    });

    child.stdin.write(prompt);
    child.stdin.end();
  });
}

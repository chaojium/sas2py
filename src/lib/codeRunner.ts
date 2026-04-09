import "server-only";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { spawn } from "node:child_process";
import {
  isAzureBlobUploadConfigured,
  uploadExecutionInputsToAzure,
} from "@/lib/blobStorage";

export type ExecutionLanguage = "PYTHON" | "R";
export type ExecutionBackend = "databricks" | "docker";
type PackagePolicyMode = "off" | "blocklist" | "allowlist";
export type ExecutionInputFile = {
  name: string;
  content: Buffer;
};
export type DatabricksExecutionHandle = {
  runId: number;
  language: ExecutionLanguage;
  detectedPackages: string[];
  policyMode: PackagePolicyMode;
  startedAt: number;
  backend: "databricks";
};
export type DatabricksExecutionStatus =
  | {
      completed: false;
      runId: number;
      language: ExecutionLanguage;
      statusMessage: string;
      lifeCycleState: string;
      resultState: string;
      durationMs: number;
      backend: "databricks";
    }
  | {
      completed: true;
      runId: number;
      language: ExecutionLanguage;
      result: RawExecutionResult;
      backend: "databricks";
    };

type RunState = {
  life_cycle_state?: string;
  result_state?: string;
  state_message?: string;
};

type RunTask = {
  run_id?: number;
  task_key?: string;
};

type RunDetails = {
  state?: RunState;
  tasks?: RunTask[];
};

type RawExecutionResult = {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  timedOut: boolean;
  durationMs: number;
  images: string[];
};

export type CodeExecutionResult = {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  timedOut: boolean;
  durationMs: number;
  detectedPackages: string[];
  policyMode: PackagePolicyMode;
  images: string[];
  backend: ExecutionBackend;
};

const DEFAULT_TIMEOUT_MS = 120_000;
const DEFAULT_MAX_OUTPUT_CHARS = 100_000;
const DEFAULT_POLL_INTERVAL_MS = 2_000;
const DEFAULT_DATABRICKS_INPUT_FILE_MAX_BYTES = 256_000;
const DEFAULT_PYTHON_BLOCKLIST = [
  "subprocess",
  "socket",
  "ctypes",
  "multiprocessing",
  "threading",
  "urllib",
  "http",
  "requests",
  "pip",
];
const DEFAULT_R_BLOCKLIST = [
  "httr",
  "curl",
  "parallel",
  "processx",
  "sys",
  "socket",
];

function parsePositiveInt(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.floor(parsed);
}

function truncateOutput(value: string, maxChars: number) {
  if (value.length <= maxChars) return value;
  return `${value.slice(0, maxChars)}\n\n[output truncated]`;
}

function splitByComma(value: string | undefined) {
  return (value || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function splitPackageList(value: string | undefined) {
  return splitByComma(value).map((entry) => entry.toLowerCase());
}

function unique(values: string[]) {
  return [...new Set(values)];
}

function sanitizeUploadName(name: string) {
  const cleaned = basename(name).replace(/[^A-Za-z0-9._-]+/g, "_");
  return cleaned || "input.dat";
}

function buildPythonDatabricksInputSetup(inputFiles: ExecutionInputFile[]) {
  if (inputFiles.length === 0) return "";
  const encodedFiles = inputFiles.map((file) => ({
    name: sanitizeUploadName(file.name),
    contentBase64: file.content.toString("base64"),
  }));
  const escaped = JSON.stringify(encodedFiles);
  return [
    "import base64 as __sas2py_input_b64",
    "import json as __sas2py_input_json",
    "import os as __sas2py_input_os",
    "import tempfile as __sas2py_input_tempfile",
    "__sas2py_input_dir = __sas2py_input_tempfile.mkdtemp(prefix='sas2py-input-')",
    "__sas2py_input_files = " + escaped,
    "__sas2py_input_os.makedirs(__sas2py_input_dir, exist_ok=True)",
    "__sas2py_input_os.environ['SAS2PY_INPUT_DIR'] = __sas2py_input_dir",
    "__sas2py_input_os.chdir(__sas2py_input_dir)",
    "for __sas2py_file in __sas2py_input_files:",
    "    with open(__sas2py_input_os.path.join(__sas2py_input_dir, __sas2py_file['name']), 'wb') as __sas2py_handle:",
    "        __sas2py_handle.write(__sas2py_input_b64.b64decode(__sas2py_file['contentBase64']))",
  ].join("\n");
}

function buildPythonDatabricksBlobSetup(
  inputFiles: { name: string; url: string }[],
) {
  if (inputFiles.length === 0) return "";
  const escaped = JSON.stringify(
    inputFiles.map((file) => ({
      name: sanitizeUploadName(file.name),
      url: file.url,
    })),
  );
  return [
    "import json as __sas2py_input_json",
    "import os as __sas2py_input_os",
    "import tempfile as __sas2py_input_tempfile",
    "import urllib.request as __sas2py_input_request",
    "__sas2py_input_dir = __sas2py_input_tempfile.mkdtemp(prefix='sas2py-input-')",
    "__sas2py_input_files = " + escaped,
    "__sas2py_input_os.makedirs(__sas2py_input_dir, exist_ok=True)",
    "__sas2py_input_os.environ['SAS2PY_INPUT_DIR'] = __sas2py_input_dir",
    "__sas2py_input_os.chdir(__sas2py_input_dir)",
    "for __sas2py_file in __sas2py_input_files:",
    "    __sas2py_path = __sas2py_input_os.path.join(__sas2py_input_dir, __sas2py_file['name'])",
    "    with __sas2py_input_request.urlopen(__sas2py_file['url']) as __sas2py_response, open(__sas2py_path, 'wb') as __sas2py_handle:",
    "        __sas2py_handle.write(__sas2py_response.read())",
  ].join("\n");
}

function buildRDatabricksInputSetup(inputFiles: ExecutionInputFile[]) {
  if (inputFiles.length === 0) return "";
  const fileListLiteral = `list(${inputFiles
    .map(
      (file) =>
        `list(name=${JSON.stringify(sanitizeUploadName(file.name))}, contentBase64=${JSON.stringify(file.content.toString("base64"))})`,
    )
    .join(", ")})`;
  return [
    "sas2py_input_dir <- tempfile('sas2py-input-')",
    "dir.create(sas2py_input_dir, recursive = TRUE, showWarnings = FALSE)",
    `sas2py_input_files <- ${fileListLiteral}`,
    "Sys.setenv(SAS2PY_INPUT_DIR = sas2py_input_dir)",
    "setwd(sas2py_input_dir)",
    "sas2py_b64_map <- setNames(0:63, strsplit('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/', '')[[1]])",
    "sas2py_base64_decode <- function(value) {",
    "  chars <- strsplit(gsub('\\n', '', value), '')[[1]]",
    "  chars <- chars[chars != '=']",
    "  if (!length(chars)) return(raw())",
    "  values <- unname(sas2py_b64_map[chars])",
    "  values[is.na(values)] <- 0L",
    "  out <- raw()",
    "  for (index in seq(1, length(values), by = 4)) {",
    "    chunk <- values[index:min(index + 3, length(values))]",
    "    if (length(chunk) < 4) chunk <- c(chunk, rep(0L, 4 - length(chunk)))",
    "    bits <- bitwShiftL(chunk[1], 18) + bitwShiftL(chunk[2], 12) + bitwShiftL(chunk[3], 6) + chunk[4]",
    "    out <- c(out, as.raw(bitwAnd(bitwShiftR(bits, 16), 255L)))",
    "    out <- c(out, as.raw(bitwAnd(bitwShiftR(bits, 8), 255L)))",
    "    out <- c(out, as.raw(bitwAnd(bits, 255L)))",
    "  }",
    "  padding <- nchar(gsub('[^=]', '', value))",
    "  if (padding > 0 && length(out) >= padding) out <- out[seq_len(length(out) - padding)]",
    "  out",
    "}",
    "for (sas2py_file in sas2py_input_files) {",
    "  writeBin(sas2py_base64_decode(sas2py_file$contentBase64), file.path(sas2py_input_dir, sas2py_file$name))",
    "}",
  ].join("\n");
}

function buildRDatabricksBlobSetup(inputFiles: { name: string; url: string }[]) {
  if (inputFiles.length === 0) return "";
  const fileListLiteral = `list(${inputFiles
    .map(
      (file) =>
        `list(name=${JSON.stringify(sanitizeUploadName(file.name))}, url=${JSON.stringify(file.url)})`,
    )
    .join(", ")})`;
  return [
    "sas2py_input_dir <- tempfile('sas2py-input-')",
    "dir.create(sas2py_input_dir, recursive = TRUE, showWarnings = FALSE)",
    `sas2py_input_files <- ${fileListLiteral}`,
    "Sys.setenv(SAS2PY_INPUT_DIR = sas2py_input_dir)",
    "setwd(sas2py_input_dir)",
    "for (sas2py_file in sas2py_input_files) {",
    "  download.file(sas2py_file$url, destfile = file.path(sas2py_input_dir, sas2py_file$name), mode = 'wb', quiet = TRUE)",
    "}",
  ].join("\n");
}

function applyDatabricksInputSetup(
  code: string,
  language: ExecutionLanguage,
  inputFiles: ExecutionInputFile[],
) {
  if (inputFiles.length === 0) return code;
  const setup =
    language === "R"
      ? buildRDatabricksInputSetup(inputFiles)
      : buildPythonDatabricksInputSetup(inputFiles);
  return `${setup}\n${code}`;
}

function applyDatabricksBlobSetup(
  code: string,
  language: ExecutionLanguage,
  inputFiles: { name: string; url: string }[],
) {
  if (inputFiles.length === 0) return code;
  const setup =
    language === "R"
      ? buildRDatabricksBlobSetup(inputFiles)
      : buildPythonDatabricksBlobSetup(inputFiles);
  return `${setup}\n${code}`;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeDatabricksBaseUrl(host: string) {
  let value = host.trim();
  value = value.replace(/^https?:\/\/https?:\/\//i, "https://");
  if (!/^https?:\/\//i.test(value)) {
    value = `https://${value}`;
  }
  return value.replace(/\/+$/, "");
}

function parsePolicyMode(value: string | undefined): PackagePolicyMode {
  const mode = (value || "blocklist").trim().toLowerCase();
  if (mode === "off" || mode === "allowlist" || mode === "blocklist") {
    return mode;
  }
  return "blocklist";
}

function normalizeBackend(value: string | undefined): ExecutionBackend | null {
  const normalized = (value || "").trim().toLowerCase();
  if (normalized === "databricks" || normalized === "docker") {
    return normalized;
  }
  return null;
}

function resolveBackend(
  language: ExecutionLanguage,
  requestedBackend?: ExecutionBackend,
) {
  if (requestedBackend) return requestedBackend;
  const envKey =
    language === "R"
      ? "CODE_RUNNER_BACKEND_R"
      : "CODE_RUNNER_BACKEND_PYTHON";
  const fallback = language === "R" ? "docker" : "databricks";
  return normalizeBackend(process.env[envKey]) || fallback;
}

function extractPythonPackages(code: string) {
  const packages: string[] = [];
  for (const rawLine of code.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;

    const importMatch = line.match(/^import\s+(.+)$/);
    if (importMatch) {
      const modules = importMatch[1]
        .split(",")
        .map((part) => part.trim().split(/\s+as\s+/i)[0]?.trim())
        .filter(Boolean);
      for (const moduleName of modules) {
        const root = moduleName.split(".")[0]?.toLowerCase();
        if (root) packages.push(root);
      }
      continue;
    }

    const fromMatch = line.match(/^from\s+([a-zA-Z0-9_\.]+)\s+import\s+/);
    if (fromMatch) {
      const root = fromMatch[1]?.split(".")[0]?.toLowerCase();
      if (root) packages.push(root);
    }
  }
  return unique(packages);
}

function extractRPackages(code: string) {
  const packages: string[] = [];
  for (const rawLine of code.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;

    for (const match of line.matchAll(
      /(?:library|require)\(\s*([A-Za-z0-9\._]+)\s*\)/g,
    )) {
      const name = match[1]?.toLowerCase();
      if (name) packages.push(name);
    }
    for (const match of line.matchAll(/([A-Za-z0-9\._]+)::/g)) {
      const name = match[1]?.toLowerCase();
      if (name) packages.push(name);
    }
  }
  return unique(packages);
}

function sanitizeRSource(code: string) {
  return code
    .replace(/^\uFEFF/, "")
    .replace(/[\u200B\u200C\u200D\u2060\uFEFF]/g, "")
    .replace(/\u00A0/g, " ")
    .replace(/[\u1680\u180E\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]/g, " ")
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/g, "")
    .replace(/\r\n/g, "\n");
}

function validatePackagePolicy(code: string, language: ExecutionLanguage) {
  const mode = parsePolicyMode(process.env.CODE_RUNNER_PACKAGE_POLICY);
  const detectedPackages =
    language === "R" ? extractRPackages(code) : extractPythonPackages(code);

  if (mode === "off") return { mode, detectedPackages };

  const envPrefix = language === "R" ? "CODE_RUNNER_R" : "CODE_RUNNER_PYTHON";
  const defaultBlocklist =
    language === "R" ? DEFAULT_R_BLOCKLIST : DEFAULT_PYTHON_BLOCKLIST;
  const blocklist = unique(
    splitPackageList(process.env[`${envPrefix}_BLOCKLIST`]).concat(
      defaultBlocklist,
    ),
  );
  const allowlist = unique(splitPackageList(process.env[`${envPrefix}_ALLOWLIST`]));

  if (mode === "blocklist") {
    const blocked = detectedPackages.filter((name) => blocklist.includes(name));
    if (blocked.length > 0) {
      throw new Error(
        `Blocked package(s) detected: ${blocked.join(", ")}. Update package policy or modify code.`,
      );
    }
    return { mode, detectedPackages };
  }

  if (allowlist.length === 0) {
    throw new Error(
      `Package policy is allowlist, but ${envPrefix}_ALLOWLIST is empty.`,
    );
  }
  const disallowed = detectedPackages.filter((name) => !allowlist.includes(name));
  if (disallowed.length > 0) {
    throw new Error(
      `Package(s) not in allowlist: ${disallowed.join(", ")}. Update allowlist or modify code.`,
    );
  }
  return { mode, detectedPackages };
}

function buildPythonExecutionEnvelope(code: string) {
  const escaped = JSON.stringify(code);
  const marker = "__SAS2PY_RESULT__";
  return [
    "import base64 as __sas2py_b64",
    "import ast as __sas2py_ast",
    "import io as __sas2py_io",
    "import json as __sas2py_json",
    "import sys as __sas2py_sys",
    "import traceback as __sas2py_traceback",
    "from contextlib import redirect_stderr as __sas2py_redirect_stderr, redirect_stdout as __sas2py_redirect_stdout",
    "__sas2py_user_code = " + escaped,
    "__sas2py_scope = {'__name__': '__main__'}",
    "__sas2py_stdout = __sas2py_io.StringIO()",
    "__sas2py_stderr = __sas2py_io.StringIO()",
    "__sas2py_error = ''",
    "__sas2py_images = []",
    "__sas2py_seen_figs = set()",
    "def __sas2py_render_value(__value):",
    "    if __value is None:",
    "        return",
    "    try:",
    "        print(__value)",
    "    except Exception:",
    "        print(repr(__value))",
    "def __sas2py_exec_with_notebook_tail(__code, __scope, __render_value=__sas2py_render_value):",
    "    try:",
    "        import ast as __sas2py_ast_local",
    "        __module = __sas2py_ast_local.parse(__code, mode='exec')",
    "    except Exception:",
    "        exec(__code, __scope, __scope)",
    "        return",
    "    if __module.body and isinstance(__module.body[-1], __sas2py_ast_local.Expr):",
    "        __prefix = __sas2py_ast_local.Module(body=__module.body[:-1], type_ignores=[])",
    "        exec(compile(__prefix, '<sas2py>', 'exec'), __scope, __scope)",
    "        __last = __sas2py_ast_local.Expression(__module.body[-1].value)",
    "        __value = eval(compile(__last, '<sas2py>', 'eval'), __scope, __scope)",
    "        __render_value(__value)",
    "    else:",
    "        exec(compile(__module, '<sas2py>', 'exec'), __scope, __scope)",
    "def __sas2py_capture_figures(__seen=__sas2py_seen_figs, __images=__sas2py_images, __io=__sas2py_io, __b64=__sas2py_b64):",
    "    try:",
    "        import matplotlib.pyplot as __sas2py_plt",
    "    except Exception:",
    "        return",
    "    for __sas2py_fig_num in __sas2py_plt.get_fignums():",
    "        if __sas2py_fig_num in __seen:",
    "            continue",
    "        __sas2py_fig = __sas2py_plt.figure(__sas2py_fig_num)",
    "        __sas2py_buf = __io.BytesIO()",
    "        __sas2py_fig.savefig(__sas2py_buf, format='png', bbox_inches='tight')",
    "        __images.append(__b64.b64encode(__sas2py_buf.getvalue()).decode('ascii'))",
    "        __seen.add(__sas2py_fig_num)",
    "        __sas2py_buf.close()",
    "try:",
    "    import matplotlib.pyplot as __sas2py_plt",
    "    __sas2py_original_show = __sas2py_plt.show",
    "    def __sas2py_show_wrapper(*args, __capture=__sas2py_capture_figures, __show=__sas2py_original_show, **kwargs):",
    "        __capture()",
    "        try:",
    "            return __show(*args, **kwargs)",
    "        except Exception:",
    "            return None",
    "    __sas2py_plt.show = __sas2py_show_wrapper",
    "except Exception:",
    "    pass",
    "try:",
    "    __sas2py_original_argv = list(__sas2py_sys.argv)",
    "except Exception:",
    "    __sas2py_original_argv = []",
    "__sas2py_sys.argv = ['sas2py']",
    "try:",
    "    with __sas2py_redirect_stdout(__sas2py_stdout), __sas2py_redirect_stderr(__sas2py_stderr):",
    "        __sas2py_exec_with_notebook_tail(__sas2py_user_code, __sas2py_scope)",
    "except Exception:",
    "    __sas2py_error = __sas2py_traceback.format_exc()",
    "finally:",
    "    try:",
    "        __sas2py_sys.argv = __sas2py_original_argv",
    "    except Exception:",
    "        pass",
    "__sas2py_capture_figures()",
    "try:",
    "    import matplotlib.pyplot as __sas2py_plt",
    "    __sas2py_plt.close('all')",
    "except Exception:",
    "    pass",
    "__sas2py_payload = {",
    "    'stdout': __sas2py_stdout.getvalue(),",
    "    'stderr': __sas2py_stderr.getvalue() + (('\\n' + __sas2py_error) if __sas2py_error else ''),",
    "    'images': __sas2py_images,",
    "}",
    `print('${marker}' + __sas2py_json.dumps(__sas2py_payload))`,
  ].join("\n");
}

function buildRExecutionEnvelope(code: string) {
  const escaped = JSON.stringify(sanitizeRSource(code));
  const marker = "__SAS2PY_RESULT__";
  return [
    "sas2py_user_code <- " + escaped,
    "sas2py_stdout <- character()",
    "sas2py_stderr <- character()",
    "sas2py_plot_dir <- tempfile('sas2py-plots-')",
    "dir.create(sas2py_plot_dir, recursive = TRUE, showWarnings = FALSE)",
    "sas2py_plot_pattern <- file.path(sas2py_plot_dir, 'plot-%03d.png')",
    "png(filename = sas2py_plot_pattern, width = 1400, height = 900, res = 144)",
    "sas2py_capture_output <- function(expr) {",
    "  output <- capture.output(result <- force(expr), type = 'output')",
    "  if (length(output) > 0) sas2py_stdout <<- c(sas2py_stdout, output)",
    "  result",
    "}",
    "sas2py_render_value <- function(value) {",
    "  if (is.null(value)) return(invisible(NULL))",
    "  if (inherits(value, 'ggplot')) {",
    "    print(value)",
    "    return(invisible(NULL))",
    "  }",
    "  if (isS4(value) && methods::is(value, 'ggplot')) {",
    "    print(value)",
    "    return(invisible(NULL))",
    "  }",
    "  print(value)",
    "  invisible(NULL)",
    "}",
    "sas2py_normalize_code <- function(code) {",
    "  code <- enc2utf8(code)",
    "  code <- sub(paste0('^', intToUtf8(65279L)), '', code)",
    "  code <- gsub('[\\u200B\\u200C\\u200D\\u2060\\uFEFF]', '', code, perl = TRUE)",
    "  code <- gsub('\\u00A0', ' ', code, perl = TRUE)",
    "  code <- gsub('\\u1680|\\u180E|[\\u2000-\\u200A]|\\u202F|\\u205F|\\u3000', ' ', code, perl = TRUE)",
    "  code",
    "}",
    "sas2py_exec_with_notebook_tail <- function(code) {",
    "  code <- sas2py_normalize_code(code)",
    "  expressions <- parse(text = code)",
    "  if (length(expressions) == 0) return(invisible(NULL))",
    "  if (length(expressions) > 1) {",
    "    for (index in seq_len(length(expressions) - 1)) {",
    "      sas2py_capture_output(eval(expressions[[index]], envir = .GlobalEnv))",
    "    }",
    "  }",
    "  last_value <- sas2py_capture_output(eval(expressions[[length(expressions)]], envir = .GlobalEnv))",
    "  sas2py_capture_output(sas2py_render_value(last_value))",
    "}",
    "sas2py_base64_encode <- function(raw_value) {",
    "  if (!length(raw_value)) return('')",
    "  alphabet <- strsplit('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/', '')[[1]]",
    "  bytes <- as.integer(raw_value)",
    "  result <- character()",
    "  for (index in seq(1, length(bytes), by = 3)) {",
    "    chunk <- bytes[index:min(index + 2, length(bytes))]",
    "    padding <- 3 - length(chunk)",
    "    if (padding > 0) chunk <- c(chunk, rep(0L, padding))",
    "    bits <- bitwShiftL(chunk[1], 16) + bitwShiftL(chunk[2], 8) + chunk[3]",
    "    values <- c(",
    "      bitwAnd(bitwShiftR(bits, 18), 63L),",
    "      bitwAnd(bitwShiftR(bits, 12), 63L),",
    "      bitwAnd(bitwShiftR(bits, 6), 63L),",
    "      bitwAnd(bits, 63L)",
    "    )",
    "    chars <- alphabet[values + 1L]",
    "    if (padding >= 1) chars[4] <- '='",
    "    if (padding == 2) chars[3] <- '='",
    "    result <- c(result, chars)",
    "  }",
    "  paste(result, collapse = '')",
    "}",
    "sas2py_json_escape <- function(value) {",
    "  value <- gsub('\\\\\\\\', '\\\\\\\\\\\\\\\\', value)",
    "  value <- gsub('\"', '\\\\\\\\\"', value)",
    "  value <- gsub('\\r', '\\\\\\\\r', value)",
    "  value <- gsub('\\n', '\\\\\\\\n', value)",
    "  value <- gsub('\\t', '\\\\\\\\t', value)",
    "  value",
    "}",
    "sas2py_json_string <- function(value) {",
    "  paste0('\"', sas2py_json_escape(paste(value, collapse = '\\n')), '\"')",
    "}",
    "sas2py_json_array <- function(values) {",
    "  if (!length(values)) return('[]')",
    "  paste0('[', paste(vapply(values, sas2py_json_string, character(1)), collapse = ','), ']')",
    "}",
    "tryCatch(",
    "  {",
    "    sas2py_exec_with_notebook_tail(sas2py_user_code)",
    "  },",
    "  error = function(error) {",
    "    sas2py_stderr <<- c(sas2py_stderr, conditionMessage(error))",
    "  },",
    "  finally = {",
    "    try(dev.off(), silent = TRUE)",
    "  }",
    ")",
    "sas2py_plot_files <- sort(Sys.glob(file.path(sas2py_plot_dir, 'plot-*.png')))",
    "sas2py_images <- character()",
    "if (length(sas2py_plot_files) > 0) {",
    "  file_info <- file.info(sas2py_plot_files)",
    "  non_empty <- rownames(file_info)[!is.na(file_info$size) & file_info$size > 0]",
    "  if (length(non_empty) > 0) {",
    "    sas2py_images <- vapply(non_empty, function(path) {",
    "      sas2py_base64_encode(readBin(path, what = 'raw', n = file.info(path)$size))",
    "    }, character(1))",
    "  }",
    "}",
    "sas2py_payload <- paste0(",
    "  '{',",
    "  '\"stdout\":', sas2py_json_string(sas2py_stdout), ',',",
    "  '\"stderr\":', sas2py_json_string(sas2py_stderr), ',',",
    "  '\"images\":', sas2py_json_array(sas2py_images),",
    "  '}'",
    ")",
    `cat('${marker}', sas2py_payload, sep = '')`,
  ].join("\n");
}

function parseExecutionEnvelope(output: string) {
  const marker = "__SAS2PY_RESULT__";
  const index = output.lastIndexOf(marker);
  if (index < 0) return null;

  const prefix = output.slice(0, index).trim();
  const rawJson = output.slice(index + marker.length).trim();
  try {
    const parsed = JSON.parse(rawJson) as {
      stdout?: string;
      stderr?: string;
      images?: string[];
    };
    return {
      stdout: `${prefix}${prefix && parsed.stdout ? "\n" : ""}${parsed.stdout || ""}`.trim(),
      stderr: parsed.stderr || "",
      images: Array.isArray(parsed.images)
        ? parsed.images.filter((value) => typeof value === "string" && value.length > 0)
        : [],
    };
  } catch {
    return null;
  }
}

function getDatabricksConfig(language: ExecutionLanguage) {
  const host =
    (language === "R"
      ? process.env.CODE_RUNNER_DATABRICKS_R_HOST
      : process.env.CODE_RUNNER_DATABRICKS_PYTHON_HOST) ||
    process.env.DATABRICKS_SERVER_HOSTNAME;
  const token =
    (language === "R"
      ? process.env.CODE_RUNNER_DATABRICKS_R_TOKEN
      : process.env.CODE_RUNNER_DATABRICKS_PYTHON_TOKEN) ||
    process.env.DATABRICKS_ACCESS_TOKEN;
  const normalizedHost = host?.trim();
  const normalizedToken = token?.trim();
  if (!normalizedHost || !normalizedToken) {
    throw new Error(
      language === "R"
        ? "Databricks R runner requires CODE_RUNNER_DATABRICKS_R_HOST and CODE_RUNNER_DATABRICKS_R_TOKEN, or the shared DATABRICKS_SERVER_HOSTNAME and DATABRICKS_ACCESS_TOKEN."
        : "Databricks Python runner requires CODE_RUNNER_DATABRICKS_PYTHON_HOST and CODE_RUNNER_DATABRICKS_PYTHON_TOKEN, or the shared DATABRICKS_SERVER_HOSTNAME and DATABRICKS_ACCESS_TOKEN.",
    );
  }
  const baseUrl = normalizeDatabricksBaseUrl(normalizedHost);
  return { baseUrl, token: normalizedToken };
}

function getJobId(language: ExecutionLanguage) {
  const raw =
    language === "R"
      ? process.env.CODE_RUNNER_DATABRICKS_R_JOB_ID
      : process.env.CODE_RUNNER_DATABRICKS_PYTHON_JOB_ID;
  const jobId = Number(raw);
  if (!Number.isFinite(jobId) || jobId <= 0) {
    const key =
      language === "R"
        ? "CODE_RUNNER_DATABRICKS_R_JOB_ID"
        : "CODE_RUNNER_DATABRICKS_PYTHON_JOB_ID";
    throw new Error(`Missing or invalid ${key}.`);
  }
  return Math.floor(jobId);
}

function getPreferredTaskKey(language: ExecutionLanguage) {
  return (
    (language === "R"
      ? process.env.CODE_RUNNER_DATABRICKS_R_TASK_KEY
      : process.env.CODE_RUNNER_DATABRICKS_PYTHON_TASK_KEY) || ""
  ).trim();
}

async function databricksApi<T>(
  baseUrl: string,
  token: string,
  path: string,
  init?: RequestInit,
) {
  const response = await fetch(`${baseUrl}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Databricks Jobs API failed (${response.status}) on ${path}: ${text.slice(0, 300)}`,
    );
  }
  return (await response.json()) as T;
}

async function getRunDetails(baseUrl: string, token: string, runId: number) {
  return databricksApi<RunDetails>(
    baseUrl,
    token,
    `/api/2.1/jobs/runs/get?run_id=${runId}`,
  );
}

async function getRunOutput(baseUrl: string, token: string, runId: number) {
  return databricksApi<{
    notebook_output?: { result?: string; truncated?: boolean };
    error?: string;
  }>(baseUrl, token, `/api/2.1/jobs/runs/get-output?run_id=${runId}`);
}

function isMultiTaskOutputError(error: unknown) {
  return (
    error instanceof Error &&
    error.message.includes("multiple tasks") &&
    error.message.includes("get-output")
  );
}

async function getOutputWithTaskFallback(
  baseUrl: string,
  token: string,
  parentRunId: number,
  language: ExecutionLanguage,
) {
  try {
    return await getRunOutput(baseUrl, token, parentRunId);
  } catch (error) {
    if (!isMultiTaskOutputError(error)) throw error;

    const details = await getRunDetails(baseUrl, token, parentRunId);
    const preferredTaskKey = getPreferredTaskKey(language);
    const candidates = (details.tasks || []).filter(
      (task) => Number.isFinite(task.run_id) && (task.run_id || 0) > 0,
    );
    if (candidates.length === 0) {
      throw new Error(
        "Run has multiple tasks, but no task run IDs were found for output retrieval.",
      );
    }

    const orderedCandidates = preferredTaskKey
      ? [
          ...candidates.filter((task) => task.task_key === preferredTaskKey),
          ...candidates.filter((task) => task.task_key !== preferredTaskKey),
        ]
      : candidates;

    const errors: string[] = [];
    for (const task of orderedCandidates) {
      try {
        return await getRunOutput(baseUrl, token, task.run_id as number);
      } catch (taskError) {
        if (taskError instanceof Error) {
          errors.push(`${task.task_key || task.run_id}: ${taskError.message.slice(0, 200)}`);
        }
      }
    }
    throw new Error(`Failed to retrieve output from task runs. ${errors.join(" | ")}`);
  }
}

async function cancelRun(baseUrl: string, token: string, runId: number) {
  try {
    await databricksApi(
      baseUrl,
      token,
      "/api/2.1/jobs/runs/cancel",
      {
        method: "POST",
        body: JSON.stringify({ run_id: runId }),
      },
    );
  } catch {
    // ignore cancel failure
  }
}

async function buildDatabricksPayloadCode(
  code: string,
  language: ExecutionLanguage,
  inputFiles: ExecutionInputFile[] = [],
) {
  const basePayloadCode =
    language === "PYTHON"
      ? buildPythonExecutionEnvelope(code)
      : buildRExecutionEnvelope(code);
  if (inputFiles.length > 0 && isAzureBlobUploadConfigured()) {
    const uploadedFiles = await uploadExecutionInputsToAzure(
      inputFiles.map((file) => ({
        name: sanitizeUploadName(file.name),
        content: file.content,
      })),
    );
    return applyDatabricksBlobSetup(basePayloadCode, language, uploadedFiles);
  }
  if (inputFiles.length > 0) {
    const databricksInputFileMaxBytes = parsePositiveInt(
      process.env.CODE_RUNNER_DATABRICKS_INPUT_FILE_MAX_BYTES,
      DEFAULT_DATABRICKS_INPUT_FILE_MAX_BYTES,
    );
    const totalInputBytes = inputFiles.reduce(
      (sum, file) => sum + file.content.length,
      0,
    );
    if (totalInputBytes > databricksInputFileMaxBytes) {
      throw new Error(
        `Uploaded input files are too large for Databricks notebook parameters (${totalInputBytes} bytes). Limit is ${databricksInputFileMaxBytes} bytes. Configure Azure Blob Storage for Databricks file handoff, or use the docker runner.`,
      );
    }
    return applyDatabricksInputSetup(basePayloadCode, language, inputFiles);
  }
  return basePayloadCode;
}

async function submitDatabricksRun(
  payloadCode: string,
  language: ExecutionLanguage,
) {
  const jobId = getJobId(language);
  const { baseUrl, token } = getDatabricksConfig(language);
  const submit = await databricksApi<{ run_id: number }>(
    baseUrl,
    token,
    "/api/2.1/jobs/run-now",
    {
      method: "POST",
      body: JSON.stringify({
        job_id: jobId,
        notebook_params: { code: payloadCode },
      }),
    },
  );
  return {
    runId: submit.run_id,
    baseUrl,
    token,
  };
}

async function fetchDatabricksRunStatus(
  runId: number,
  language: ExecutionLanguage,
  startedAt: number,
) {
  const timeoutMs = parsePositiveInt(
    process.env.CODE_RUNNER_TIMEOUT_MS,
    DEFAULT_TIMEOUT_MS,
  );
  const { baseUrl, token } = getDatabricksConfig(language);
  const status = await getRunDetails(baseUrl, token, runId);
  const lifeCycleState = status.state?.life_cycle_state || "";
  const resultState = status.state?.result_state || "";
  const statusMessage = status.state?.state_message || "";

  if (Date.now() - startedAt > timeoutMs) {
    await cancelRun(baseUrl, token, runId);
    return {
      completed: true,
      runId,
      language,
      backend: "databricks" as const,
      result: {
        stdout: statusMessage,
        stderr: "Execution timed out and run was cancelled.",
        exitCode: 1,
        timedOut: true,
        durationMs: Date.now() - startedAt,
        images: [],
      },
    } satisfies DatabricksExecutionStatus;
  }

  if (
    lifeCycleState !== "TERMINATED" &&
    lifeCycleState !== "SKIPPED" &&
    lifeCycleState !== "INTERNAL_ERROR"
  ) {
    return {
      completed: false,
      runId,
      language,
      statusMessage,
      lifeCycleState,
      resultState,
      durationMs: Date.now() - startedAt,
      backend: "databricks" as const,
    } satisfies DatabricksExecutionStatus;
  }

  let stdout = "";
  let stderr = "";
  let images: string[] = [];
  try {
    const output = await getOutputWithTaskFallback(
      baseUrl,
      token,
      runId,
      language,
    );
    stdout = output.notebook_output?.result || "";
    stderr = output.error || "";
    if (output.notebook_output?.truncated) {
      stdout = `${stdout}\n\n[output truncated by Databricks]`;
    }
  } catch (error) {
    stderr =
      error instanceof Error ? error.message : "Failed to fetch Databricks run output.";
  }

  if (stdout) {
    const parsed = parseExecutionEnvelope(stdout);
    if (parsed) {
      stdout = parsed.stdout;
      stderr = `${stderr}${stderr && parsed.stderr ? "\n" : ""}${parsed.stderr}`;
      images = parsed.images;
    }
  }

  if (!stdout && statusMessage) {
    stdout = statusMessage;
  }

  return {
    completed: true,
    runId,
    language,
    backend: "databricks" as const,
    result: {
      stdout,
      stderr,
      exitCode: resultState && resultState !== "SUCCESS" ? 1 : 0,
      timedOut: false,
      durationMs: Date.now() - startedAt,
      images,
    },
  } satisfies DatabricksExecutionStatus;
}

export async function startDatabricksExecution(
  code: string,
  language: ExecutionLanguage,
  inputFiles: ExecutionInputFile[] = [],
) {
  if (!code.trim()) {
    throw new Error("Code is required.");
  }
  const { mode, detectedPackages } = validatePackagePolicy(code, language);
  const payloadCode = await buildDatabricksPayloadCode(code, language, inputFiles);
  const submitted = await submitDatabricksRun(payloadCode, language);
  return {
    runId: submitted.runId,
    language,
    detectedPackages,
    policyMode: mode,
    startedAt: Date.now(),
    backend: "databricks" as const,
  } satisfies DatabricksExecutionHandle;
}

export async function getDatabricksExecutionStatus(
  handle: DatabricksExecutionHandle,
) {
  const maxOutputChars = parsePositiveInt(
    process.env.CODE_RUNNER_MAX_OUTPUT_CHARS,
    DEFAULT_MAX_OUTPUT_CHARS,
  );
  const status = await fetchDatabricksRunStatus(
    handle.runId,
    handle.language,
    handle.startedAt,
  );
  if (!status.completed) {
    return status;
  }
  return {
    ...status,
    result: {
      stdout: truncateOutput(status.result.stdout, maxOutputChars),
      stderr: truncateOutput(status.result.stderr, maxOutputChars),
      exitCode: status.result.exitCode,
      timedOut: status.result.timedOut,
      durationMs: status.result.durationMs,
      images: status.result.images,
      detectedPackages: handle.detectedPackages,
      policyMode: handle.policyMode,
      backend: "databricks" as const,
    },
  };
}

async function runCodeInDatabricks(
  code: string,
  language: ExecutionLanguage,
  timeoutMs: number,
  pollIntervalMs: number,
  inputFiles: ExecutionInputFile[] = [],
) {
  const handle = await startDatabricksExecution(code, language, inputFiles);
  const startedAt = handle.startedAt;

  while (true) {
    const status = await fetchDatabricksRunStatus(
      handle.runId,
      language,
      startedAt,
    );
    if (status.completed) {
      return status.result satisfies RawExecutionResult;
    }
    if (Date.now() - startedAt > timeoutMs) {
      return {
        stdout: status.statusMessage,
        stderr: "Execution timed out and run was cancelled.",
        exitCode: 1,
        timedOut: true,
        durationMs: Date.now() - startedAt,
        images: [],
      } satisfies RawExecutionResult;
    }
    await sleep(pollIntervalMs);
  }
}

function getDockerConfig(language: ExecutionLanguage) {
  const dockerCommand = process.env.CODE_RUNNER_DOCKER_COMMAND || "docker";
  const image =
    language === "R"
      ? process.env.CODE_RUNNER_DOCKER_R_IMAGE || "r-base:4.3.3"
      : process.env.CODE_RUNNER_DOCKER_PYTHON_IMAGE || "python:3.12-slim";
  const dockerArgs = splitByComma(process.env.CODE_RUNNER_DOCKER_ARGS);
  const commandArgs =
    language === "R" ? ["Rscript", "-"] : ["python", "-u", "-"];
  return {
    dockerCommand,
    args: ["run", "--rm", "-i", ...dockerArgs, image, ...commandArgs],
  };
}

async function createExecutionInputWorkspace(files: ExecutionInputFile[]) {
  const rootDir = await mkdtemp(join(tmpdir(), "sas2py-run-"));
  const inputDir = join(rootDir, "input");
  await mkdir(inputDir, { recursive: true });

  const usedNames = new Set<string>();
  const normalizedFiles: string[] = [];

  for (const file of files) {
    const baseName = sanitizeUploadName(file.name);
    let candidate = baseName;
    let counter = 1;
    while (usedNames.has(candidate)) {
      const dotIndex = baseName.lastIndexOf(".");
      candidate =
        dotIndex >= 0
          ? `${baseName.slice(0, dotIndex)}-${counter}${baseName.slice(dotIndex)}`
          : `${baseName}-${counter}`;
      counter += 1;
    }
    usedNames.add(candidate);
    normalizedFiles.push(candidate);
    await writeFile(join(inputDir, candidate), file.content);
  }

  return {
    rootDir,
    hostInputDir: inputDir,
    containerInputDir: "/workspace/input",
    fileNames: normalizedFiles,
  };
}

async function runCodeInDocker(
  code: string,
  language: ExecutionLanguage,
  timeoutMs: number,
  inputFiles: ExecutionInputFile[] = [],
) {
  const payloadCode =
    language === "PYTHON"
      ? buildPythonExecutionEnvelope(code)
      : buildRExecutionEnvelope(code);
  const { dockerCommand, args } = getDockerConfig(language);
  const startedAt = Date.now();
  const workspace =
    inputFiles.length > 0 ? await createExecutionInputWorkspace(inputFiles) : null;
  const finalArgs = workspace
    ? [
        "run",
        "--rm",
        "-i",
        "-v",
        `${workspace.hostInputDir}:${workspace.containerInputDir}:ro`,
        "-w",
        workspace.containerInputDir,
        "-e",
        `SAS2PY_INPUT_DIR=${workspace.containerInputDir}`,
        ...args.slice(3),
      ]
    : args;

  return new Promise<RawExecutionResult>((resolve, reject) => {
    const child = spawn(dockerCommand, finalArgs, {
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    let settled = false;

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      void (workspace ? rm(workspace.rootDir, { recursive: true, force: true }) : Promise.resolve());
      reject(
        new Error(
          `Docker code runner failed to start. ${error.message}. Is Docker available on this host?`,
        ),
      );
    });

    child.on("close", (exitCode) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      void (workspace ? rm(workspace.rootDir, { recursive: true, force: true }) : Promise.resolve());

      let images: string[] = [];
      if (stdout) {
        const parsed = parseExecutionEnvelope(stdout);
        if (parsed) {
          stdout = parsed.stdout;
          stderr = `${stderr}${stderr && parsed.stderr ? "\n" : ""}${parsed.stderr}`;
          images = parsed.images;
        }
      }

      if (timedOut && !stderr) {
        stderr = "Execution timed out and process was terminated.";
      }

      resolve({
        stdout,
        stderr,
        exitCode,
        timedOut,
        durationMs: Date.now() - startedAt,
        images,
      });
    });

    child.stdin.write(payloadCode);
    child.stdin.end();
  });
}

export async function runCodeInContainer(
  code: string,
  language: ExecutionLanguage,
  requestedBackend?: ExecutionBackend,
  inputFiles: ExecutionInputFile[] = [],
) {
  if (!code.trim()) {
    throw new Error("Code is required.");
  }

  const timeoutMs = parsePositiveInt(
    process.env.CODE_RUNNER_TIMEOUT_MS,
    DEFAULT_TIMEOUT_MS,
  );
  const maxOutputChars = parsePositiveInt(
    process.env.CODE_RUNNER_MAX_OUTPUT_CHARS,
    DEFAULT_MAX_OUTPUT_CHARS,
  );
  const pollIntervalMs = parsePositiveInt(
    process.env.CODE_RUNNER_DATABRICKS_POLL_INTERVAL_MS,
    DEFAULT_POLL_INTERVAL_MS,
  );
  const backend = resolveBackend(language, requestedBackend);
  const { mode, detectedPackages } = validatePackagePolicy(code, language);

  const rawResult =
    backend === "databricks"
      ? await runCodeInDatabricks(
          code,
          language,
          timeoutMs,
          pollIntervalMs,
          inputFiles,
        )
      : await runCodeInDocker(code, language, timeoutMs, inputFiles);

  return {
    stdout: truncateOutput(rawResult.stdout, maxOutputChars),
    stderr: truncateOutput(rawResult.stderr, maxOutputChars),
    exitCode: rawResult.exitCode,
    timedOut: rawResult.timedOut,
    durationMs: rawResult.durationMs,
    detectedPackages,
    policyMode: mode,
    images: rawResult.images,
    backend,
  } satisfies CodeExecutionResult;
}

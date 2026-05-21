import "server-only";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { spawn } from "node:child_process";
import {
  isAzureBlobUploadConfigured,
  shouldAvoidSasUrls,
  uploadBinaryToAzureAndGetSasUrl,
  uploadTextToAzureAndGetSasUrl,
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
  artifacts: RawExecutionArtifact[];
};

type RawExecutionArtifact = {
  name: string;
  contentType: string;
  sizeBytes: number;
  contentBase64: string;
};

export type ExecutionArtifact = {
  name: string;
  contentType: string;
  sizeBytes: number;
  downloadUrl?: string;
  contentBase64?: string;
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
  artifacts: ExecutionArtifact[];
  backend: ExecutionBackend;
};

const DEFAULT_TIMEOUT_MS = 120_000;
const DEFAULT_MAX_OUTPUT_CHARS = 100_000;
const DEFAULT_POLL_INTERVAL_MS = 2_000;
const DEFAULT_DATABRICKS_INPUT_FILE_MAX_BYTES = 256_000;
const MAX_DATABRICKS_NOTEBOOK_PARAM_BYTES = 9_000;
const DEFAULT_MAX_ARTIFACT_BYTES = 768_000;
const DEFAULT_MAX_ARTIFACT_TOTAL_BYTES = 2_000_000;
const DEFAULT_MAX_ARTIFACT_COUNT = 10;
const ALLOWED_ARTIFACT_EXTENSIONS = new Set([
  ".csv",
  ".tsv",
  ".txt",
  ".json",
  ".xlsx",
  ".xls",
  ".pdf",
  ".png",
  ".html",
  ".htm",
  ".parquet",
  ".feather",
  ".rds",
  ".rdata",
  ".pkl",
  ".pickle",
]);
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

type DatabricksBlobPathConfig = {
  accountName: string;
  containerName: string;
  tenantId: string;
  clientId: string;
  clientSecret: string;
};

function getDatabricksBlobPathConfig(): DatabricksBlobPathConfig | null {
  const accountName = process.env.AZURE_STORAGE_ACCOUNT_NAME?.trim();
  const containerName = process.env.AZURE_STORAGE_CONTAINER?.trim();
  const tenantId = process.env.AZURE_TENANT_ID?.trim();
  const clientId = process.env.AZURE_CLIENT_ID?.trim();
  const clientSecret = process.env.AZURE_CLIENT_SECRET?.trim();
  if (!accountName || !containerName || !tenantId || !clientId || !clientSecret) {
    return null;
  }
  return {
    accountName,
    containerName,
    tenantId,
    clientId,
    clientSecret,
  };
}

function buildBlobUrlFromName(config: DatabricksBlobPathConfig, blobName: string) {
  return `https://${config.accountName}.blob.core.windows.net/${config.containerName}/${blobName}`;
}

function buildPythonOAuthBlobDownloader(config: DatabricksBlobPathConfig) {
  return [
    "import json as __sas2py_blob_json",
    "import urllib.parse as __sas2py_blob_parse",
    "import urllib.request as __sas2py_blob_request",
    "def __sas2py_blob_token():",
    "    __sas2py_payload = __sas2py_blob_parse.urlencode({",
    "        'grant_type': 'client_credentials',",
    `        'client_id': ${JSON.stringify(config.clientId)},`,
    `        'client_secret': ${JSON.stringify(config.clientSecret)},`,
    "        'scope': 'https://storage.azure.com/.default',",
    "    }).encode('utf-8')",
    `    __sas2py_request = __sas2py_blob_request.Request('https://login.microsoftonline.com/${config.tenantId}/oauth2/v2.0/token', data=__sas2py_payload, headers={'Content-Type': 'application/x-www-form-urlencoded'})`,
    "    with __sas2py_blob_request.urlopen(__sas2py_request) as __sas2py_response:",
    "        return __sas2py_blob_json.loads(__sas2py_response.read().decode('utf-8'))['access_token']",
    "def __sas2py_blob_download(__sas2py_url, __sas2py_destination):",
    "    __sas2py_token = __sas2py_blob_token()",
    "    __sas2py_request = __sas2py_blob_request.Request(__sas2py_url, headers={'Authorization': f'Bearer {__sas2py_token}', 'x-ms-version': '2023-11-03'})",
    "    with __sas2py_blob_request.urlopen(__sas2py_request) as __sas2py_response, open(__sas2py_destination, 'wb') as __sas2py_handle:",
    "        __sas2py_handle.write(__sas2py_response.read())",
  ].join("\n");
}

function buildPythonDatabricksDirectBlobSetup(
  inputFiles: { name: string; blobName: string }[],
  config: DatabricksBlobPathConfig,
) {
  if (inputFiles.length === 0) return "";
  const escaped = JSON.stringify(
    inputFiles.map((file) => ({
      name: sanitizeUploadName(file.name),
      url: buildBlobUrlFromName(config, file.blobName),
    })),
  );
  return [
    buildPythonOAuthBlobDownloader(config),
    "import os as __sas2py_input_os",
    "import tempfile as __sas2py_input_tempfile",
    "__sas2py_input_dir = __sas2py_input_tempfile.mkdtemp(prefix='sas2py-input-')",
    "__sas2py_input_files = " + escaped,
    "__sas2py_input_os.makedirs(__sas2py_input_dir, exist_ok=True)",
    "__sas2py_input_os.environ['SAS2PY_INPUT_DIR'] = __sas2py_input_dir",
    "__sas2py_input_os.chdir(__sas2py_input_dir)",
    "for __sas2py_file in __sas2py_input_files:",
    "    __sas2py_path = __sas2py_input_os.path.join(__sas2py_input_dir, __sas2py_file['name'])",
    "    __sas2py_blob_download(__sas2py_file['url'], __sas2py_path)",
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

function buildRDatabricksDirectBlobSetup(
  inputFiles: { name: string; blobName: string }[],
  config: DatabricksBlobPathConfig,
) {
  if (inputFiles.length === 0) return "";
  const pythonScript = [
    "import json",
    "import os",
    "import sys",
    "import urllib.parse",
    "import urllib.request",
    `TENANT_ID = ${JSON.stringify(config.tenantId)}`,
    `CLIENT_ID = ${JSON.stringify(config.clientId)}`,
    `CLIENT_SECRET = ${JSON.stringify(config.clientSecret)}`,
    "def token():",
    "    payload = urllib.parse.urlencode({",
    "        'grant_type': 'client_credentials',",
    "        'client_id': CLIENT_ID,",
    "        'client_secret': CLIENT_SECRET,",
    "        'scope': 'https://storage.azure.com/.default',",
    "    }).encode('utf-8')",
    "    req = urllib.request.Request(f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token', data=payload, headers={'Content-Type': 'application/x-www-form-urlencoded'})",
    "    with urllib.request.urlopen(req) as resp:",
    "        return json.loads(resp.read().decode('utf-8'))['access_token']",
    "def download(url, destination):",
    "    req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token()}', 'x-ms-version': '2023-11-03'})",
    "    with urllib.request.urlopen(req) as resp, open(destination, 'wb') as handle:",
    "        handle.write(resp.read())",
    "if __name__ == '__main__':",
    "    with open(sys.argv[1], 'r', encoding='utf-8') as payload_handle:",
    "        payload = json.load(payload_handle)",
    "    target_dir = sys.argv[2]",
    "    os.makedirs(target_dir, exist_ok=True)",
    "    for item in payload:",
    "        download(item['url'], os.path.join(target_dir, item['name']))",
  ].join("\n");
  const pythonPayload = JSON.stringify(
    inputFiles.map((file) => ({
      name: sanitizeUploadName(file.name),
      url: buildBlobUrlFromName(config, file.blobName),
    })),
  );
  return [
    "sas2py_input_dir <- tempfile('sas2py-input-')",
    "dir.create(sas2py_input_dir, recursive = TRUE, showWarnings = FALSE)",
    "Sys.setenv(SAS2PY_INPUT_DIR = sas2py_input_dir)",
    "setwd(sas2py_input_dir)",
    `sas2py_python_script <- ${JSON.stringify(pythonScript)}`,
    `sas2py_python_payload <- ${JSON.stringify(pythonPayload)}`,
    "sas2py_python_path <- tempfile('sas2py-blob-download-', fileext = '.py')",
    "sas2py_payload_path <- tempfile('sas2py-blob-download-', fileext = '.json')",
    "writeLines(sas2py_python_script, sas2py_python_path, useBytes = TRUE)",
    "writeLines(sas2py_python_payload, sas2py_payload_path, useBytes = TRUE)",
    "sas2py_python_bin <- Sys.which('python3')",
    "if (sas2py_python_bin == '') sas2py_python_bin <- Sys.which('python')",
    "if (sas2py_python_bin == '') stop('Python runtime is required for Azure blob download bootstrap.')",
    "sas2py_status <- system2(sas2py_python_bin, c(sas2py_python_path, sas2py_payload_path, sas2py_input_dir), stdout = TRUE, stderr = TRUE)",
    "sas2py_exit <- attr(sas2py_status, 'status')",
    "if (!is.null(sas2py_exit) && sas2py_exit != 0) stop(paste(sas2py_status, collapse='\\n'))",
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

function applyDatabricksDirectBlobSetup(
  code: string,
  language: ExecutionLanguage,
  inputFiles: { name: string; blobName: string }[],
  config: DatabricksBlobPathConfig,
) {
  if (inputFiles.length === 0) return code;
  const setup =
    language === "R"
      ? buildRDatabricksDirectBlobSetup(inputFiles, config)
      : buildPythonDatabricksDirectBlobSetup(inputFiles, config);
  return `${setup}\n${code}`;
}

function buildDatabricksBlobBootstrapCode(
  language: ExecutionLanguage,
  codeUrl: string,
) {
  if (language === "R") {
    return [
      "sas2py_code_url <- " + JSON.stringify(codeUrl),
      "sas2py_code_path <- tempfile('sas2py-code-', fileext = '.R')",
      "download.file(sas2py_code_url, destfile = sas2py_code_path, mode = 'wb', quiet = TRUE)",
      "sas2py_downloaded_lines <- readLines(sas2py_code_path, warn = FALSE, encoding = 'UTF-8')",
      "eval(parse(text = paste(sas2py_downloaded_lines, collapse = '\\n')), envir = .GlobalEnv)",
    ].join("\n");
  }

  return [
    "import urllib.request as __sas2py_code_request",
    "sas2py_code_url = " + JSON.stringify(codeUrl),
    "with __sas2py_code_request.urlopen(sas2py_code_url) as __sas2py_code_response:",
    "    __sas2py_downloaded_code = __sas2py_code_response.read().decode('utf-8')",
    "exec(compile(__sas2py_downloaded_code, '<sas2py-blob-runner>', 'exec'), {'__name__': '__main__'})",
  ].join("\n");
}

function buildDatabricksDirectBlobBootstrapCode(
  language: ExecutionLanguage,
  blobName: string,
  config: DatabricksBlobPathConfig,
) {
  const codeUrl = buildBlobUrlFromName(config, blobName);
  if (language === "R") {
    const pythonScript = [
      "import json",
      "import sys",
      "import urllib.parse",
      "import urllib.request",
      `TENANT_ID = ${JSON.stringify(config.tenantId)}`,
      `CLIENT_ID = ${JSON.stringify(config.clientId)}`,
      `CLIENT_SECRET = ${JSON.stringify(config.clientSecret)}`,
      "payload = urllib.parse.urlencode({",
      "    'grant_type': 'client_credentials',",
      "    'client_id': CLIENT_ID,",
      "    'client_secret': CLIENT_SECRET,",
      "    'scope': 'https://storage.azure.com/.default',",
      "}).encode('utf-8')",
      "req = urllib.request.Request(f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token', data=payload, headers={'Content-Type': 'application/x-www-form-urlencoded'})",
      "with urllib.request.urlopen(req) as resp:",
      "    token = json.loads(resp.read().decode('utf-8'))['access_token']",
      "download_req = urllib.request.Request(sys.argv[1], headers={'Authorization': f'Bearer {token}', 'x-ms-version': '2023-11-03'})",
      "with urllib.request.urlopen(download_req) as resp, open(sys.argv[2], 'wb') as handle:",
      "    handle.write(resp.read())",
    ].join("\n");
    return [
      `sas2py_code_url <- ${JSON.stringify(codeUrl)}`,
      "sas2py_code_path <- tempfile('sas2py-code-', fileext = '.R')",
      `sas2py_python_script <- ${JSON.stringify(pythonScript)}`,
      "sas2py_python_path <- tempfile('sas2py-blob-code-', fileext = '.py')",
      "writeLines(sas2py_python_script, sas2py_python_path, useBytes = TRUE)",
      "sas2py_python_bin <- Sys.which('python3')",
      "if (sas2py_python_bin == '') sas2py_python_bin <- Sys.which('python')",
      "if (sas2py_python_bin == '') stop('Python runtime is required for Azure blob download bootstrap.')",
      "sas2py_status <- system2(sas2py_python_bin, c(sas2py_python_path, sas2py_code_url, sas2py_code_path), stdout = TRUE, stderr = TRUE)",
      "sas2py_exit <- attr(sas2py_status, 'status')",
      "if (!is.null(sas2py_exit) && sas2py_exit != 0) stop(paste(sas2py_status, collapse='\\n'))",
      "sas2py_downloaded_lines <- readLines(sas2py_code_path, warn = FALSE, encoding = 'UTF-8')",
      "eval(parse(text = paste(sas2py_downloaded_lines, collapse = '\\n')), envir = .GlobalEnv)",
    ].join("\n");
  }

  return [
    buildPythonOAuthBlobDownloader(config),
    "import tempfile as __sas2py_code_tempfile",
    "sas2py_code_url = " + JSON.stringify(codeUrl),
    "sas2py_code_path = __sas2py_code_tempfile.mktemp(prefix='sas2py-code-', suffix='.py')",
    "__sas2py_blob_download(sas2py_code_url, sas2py_code_path)",
    "with open(sas2py_code_path, 'r', encoding='utf-8') as __sas2py_code_handle:",
    "    __sas2py_downloaded_code = __sas2py_code_handle.read()",
    "exec(compile(__sas2py_downloaded_code, '<sas2py-blob-runner>', 'exec'), {'__name__': '__main__'})",
  ].join("\n");
}

function byteLengthUtf8(value: string) {
  return Buffer.byteLength(value, "utf8");
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
  const maxArtifactBytes = parsePositiveInt(
    process.env.CODE_RUNNER_MAX_ARTIFACT_BYTES,
    DEFAULT_MAX_ARTIFACT_BYTES,
  );
  const maxArtifactTotalBytes = parsePositiveInt(
    process.env.CODE_RUNNER_MAX_ARTIFACT_TOTAL_BYTES,
    DEFAULT_MAX_ARTIFACT_TOTAL_BYTES,
  );
  const maxArtifactCount = parsePositiveInt(
    process.env.CODE_RUNNER_MAX_ARTIFACT_COUNT,
    DEFAULT_MAX_ARTIFACT_COUNT,
  );
  const allowedExtensions = JSON.stringify([...ALLOWED_ARTIFACT_EXTENSIONS]);
  return [
    "import base64 as __sas2py_b64",
    "import ast as __sas2py_ast",
    "import io as __sas2py_io",
    "import json as __sas2py_json",
    "import mimetypes as __sas2py_mimetypes",
    "import os as __sas2py_os",
    "from pathlib import Path as __sas2py_Path",
    "import sys as __sas2py_sys",
    "import tempfile as __sas2py_tempfile",
    "import traceback as __sas2py_traceback",
    "from contextlib import redirect_stderr as __sas2py_redirect_stderr, redirect_stdout as __sas2py_redirect_stdout",
    "__sas2py_user_code = " + escaped,
    "__sas2py_scope = {'__name__': '__main__'}",
    "__sas2py_stdout = __sas2py_io.StringIO()",
    "__sas2py_stderr = __sas2py_io.StringIO()",
    "__sas2py_error = ''",
    "__sas2py_images = []",
    "__sas2py_artifacts = []",
    "__sas2py_seen_figs = set()",
    "__sas2py_allowed_artifact_extensions = set(" + allowedExtensions + ")",
    "__sas2py_max_artifact_bytes = " + String(maxArtifactBytes),
    "__sas2py_max_artifact_total_bytes = " + String(maxArtifactTotalBytes),
    "__sas2py_max_artifact_count = " + String(maxArtifactCount),
    "__sas2py_mpl_config_dir = __sas2py_tempfile.mkdtemp(prefix='sas2py-mpl-')",
    "__sas2py_os.environ['MPLCONFIGDIR'] = __sas2py_mpl_config_dir",
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
    "def __sas2py_snapshot_files(__Path=__sas2py_Path):",
    "    __snapshot = {}",
    "    __root = __Path.cwd()",
    "    for __path in __root.rglob('*'):",
    "        try:",
    "            if not __path.is_file():",
    "                continue",
    "            if any(__part in {'.git', '__pycache__'} for __part in __path.parts):",
    "                continue",
    "            __relative = __path.relative_to(__root).as_posix()",
    "            __stat = __path.stat()",
    "            __snapshot[__relative] = (__stat.st_size, __stat.st_mtime_ns)",
    "        except Exception:",
    "            continue",
    "    return __snapshot",
    "def __sas2py_collect_artifacts(__before, __Path=__sas2py_Path, __mimetypes=__sas2py_mimetypes, __b64=__sas2py_b64):",
    "    __artifacts = []",
    "    __total_bytes = 0",
    "    __root = __Path.cwd()",
    "    for __path in sorted(__root.rglob('*')):",
    "        try:",
    "            if not __path.is_file():",
    "                continue",
    "            if any(__part in {'.git', '__pycache__'} for __part in __path.parts):",
    "                continue",
    "            __relative = __path.relative_to(__root).as_posix()",
    "            __suffix = __path.suffix.lower()",
    "            if __suffix not in __sas2py_allowed_artifact_extensions:",
    "                continue",
    "            __stat = __path.stat()",
    "            __previous = __before.get(__relative)",
    "            if __previous == (__stat.st_size, __stat.st_mtime_ns):",
    "                continue",
    "            if __stat.st_size <= 0 or __stat.st_size > __sas2py_max_artifact_bytes:",
    "                continue",
    "            if len(__artifacts) >= __sas2py_max_artifact_count:",
    "                break",
    "            if __total_bytes + __stat.st_size > __sas2py_max_artifact_total_bytes:",
    "                break",
    "            __content = __path.read_bytes()",
    "            __content_type = __mimetypes.guess_type(__path.name)[0] or 'application/octet-stream'",
    "            __artifacts.append({",
    "                'name': __relative,",
    "                'contentType': __content_type,",
    "                'sizeBytes': __stat.st_size,",
    "                'contentBase64': __b64.b64encode(__content).decode('ascii'),",
    "            })",
    "            __total_bytes += __stat.st_size",
    "        except Exception:",
    "            continue",
    "    return __artifacts",
    "__sas2py_before_files = __sas2py_snapshot_files()",
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
    "__sas2py_artifacts = __sas2py_collect_artifacts(__sas2py_before_files)",
    "try:",
    "    import matplotlib.pyplot as __sas2py_plt",
    "    __sas2py_plt.close('all')",
    "except Exception:",
    "    pass",
    "__sas2py_payload = {",
    "    'stdout': __sas2py_stdout.getvalue(),",
    "    'stderr': __sas2py_stderr.getvalue() + (('\\n' + __sas2py_error) if __sas2py_error else ''),",
    "    'images': __sas2py_images,",
    "    'artifacts': __sas2py_artifacts,",
    "}",
    `print('${marker}' + __sas2py_json.dumps(__sas2py_payload))`,
  ].join("\n");
}

function buildRExecutionEnvelope(code: string) {
  const escaped = JSON.stringify(sanitizeRSource(code));
  const marker = "__SAS2PY_RESULT__";
  const maxArtifactBytes = parsePositiveInt(
    process.env.CODE_RUNNER_MAX_ARTIFACT_BYTES,
    DEFAULT_MAX_ARTIFACT_BYTES,
  );
  const maxArtifactTotalBytes = parsePositiveInt(
    process.env.CODE_RUNNER_MAX_ARTIFACT_TOTAL_BYTES,
    DEFAULT_MAX_ARTIFACT_TOTAL_BYTES,
  );
  const maxArtifactCount = parsePositiveInt(
    process.env.CODE_RUNNER_MAX_ARTIFACT_COUNT,
    DEFAULT_MAX_ARTIFACT_COUNT,
  );
  const allowedExtensionsLiteral = `c(${[...ALLOWED_ARTIFACT_EXTENSIONS]
    .map((extension) => JSON.stringify(extension))
    .join(", ")})`;
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
    "sas2py_json_artifact <- function(item) {",
    "  paste0(",
    "    '{',",
    "    '\"name\":', sas2py_json_string(item$name), ',',",
    "    '\"contentType\":', sas2py_json_string(item$contentType), ',',",
    "    '\"sizeBytes\":', as.character(item$sizeBytes), ',',",
    "    '\"contentBase64\":', sas2py_json_string(item$contentBase64),",
    "    '}'",
    "  )",
    "}",
    "sas2py_json_artifact_array <- function(items) {",
    "  if (!length(items)) return('[]')",
    "  paste0('[', paste(vapply(items, sas2py_json_artifact, character(1)), collapse = ','), ']')",
    "}",
    "sas2py_allowed_artifact_extensions <- " + allowedExtensionsLiteral,
    "sas2py_max_artifact_bytes <- " + String(maxArtifactBytes),
    "sas2py_max_artifact_total_bytes <- " + String(maxArtifactTotalBytes),
    "sas2py_max_artifact_count <- " + String(maxArtifactCount),
    "sas2py_guess_content_type <- function(path) {",
    "  extension <- tolower(tools::file_ext(path))",
    "  switch(extension,",
    "    csv = 'text/csv',",
    "    tsv = 'text/tab-separated-values',",
    "    txt = 'text/plain',",
    "    json = 'application/json',",
    "    xlsx = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',",
    "    xls = 'application/vnd.ms-excel',",
    "    pdf = 'application/pdf',",
    "    png = 'image/png',",
    "    html = 'text/html',",
    "    htm = 'text/html',",
    "    'application/octet-stream'",
    "  )",
    "}",
    "sas2py_snapshot_files <- function() {",
    "  files <- list.files('.', recursive = TRUE, all.files = TRUE, no.. = TRUE, full.names = FALSE, include.dirs = FALSE)",
    "  if (!length(files)) return(setNames(character(), character()))",
    "  files <- files[!grepl('(^|/)(\\\\.git|__pycache__)(/|$)', files)]",
    "  if (!length(files)) return(setNames(character(), character()))",
    "  info <- file.info(files)",
    "  values <- paste0(ifelse(is.na(info$size), 0, info$size), '|', ifelse(is.na(info$mtime), '', format(info$mtime, '%Y-%m-%dT%H:%M:%OS6', tz = 'UTC')))",
    "  setNames(values, rownames(info))",
    "}",
    "sas2py_collect_artifacts <- function(before) {",
    "  files <- list.files('.', recursive = TRUE, all.files = TRUE, no.. = TRUE, full.names = FALSE, include.dirs = FALSE)",
    "  files <- sort(files[!grepl('(^|/)(\\\\.git|__pycache__)(/|$)', files)])",
    "  if (!length(files)) return(list())",
    "  total_bytes <- 0",
    "  artifacts <- list()",
    "  for (path in files) {",
    "    extension <- tolower(paste0('.', tools::file_ext(path)))",
    "    if (!(extension %in% sas2py_allowed_artifact_extensions)) next",
    "    info <- file.info(path)",
    "    if (is.na(info$size) || info$size <= 0 || info$size > sas2py_max_artifact_bytes) next",
    "    current_value <- paste0(info$size, '|', format(info$mtime, '%Y-%m-%dT%H:%M:%OS6', tz = 'UTC'))",
    "    previous_value <- before[[path]]",
    "    if (!is.null(previous_value) && identical(previous_value, current_value)) next",
    "    if (length(artifacts) >= sas2py_max_artifact_count) break",
    "    if (total_bytes + info$size > sas2py_max_artifact_total_bytes) break",
    "    artifacts[[length(artifacts) + 1]] <- list(",
    "      name = path,",
    "      contentType = sas2py_guess_content_type(path),",
    "      sizeBytes = unname(as.integer(info$size)),",
    "      contentBase64 = sas2py_base64_encode(readBin(path, what = 'raw', n = info$size))",
    "    )",
    "    total_bytes <- total_bytes + info$size",
    "  }",
    "  artifacts",
    "}",
    "sas2py_before_files <- sas2py_snapshot_files()",
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
    "sas2py_artifacts <- sas2py_collect_artifacts(sas2py_before_files)",
    "sas2py_payload <- paste0(",
    "  '{',",
    "  '\"stdout\":', sas2py_json_string(sas2py_stdout), ',',",
    "  '\"stderr\":', sas2py_json_string(sas2py_stderr), ',',",
    "  '\"images\":', sas2py_json_array(sas2py_images), ',',",
    "  '\"artifacts\":', sas2py_json_artifact_array(sas2py_artifacts),",
    "  '}'",
    ")",
    `cat('${marker}', sas2py_payload, sep = '')`,
  ].join("\n");
}

function parseExecutionEnvelope(output: string) {
  const marker = "__SAS2PY_RESULT__";
  let current = output;
  let aggregatePrefix = "";
  let aggregateStderr = "";
  let aggregateImages: string[] = [];
  let aggregateArtifacts: RawExecutionArtifact[] = [];

  while (true) {
    const index = current.lastIndexOf(marker);
    if (index < 0) {
      break;
    }

    const prefix = current.slice(0, index).trim();
    const rawJson = current.slice(index + marker.length).trim();

    try {
      const parsed = JSON.parse(rawJson) as {
        stdout?: string;
        stderr?: string;
        images?: string[];
        artifacts?: RawExecutionArtifact[];
      };
      if (prefix) {
        aggregatePrefix = `${aggregatePrefix}${aggregatePrefix ? "\n" : ""}${prefix}`;
      }
      if (parsed.stderr) {
        aggregateStderr = `${aggregateStderr}${aggregateStderr && parsed.stderr ? "\n" : ""}${parsed.stderr}`;
      }
      if (Array.isArray(parsed.images)) {
        aggregateImages = aggregateImages.concat(
          parsed.images.filter((value) => typeof value === "string" && value.length > 0),
        );
      }
      if (Array.isArray(parsed.artifacts)) {
        aggregateArtifacts = aggregateArtifacts.concat(
          parsed.artifacts.filter(
            (value): value is RawExecutionArtifact =>
              Boolean(
                value &&
                  typeof value.name === "string" &&
                  typeof value.contentType === "string" &&
                  Number.isFinite(value.sizeBytes) &&
                  typeof value.contentBase64 === "string",
              ),
          ),
        );
      }
      current = parsed.stdout || "";
    } catch {
      return null;
    }
  }

  if (
    !aggregatePrefix &&
    !aggregateStderr &&
    aggregateImages.length === 0 &&
    aggregateArtifacts.length === 0 &&
    current === output
  ) {
    return null;
  }

  return {
    stdout: `${aggregatePrefix}${aggregatePrefix && current ? "\n" : ""}${current}`.trim(),
    stderr: aggregateStderr,
    images: aggregateImages,
    artifacts: aggregateArtifacts,
  };
}

async function materializeExecutionArtifacts(
  artifacts: RawExecutionArtifact[],
): Promise<ExecutionArtifact[]> {
  const normalized = artifacts.filter(
    (artifact) =>
      artifact.name.trim().length > 0 &&
      artifact.contentBase64.trim().length > 0 &&
      artifact.sizeBytes > 0,
  );
  if (normalized.length === 0) {
    return [];
  }

  if (!isAzureBlobUploadConfigured()) {
    return normalized.map((artifact) => ({
      name: artifact.name,
      contentType: artifact.contentType,
      sizeBytes: artifact.sizeBytes,
      contentBase64: artifact.contentBase64,
    }));
  }

  if (shouldAvoidSasUrls()) {
    return normalized.map((artifact) => ({
      name: artifact.name,
      contentType: artifact.contentType,
      sizeBytes: artifact.sizeBytes,
      contentBase64: artifact.contentBase64,
    }));
  }

  return Promise.all(
    normalized.map(async (artifact) => {
      const uploaded = await uploadBinaryToAzureAndGetSasUrl({
        fileName: sanitizeUploadName(artifact.name),
        content: Buffer.from(artifact.contentBase64, "base64"),
        contentType: artifact.contentType,
      });
      return {
        name: artifact.name,
        contentType: artifact.contentType,
        sizeBytes: artifact.sizeBytes,
        downloadUrl: uploaded.url,
      } satisfies ExecutionArtifact;
    }),
  );
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
  const directBlobConfig =
    shouldAvoidSasUrls() ? getDatabricksBlobPathConfig() : null;
  if (inputFiles.length > 0 && isAzureBlobUploadConfigured()) {
    const uploadedFiles = await uploadExecutionInputsToAzure(
      inputFiles.map((file) => ({
        name: sanitizeUploadName(file.name),
        content: file.content,
      })),
    );
    const payload = directBlobConfig
      ? applyDatabricksDirectBlobSetup(
          basePayloadCode,
          language,
          uploadedFiles,
          directBlobConfig,
        )
      : applyDatabricksBlobSetup(
          basePayloadCode,
          language,
          uploadedFiles.filter(
            (file) => typeof file.url === "string" && file.url.length > 0,
          ).map((file) => ({
            name: file.name,
            url: file.url as string,
          })),
        );
    if (byteLengthUtf8(payload) <= MAX_DATABRICKS_NOTEBOOK_PARAM_BYTES) {
      return payload;
    }
    const uploadedCode = await uploadTextToAzureAndGetSasUrl({
      fileName: language === "R" ? "sas2py-runner.R" : "sas2py-runner.py",
      content: payload,
      contentType: "text/plain; charset=utf-8",
    });
    if (directBlobConfig) {
      return buildDatabricksDirectBlobBootstrapCode(
        language,
        uploadedCode.blobName,
        directBlobConfig,
      );
    }
    if (!uploadedCode.url) {
      throw new Error("Azure blob upload succeeded, but no SAS URL was returned.");
    }
    return buildDatabricksBlobBootstrapCode(language, uploadedCode.url);
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
    const payload = applyDatabricksInputSetup(basePayloadCode, language, inputFiles);
    if (byteLengthUtf8(payload) <= MAX_DATABRICKS_NOTEBOOK_PARAM_BYTES) {
      return payload;
    }
    if (!isAzureBlobUploadConfigured()) {
      throw new Error(
        `Databricks notebook parameter payload is too large (${byteLengthUtf8(payload)} bytes). Configure Azure Blob Storage so the code payload can be offloaded, or shorten the code/input files.`,
      );
    }
    const uploadedCode = await uploadTextToAzureAndGetSasUrl({
      fileName: language === "R" ? "sas2py-runner.R" : "sas2py-runner.py",
      content: payload,
      contentType: "text/plain; charset=utf-8",
    });
    if (directBlobConfig) {
      return buildDatabricksDirectBlobBootstrapCode(
        language,
        uploadedCode.blobName,
        directBlobConfig,
      );
    }
    if (!uploadedCode.url) {
      throw new Error("Azure blob upload succeeded, but no SAS URL was returned.");
    }
    return buildDatabricksBlobBootstrapCode(language, uploadedCode.url);
  }
  if (byteLengthUtf8(basePayloadCode) <= MAX_DATABRICKS_NOTEBOOK_PARAM_BYTES) {
    return basePayloadCode;
  }
  if (!isAzureBlobUploadConfigured()) {
    throw new Error(
      `Databricks notebook parameter payload is too large (${byteLengthUtf8(basePayloadCode)} bytes). Configure Azure Blob Storage so the code payload can be offloaded, or shorten the generated code.`,
    );
  }
  const uploadedCode = await uploadTextToAzureAndGetSasUrl({
    fileName: language === "R" ? "sas2py-runner.R" : "sas2py-runner.py",
    content: basePayloadCode,
    contentType: "text/plain; charset=utf-8",
  });
  if (directBlobConfig) {
    return buildDatabricksDirectBlobBootstrapCode(
      language,
      uploadedCode.blobName,
      directBlobConfig,
    );
  }
  if (!uploadedCode.url) {
    throw new Error("Azure blob upload succeeded, but no SAS URL was returned.");
  }
  return buildDatabricksBlobBootstrapCode(language, uploadedCode.url);
}

async function submitDatabricksRun(
  payloadCode: string,
  language: ExecutionLanguage,
) {
  const jobId = getJobId(language);
  const { baseUrl, token } = getDatabricksConfig(language);
  const notebookParams: Record<string, string> = { code: payloadCode };
  if (language === "R") {
    const packages = splitByComma(process.env.CODE_RUNNER_DATABRICKS_R_PACKAGES)
      .join(",");
    if (packages) {
      notebookParams.packages = packages;
    }
    const cranRepo = process.env.CODE_RUNNER_DATABRICKS_R_CRAN_REPO?.trim();
    if (cranRepo) {
      notebookParams.cran_repo = cranRepo;
    }
  }
  const submit = await databricksApi<{ run_id: number }>(
    baseUrl,
    token,
    "/api/2.1/jobs/run-now",
    {
      method: "POST",
      body: JSON.stringify({
        job_id: jobId,
        notebook_params: notebookParams,
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
        artifacts: [],
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
  let artifacts: RawExecutionArtifact[] = [];
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
      artifacts = parsed.artifacts;
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
      artifacts,
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
      artifacts: await materializeExecutionArtifacts(status.result.artifacts),
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
        artifacts: [],
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
      let artifacts: RawExecutionArtifact[] = [];
      if (stdout) {
        const parsed = parseExecutionEnvelope(stdout);
        if (parsed) {
          stdout = parsed.stdout;
          stderr = `${stderr}${stderr && parsed.stderr ? "\n" : ""}${parsed.stderr}`;
          images = parsed.images;
          artifacts = parsed.artifacts;
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
        artifacts,
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
  const artifacts = await materializeExecutionArtifacts(rawResult.artifacts);

  return {
    stdout: truncateOutput(rawResult.stdout, maxOutputChars),
    stderr: truncateOutput(rawResult.stderr, maxOutputChars),
    exitCode: rawResult.exitCode,
    timedOut: rawResult.timedOut,
    durationMs: rawResult.durationMs,
    detectedPackages,
    policyMode: mode,
    images: rawResult.images,
    artifacts,
    backend,
  } satisfies CodeExecutionResult;
}

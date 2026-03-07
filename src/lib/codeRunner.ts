import "server-only";

export type ExecutionLanguage = "PYTHON" | "R";
type PackagePolicyMode = "off" | "blocklist" | "allowlist";
type RunState = {
  life_cycle_state?: string;
  result_state?: string;
  state_message?: string;
};
type RunTask = {
  run_id?: number;
  task_key?: string;
  state?: RunState;
};
type RunDetails = {
  state?: RunState;
  tasks?: RunTask[];
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
};

const DEFAULT_MAX_OUTPUT_CHARS = 100_000;
const DEFAULT_POLL_INTERVAL_MS = 2_000;
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
  if (value.length <= maxChars) {
    return value;
  }
  return `${value.slice(0, maxChars)}\n\n[output truncated]`;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function splitPackageList(value: string | undefined) {
  return (value || "")
    .split(",")
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
}

function unique(values: string[]) {
  return [...new Set(values)];
}

function parsePolicyMode(value: string | undefined): PackagePolicyMode {
  const mode = (value || "blocklist").trim().toLowerCase();
  if (mode === "off" || mode === "allowlist" || mode === "blocklist") {
    return mode;
  }
  return "blocklist";
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

    for (const match of line.matchAll(/(?:library|require)\(\s*([A-Za-z0-9\._]+)\s*\)/g)) {
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

function validatePackagePolicy(code: string, language: ExecutionLanguage) {
  const mode = parsePolicyMode(process.env.CODE_RUNNER_PACKAGE_POLICY);
  const detectedPackages =
    language === "R" ? extractRPackages(code) : extractPythonPackages(code);

  if (mode === "off") {
    return { mode, detectedPackages };
  }

  const envPrefix = language === "R" ? "CODE_RUNNER_R" : "CODE_RUNNER_PYTHON";
  const defaultBlocklist =
    language === "R" ? DEFAULT_R_BLOCKLIST : DEFAULT_PYTHON_BLOCKLIST;
  const blocklist = unique(
    splitPackageList(process.env[`${envPrefix}_BLOCKLIST`]).concat(
      defaultBlocklist,
    ),
  );
  const allowlist = unique(
    splitPackageList(process.env[`${envPrefix}_ALLOWLIST`]),
  );

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

function getDatabricksConfig() {
  const host = process.env.DATABRICKS_SERVER_HOSTNAME?.trim();
  const token = process.env.DATABRICKS_ACCESS_TOKEN?.trim();
  if (!host || !token) {
    throw new Error(
      "Databricks Jobs runner requires DATABRICKS_SERVER_HOSTNAME and DATABRICKS_ACCESS_TOKEN.",
    );
  }
  const baseUrl = host.startsWith("http") ? host : `https://${host}`;
  return { baseUrl, token };
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

function buildJobCodePayload(code: string, language: ExecutionLanguage) {
  if (language !== "PYTHON") {
    return code;
  }

  // Wrap user code so imports/functions execute in one namespace and embed a
  // structured result payload that includes captured matplotlib figures.
  const escaped = JSON.stringify(code);
  const marker = "__SAS2PY_RESULT__";
  return [
    "import base64 as __sas2py_b64",
    "import ast as __sas2py_ast",
    "import io as __sas2py_io",
    "import json as __sas2py_json",
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
    "def __sas2py_exec_with_notebook_tail(__code, __scope):",
    "    try:",
    "        __module = __sas2py_ast.parse(__code, mode='exec')",
    "    except Exception:",
    "        exec(__code, __scope, __scope)",
    "        return",
    "    if __module.body and isinstance(__module.body[-1], __sas2py_ast.Expr):",
    "        __prefix = __sas2py_ast.Module(body=__module.body[:-1], type_ignores=[])",
    "        exec(compile(__prefix, '<sas2py>', 'exec'), __scope, __scope)",
    "        __last = __sas2py_ast.Expression(__module.body[-1].value)",
    "        __value = eval(compile(__last, '<sas2py>', 'eval'), __scope, __scope)",
    "        __sas2py_render_value(__value)",
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
    "    with __sas2py_redirect_stdout(__sas2py_stdout), __sas2py_redirect_stderr(__sas2py_stderr):",
    "        __sas2py_exec_with_notebook_tail(__sas2py_user_code, __sas2py_scope)",
    "except Exception:",
    "    __sas2py_error = __sas2py_traceback.format_exc()",
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

function parsePythonExecutionEnvelope(output: string) {
  const marker = "__SAS2PY_RESULT__";
  const index = output.lastIndexOf(marker);
  if (index < 0) {
    return null;
  }

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
      `Databricks Jobs API failed (${response.status}) on ${path}: ${text.slice(
        0,
        300,
      )}`,
    );
  }
  return (await response.json()) as T;
}

function getPreferredTaskKey(language: ExecutionLanguage) {
  return (
    (language === "R"
      ? process.env.CODE_RUNNER_DATABRICKS_R_TASK_KEY
      : process.env.CODE_RUNNER_DATABRICKS_PYTHON_TASK_KEY) || ""
  ).trim();
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
    notebook_output?: {
      result?: string;
      truncated?: boolean;
    };
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
    if (!isMultiTaskOutputError(error)) {
      throw error;
    }

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
          errors.push(
            `${task.task_key || task.run_id}: ${taskError.message.slice(0, 200)}`,
          );
        }
      }
    }

    throw new Error(
      `Failed to retrieve output from task runs. ${errors.join(" | ")}`,
    );
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
    // Ignore cancellation errors so timeout response still returns.
  }
}

export async function runCodeInContainer(
  code: string,
  language: ExecutionLanguage,
) {
  if (!code.trim()) {
    throw new Error("Code is required.");
  }

  const timeoutMs = parsePositiveInt(
    process.env.CODE_RUNNER_TIMEOUT_MS,
    120_000,
  );
  const maxOutputChars = parsePositiveInt(
    process.env.CODE_RUNNER_MAX_OUTPUT_CHARS,
    DEFAULT_MAX_OUTPUT_CHARS,
  );
  const pollIntervalMs = parsePositiveInt(
    process.env.CODE_RUNNER_DATABRICKS_POLL_INTERVAL_MS,
    DEFAULT_POLL_INTERVAL_MS,
  );
  const { mode, detectedPackages } = validatePackagePolicy(code, language);
  const payloadCode = buildJobCodePayload(code, language);
  const jobId = getJobId(language);
  const { baseUrl, token } = getDatabricksConfig();

  const startedAt = Date.now();
  const submit = await databricksApi<{ run_id: number }>(
    baseUrl,
    token,
    "/api/2.1/jobs/run-now",
    {
      method: "POST",
      body: JSON.stringify({
        job_id: jobId,
        notebook_params: {
          code: payloadCode,
        },
      }),
    },
  );

  let runStateMessage = "";
  let resultState = "";
  let timedOut = false;

  while (true) {
    if (Date.now() - startedAt > timeoutMs) {
      timedOut = true;
      await cancelRun(baseUrl, token, submit.run_id);
      break;
    }

    const status = await getRunDetails(baseUrl, token, submit.run_id);

    const lifeCycle = status.state?.life_cycle_state || "";
    resultState = status.state?.result_state || "";
    runStateMessage = status.state?.state_message || "";

    if (
      lifeCycle === "TERMINATED" ||
      lifeCycle === "SKIPPED" ||
      lifeCycle === "INTERNAL_ERROR"
    ) {
      break;
    }

    await sleep(pollIntervalMs);
  }

  let stdout = "";
  let stderr = "";
  let images: string[] = [];
  if (!timedOut) {
    try {
      const output = await getOutputWithTaskFallback(
        baseUrl,
        token,
        submit.run_id,
        language,
      );
      stdout = output.notebook_output?.result || "";
      stderr = output.error || "";
      if (output.notebook_output?.truncated) {
        stdout = `${stdout}\n\n[output truncated by Databricks]`;
      }
    } catch (error) {
      stderr =
        error instanceof Error
          ? error.message
          : "Failed to fetch Databricks run output.";
    }
  }

  if (language === "PYTHON" && stdout) {
    const parsed = parsePythonExecutionEnvelope(stdout);
    if (parsed) {
      stdout = parsed.stdout;
      stderr = `${stderr}${stderr && parsed.stderr ? "\n" : ""}${parsed.stderr}`;
      images = parsed.images;
    }
  }

  if (!stdout && runStateMessage) {
    stdout = runStateMessage;
  }

  if (timedOut) {
    stderr = stderr || "Execution timed out and run was cancelled.";
  }

  const exitCode =
    timedOut || (resultState && resultState !== "SUCCESS") ? 1 : 0;

  return {
    stdout: truncateOutput(stdout, maxOutputChars),
    stderr: truncateOutput(stderr, maxOutputChars),
    exitCode,
    timedOut,
    durationMs: Date.now() - startedAt,
    detectedPackages,
    policyMode: mode,
    images,
  };
}

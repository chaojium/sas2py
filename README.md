# SAS2Py Studio

Convert SAS scripts to Python or R with Codex CLI, then keep a review trail in Databricks SQL behind NextAuth.

## Features

- Codex CLI conversion
- NextAuth-secured workspace
- Databricks SQL storage for SAS and converted code
- Review notes and ratings per conversion

## Setup

1. Install dependencies:

```bash
npm install
```

2. Copy and update environment variables:

```bash
copy .env.example .env
```

3. Configure your Databricks SQL connection (see below).
4. Start the app:

```bash
npm run dev
```

## Databricks SQL

Set the following in `.env`:

- `DATABRICKS_SERVER_HOSTNAME`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_ACCESS_TOKEN`
- `DATABRICKS_CATALOG` (default: `sas2py`)
- `DATABRICKS_SCHEMA` (default: `prod`)

Create tables in Databricks:

```sql
CREATE CATALOG IF NOT EXISTS sas2py;
CREATE SCHEMA IF NOT EXISTS sas2py.prod;

CREATE TABLE IF NOT EXISTS sas2py.prod.code_entries (
  id STRING,
  user_id STRING,
  name STRING,
  language STRING,
  sas_code STRING,
  python_code STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sas2py.prod.code_reviews (
  id STRING,
  code_entry_id STRING,
  reviewer_id STRING,
  rating INT,
  summary STRING,
  comments STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sas2py.prod.code_runs (
  id STRING,
  code_entry_id STRING,
  user_id STRING,
  language STRING,
  stdout STRING,
  stderr STRING,
  exit_code INT,
  timed_out BOOLEAN,
  duration_ms INT,
  detected_packages STRING,
  policy_mode STRING,
  created_at TIMESTAMP
);
```

## Codex CLI

Set `CODEX_CLI_COMMAND` and `CODEX_CLI_ARGS` in `.env`. The app sends the SAS prompt over stdin.

## Auth

This project ships with the GitHub provider enabled. Add `GITHUB_ID` and `GITHUB_SECRET` to `.env`, or swap providers in `src/lib/auth.ts`.

## Docker

Build image:

```bash
docker build -t sas2py:latest .
```

Run container:

```bash
docker run --rm -p 3000:3000 --env-file .env sas2py:latest
```

Notes:

- The app reads runtime configuration from `.env`.
- Conversion endpoints call the `codex` CLI from the backend process. Ensure the container image has Codex CLI installed and authenticated, or override `CODEX_CLI_COMMAND` to a command available in the container.
- Code execution endpoints call Databricks Jobs from the backend process.

## Databricks Jobs Code Runner

After conversion, users can run generated Python or R via Databricks Jobs through `POST /api/execute`.

Set these optional `.env` variables to tune the runner:

- `CODE_RUNNER_DATABRICKS_PYTHON_JOB_ID` (required for Python execution)
- `CODE_RUNNER_DATABRICKS_R_JOB_ID` (required for R execution)
- `CODE_RUNNER_DATABRICKS_PYTHON_TASK_KEY` (optional, recommended if Python job has multiple tasks)
- `CODE_RUNNER_DATABRICKS_R_TASK_KEY` (optional, recommended if R job has multiple tasks)
- `CODE_RUNNER_TIMEOUT_MS` (default: `120000`)
- `CODE_RUNNER_DATABRICKS_POLL_INTERVAL_MS` (default: `2000`)
- `CODE_RUNNER_MAX_OUTPUT_CHARS` (default: `100000`)
- `CODE_RUNNER_PACKAGE_POLICY` (`off`, `blocklist`, `allowlist`; default: `blocklist`)
- `CODE_RUNNER_PYTHON_BLOCKLIST` (comma-separated modules)
- `CODE_RUNNER_R_BLOCKLIST` (comma-separated packages)
- `CODE_RUNNER_PYTHON_ALLOWLIST` (comma-separated modules; required if policy is `allowlist`)
- `CODE_RUNNER_R_ALLOWLIST` (comma-separated packages; required if policy is `allowlist`)

Job contract:

- Configure each Databricks job to accept a notebook parameter named `code`.
- The notebook should execute that code and return a result string via `dbutils.notebook.exit(...)` for stdout-like output.
- If a job has multiple tasks, set the corresponding `..._TASK_KEY` so output retrieval targets the correct task.

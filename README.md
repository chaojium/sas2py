# SAS2Py Studio

Convert SAS scripts to Python or R with OpenAI GPT-5.2, then keep a review trail in Databricks SQL behind NextAuth.

## Features

- GPT-5.2 conversion
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

## OpenAI API

Set these in `.env`:

- `OPENAI_API_KEY`
- `OPENAI_MODEL` (default: `gpt-5.2`)
- `OPENAI_TIMEOUT_MS` (default: `90000`)
- `OPENAI_CA_CERT_PATH` (optional PEM file for corporate TLS interception)

## Auth

This project uses Firebase Authentication.

Set these in `.env`:

- `NEXT_PUBLIC_FIREBASE_API_KEY`
- `NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN`
- `NEXT_PUBLIC_FIREBASE_PROJECT_ID`
- `NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET`
- `NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID`
- `NEXT_PUBLIC_FIREBASE_APP_ID`
- `NEXT_PUBLIC_FIREBASE_MEASUREMENT_ID` (optional)

The app supports:

- Email and password sign-in
- Google sign-in

In Firebase Console, enable both providers under Authentication.

For local development behind a corporate TLS proxy, you can temporarily set
`DEV_AUTH_BYPASS=true` to trust Firebase token payloads without server-side
signature verification. This should only be used in local development, never in
production.

If OpenAI requests fail with `self-signed certificate in certificate chain`,
configure Node to trust your corporate CA. You can either set
`OPENAI_CA_CERT_PATH` to a PEM file for this app's OpenAI requests, or set
`NODE_EXTRA_CA_CERTS` for the whole Node process.

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
- Conversion endpoints call OpenAI Responses API from the backend process.
- Code execution endpoints call Databricks Jobs from the backend process.

## Code Runner

After conversion, users can run generated code through `POST /api/execute` with per-language backends:

- Python: Databricks Jobs
- R: Docker

Set these optional `.env` variables to tune the runner:

- `CODE_RUNNER_BACKEND_R` (`docker` or `databricks`; default: `docker`)
- `CODE_RUNNER_DOCKER_COMMAND` (default: `docker`)
- `CODE_RUNNER_DOCKER_PYTHON_IMAGE` (default: `python:3.12-slim`)
- `CODE_RUNNER_DOCKER_R_IMAGE` (default: `r-base:4.3.3`)
- `CODE_RUNNER_DOCKER_ARGS` (comma-separated docker run args, e.g. `--network=none,--cpus=2`)
- `CODE_RUNNER_DATABRICKS_PYTHON_JOB_ID` (required when Python backend is `databricks`)
- `CODE_RUNNER_DATABRICKS_R_JOB_ID` (required when R backend is `databricks`)
- `CODE_RUNNER_DATABRICKS_PYTHON_TASK_KEY` (optional, recommended if Python job has multiple tasks)
- `CODE_RUNNER_DATABRICKS_R_TASK_KEY` (optional, recommended if R job has multiple tasks)
- `CODE_RUNNER_DATABRICKS_PYTHON_HOST` (optional; overrides shared Databricks host for Python job runs)
- `CODE_RUNNER_DATABRICKS_PYTHON_TOKEN` (optional; overrides shared Databricks token for Python job runs)
- `CODE_RUNNER_DATABRICKS_R_HOST` (optional; use this when R runs in a different Databricks workspace/account)
- `CODE_RUNNER_DATABRICKS_R_TOKEN` (optional; token for the R runner workspace/account)
- `AZURE_STORAGE_CONNECTION_STRING` (optional; enables Blob handoff for Databricks input files)
- `AZURE_STORAGE_ACCOUNT_NAME` (required with `AZURE_STORAGE_ACCOUNT_KEY` to generate SAS URLs)
- `AZURE_STORAGE_ACCOUNT_KEY` (required with `AZURE_STORAGE_ACCOUNT_NAME` to generate SAS URLs)
- `AZURE_STORAGE_CONTAINER` (default: `sas2py-inputs`)
- `AZURE_STORAGE_BLOB_PREFIX` (default: `execution-inputs`)
- `CODE_RUNNER_TIMEOUT_MS` (default: `120000`)
- `CODE_RUNNER_DATABRICKS_POLL_INTERVAL_MS` (default: `2000`)
- `CODE_RUNNER_DATABRICKS_INPUT_FILE_MAX_BYTES` (default: `256000`; used only when Azure Blob handoff is not configured)
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
- If Python and R run in different Databricks workspaces, set the corresponding `..._HOST` and `..._TOKEN` values for each language.
- For uploaded execution input files with Databricks, configure Azure Blob Storage in the app. The backend uploads files to Blob, injects short-lived SAS URLs into the generated `code` payload, and the payload downloads files into `SAS2PY_INPUT_DIR` before the user code runs.

ECPaaS note:

- You can host a dedicated Docker runner image on ECPaaS and point Docker runtime to that image via `CODE_RUNNER_DOCKER_PYTHON_IMAGE` and `CODE_RUNNER_DOCKER_R_IMAGE`.

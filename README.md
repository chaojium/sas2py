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
```

## Codex CLI

Set `CODEX_CLI_COMMAND` and `CODEX_CLI_ARGS` in `.env`. The app sends the SAS prompt over stdin.

## Auth

This project ships with the GitHub provider enabled. Add `GITHUB_ID` and `GITHUB_SECRET` to `.env`, or swap providers in `src/lib/auth.ts`.

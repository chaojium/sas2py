# SAS2Py Studio Architecture

## Overview

SAS2Py Studio is a Next.js application with a protected browser UI, server-side API routes, Azure OpenAI conversion/refinement services, Databricks-backed operational storage, PostgreSQL/Prisma authentication storage, and configurable code execution backends.

## Technology Stack

- Next.js 16 App Router.
- React 19.
- TypeScript.
- Tailwind CSS.
- NextAuth / Firebase-backed auth helpers.
- Prisma with PostgreSQL for user/auth-related data.
- Databricks SQL for conversion, review, run, enhancement, project, and conversation data.
- Azure OpenAI for conversion, analysis, refinement, and assistant conversations.
- Databricks Jobs and Docker for code execution.
- Azure Blob Storage for large execution payloads, uploaded input files, and artifacts.

## High-Level Flow

```text
Browser UI
  -> Next.js API routes
    -> Auth/user ownership checks
    -> Azure OpenAI for conversion/refinement/chat
    -> Databricks SQL for app records
    -> Databricks Jobs or Docker for code execution
    -> Azure Blob Storage for file handoff/artifacts
```

## Frontend

Primary UI code lives under `src/components` and `src/app`.

Important components:

- `Converter.tsx`: main conversion workspace, execution controls, reviews, recent conversions, refinement, conversations, and artifact display.
- `CodeBlock.tsx`: rendered code/output blocks.
- `AuthProvider.tsx`, `AuthButton.tsx`: auth state and account controls.
- `DashboardClient.tsx`, `HistoryClient.tsx`, `SettingsClient.tsx`: supporting authenticated screens.

The frontend calls backend APIs through authenticated fetch helpers and does not directly call Azure OpenAI, Databricks, or Azure Storage.

## Backend API Routes

Primary API routes live under `src/app/api`.

- `conversions/route.ts`: conversion list, generation, refinement, manual code update, project assignment, deletion.
- `execute/route.ts`: synchronous Docker execution or asynchronous Databricks execution/polling.
- `conversations/route.ts`: conversion-specific assistant chat.
- `reviews/route.ts`: review creation.
- `sas-analysis/route.ts`: validation planning for SAS code.
- `projects/route.ts` and `projects/[projectId]/route.ts`: project management.
- `summary/route.ts`: user dashboard summary.
- `auth/*`: sign-in, sign-up, and profile APIs.
- `debug/*`: gated Databricks diagnostics.

All protected routes must verify the authenticated user and enforce record ownership.

## AI Conversion Layer

The conversion logic lives in `src/lib/codex.ts`.

Responsibilities:

- Build target-language prompts for SAS-to-Python and SAS-to-R conversion.
- Preserve SAS comments, statistical logic, survey design, SUDAAN behavior, confidence intervals, and file-path semantics.
- Accept optional user guidance and reference URLs.
- Validate generated Python/R before saving.
- Refine existing generated code based on user instructions.
- Analyze SAS code for expected outputs and validation checks.
- Discuss/debug conversion results in a conversation thread.

Azure OpenAI configuration is server-side and environment-driven.

## Storage

### PostgreSQL / Prisma

Prisma schema lives in `prisma/schema.prisma`.

Used for:

- Users.
- Accounts.
- Sessions.
- Verification tokens.
- Projects and code entries in the Prisma model layer.

### Databricks SQL

The app also stores operational records in Databricks SQL tables. The README lists expected tables such as:

- `code_entries`
- `code_reviews`
- `code_runs`
- `projects`
- `code_enhancements`
- `code_conversations`
- `code_conversation_messages`

The Databricks helper lives in `src/lib/databricks.ts`.

## Execution Layer

Execution logic lives in `src/lib/codeRunner.ts`.

Supported backends:

- Databricks Jobs.
- Docker containers.

Execution features:

- Python and R support.
- Uploaded input files.
- `SAS2PY_INPUT_DIR` setup.
- Package allowlist/blocklist policy.
- Timeouts.
- Output truncation.
- Image and artifact capture.
- Databricks asynchronous polling.
- Multi-task Databricks output fallback.
- Azure Blob handoff when notebook parameter payloads are too large.

## Azure Blob Storage

Blob handling lives in `src/lib/blobStorage.ts`.

Used for:

- Uploading execution input files.
- Uploading large generated code payloads for Databricks.
- Uploading execution artifacts.
- Creating SAS URLs or direct blob references depending on configuration.

For Azure-hosted deployments, prefer managed identity / Entra ID where possible.

## Configuration

Configuration is environment-variable driven. See `.env.example`.

Important groups:

- Azure OpenAI credentials, endpoint, model, API version, timeout, and optional CA certificate.
- Databricks SQL hostname, HTTP path, token, catalog, and schema.
- Databricks Jobs IDs, task keys, per-language host/token overrides.
- Docker runner image and runtime arguments.
- Azure Storage account/container/prefix and access mode.
- Auth database URL, NextAuth secret, and app URL.
- Package policy and execution limits.

Secrets must stay in `.env` or deployment secrets, never committed to source control.

## Deployment

The app can run locally with:

```bash
npm install
npm run dev
```

Production options include:

- Docker image using the root `Dockerfile`.
- Azure Web App or similar Node hosting.
- External Databricks workspace for SQL and Jobs.
- Optional separate Docker runner infrastructure for controlled R/Python execution.

## Security Considerations

- Keep Azure OpenAI, Databricks, database, and storage credentials server-side.
- Enforce auth and ownership checks on every protected API.
- Treat code execution as high risk; use timeout, package policy, network restrictions, and controlled backends.
- Avoid hardcoded local paths and user-specific filesystem paths in generated code.
- Avoid exposing full stack traces in production responses.
- Use managed identity for Azure Blob access where possible.

## Extension Points

- Add more SAS procedure prompt modules in `src/lib/codex.ts`.
- Add more target languages.
- Add more execution backends.
- Add richer validation reports comparing SAS and generated outputs.
- Add role-based access control for shared projects.
- Add automated regression tests using the `examples` folder.

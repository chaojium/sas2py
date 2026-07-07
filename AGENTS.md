# AGENTS.md

## Project Overview

SAS2Py Studio is a Next.js application for converting SAS programs to Python or R, preserving statistical intent, storing conversion history, supporting review/refinement workflows, and executing generated code through Databricks Jobs or Docker.

Use the existing planning documents for deeper context:

- `SPEC.md`: product behavior and acceptance criteria.
- `ARCHITECTURE.md`: system architecture and major modules.
- `TASKS.md`: implementation backlog and task tracking.
- `README.md`: setup, environment variables, Databricks tables, Docker, and runner configuration.

## Tech Stack

- Next.js 16 App Router
- React 19
- TypeScript
- Tailwind CSS 4
- NextAuth with Prisma/PostgreSQL for auth
- Databricks SQL for operational app records
- Azure OpenAI for conversion, analysis, refinement, and chat
- Databricks Jobs and Docker for code execution
- Azure Blob Storage for large execution payloads, uploaded files, and artifacts

## Common Commands

Run commands from the repository root.

```bash
npm install
npm run dev
npm run build
npm run lint
```

Prisma generation runs through `postinstall`. If Prisma client artifacts are stale, run:

```bash
npx prisma generate
```

## Repository Structure

- `src/app`: Next.js pages, layouts, providers, and API routes.
- `src/app/api`: backend route handlers for conversions, execution, projects, reviews, conversations, auth, and diagnostics.
- `src/components`: client UI components.
- `src/lib/codex.ts`: Azure OpenAI conversion, refinement, analysis, and conversation prompt logic.
- `src/lib/databricks.ts`: Databricks SQL access.
- `src/lib/codeRunner.ts`: Python/R execution orchestration.
- `src/lib/blobStorage.ts`: Azure Blob upload and SAS URL handling.
- `src/lib/auth`: authentication helpers.
- `prisma/schema.prisma`: Prisma models for auth and related data.
- `examples`: SAS, Python, R, and input examples for conversion and validation.
- `runner`: separate runner-related Docker support.
- `public/docs`: user-facing documentation assets.

## Coding Guidelines

- Follow existing project patterns before introducing new abstractions.
- Use TypeScript for app code.
- Keep frontend code in components and backend/external-service logic in API routes or `src/lib`.
- Do not call Azure OpenAI, Databricks, storage, or database services directly from browser components.
- Keep API responses explicit and avoid leaking full stack traces or secrets.
- Enforce authentication and user ownership checks on protected API routes.
- Avoid hardcoded local machine paths in generated or stored code.
- Keep changes narrow unless the task explicitly asks for a broader refactor.
- Prefer structured parsing or existing helpers over ad hoc string manipulation when handling records, code, JSON, or API payloads.

## AI Conversion Rules

When changing `src/lib/codex.ts` or conversion prompts:

- Preserve SAS comments, headers, banners, and file metadata where possible.
- Preserve analytical intent, not just syntax.
- Preserve survey design, SUDAAN behavior, weights, confidence intervals, degrees of freedom, and proportion/percent scale details where possible.
- Return code only for conversion/refinement calls unless the calling contract explicitly expects explanatory text.
- Validate generated Python or R before saving when the existing flow supports validation.
- Do not weaken existing statistical or validation guidance without a clear reason.

## Execution And Storage Rules

- Treat code execution as high risk.
- Preserve timeout, output truncation, artifact capture, and package policy behavior.
- Keep uploaded input files available to generated code through `SAS2PY_INPUT_DIR`.
- Prefer Azure Blob handoff for large payloads when configuration supports it.
- Do not bypass Databricks or Docker policy controls to make execution "just work."
- Keep secrets in `.env`, deployment configuration, or managed identity. Never commit real credentials.

## Environment And Secrets

- Use `.env.example` as the public reference for required configuration.
- Do not print or commit secret values from `.env`, `.env.local`, `.env.vercel`, Databricks tokens, Azure credentials, database URLs, or storage keys.
- If a task requires external services, explain which environment variables are required rather than inventing placeholder production values.

## UI Guidelines

- Keep the UI practical and data-focused.
- Use accessible labels and predictable controls for forms, tables, history, reviews, execution results, and settings.
- Make conversion, refinement, review, and execution workflows easy to scan.
- Avoid decorative layout changes unless the task is specifically about visual redesign.
- Ensure text and controls fit on smaller screens.

## Verification

Before finishing code changes, run the most relevant checks:

```bash
npm run lint
npm run build
```

If the change only touches docs, these commands may be unnecessary. If a command cannot be run because services, credentials, Docker, Databricks, or network access are unavailable, state that clearly in the final response.

For conversion or execution changes, also consider validating with examples from `examples`.

## Git And Workspace Safety

- Do not revert user changes unless explicitly asked.
- Be careful with generated directories such as `.next`, `node_modules`, `.npm-cache`, and temporary document build folders.
- Do not delete example files, documentation artifacts, migrations, or environment files unless the user specifically requests it.
- Keep migrations additive and review Prisma/Databricks schema implications before changing data models.


# SAS2Py Studio Build Tasks

## Project Setup

- [x] Create Next.js app with TypeScript.
- [x] Add Tailwind CSS styling.
- [x] Add package scripts for `dev`, `build`, `start`, and `lint`.
- [x] Add `.env.example` with required configuration groups.
- [x] Add Dockerfile for container deployment.

## Authentication

- [x] Add sign-in page.
- [x] Add sign-up API.
- [x] Add profile update API.
- [x] Protect app APIs with authenticated user checks.
- [x] Store user/account/session data through Prisma/PostgreSQL.

## Conversion Workspace

- [x] Build main converter UI.
- [x] Support pasted SAS code.
- [x] Support `.sas` file upload.
- [x] Support conversion name.
- [x] Support Python and R target languages.
- [x] Support additional guidance and reference URL.
- [x] Support conversion reuse and force regeneration.
- [x] Display generated code.
- [x] Support code download.
- [x] Support manual generated-code edits.

## AI Conversion Backend

- [x] Implement Azure OpenAI client configuration.
- [x] Implement SAS-to-Python conversion.
- [x] Implement SAS-to-R conversion.
- [x] Add prompt guardrails for comments, paths, statistical logic, survey design, SUDAAN, factor levels, predictive margins, and confidence intervals.
- [x] Add generated Python validation.
- [x] Add generated R validation.
- [x] Add fallback model behavior for transient failures.
- [x] Add SAS analysis endpoint for interpretation and validation checks.

## Refinement and Conversation

- [x] Add enhancement/refinement workflow.
- [x] Store enhancement history.
- [x] Highlight changed lines after refinement.
- [x] Add conversation API for asking about a conversion.
- [x] Store conversation threads and messages.
- [x] Allow assistant suggestions to feed back into refinement workflow.

## Persistence and History

- [x] Store conversions.
- [x] Store reviews.
- [x] Store execution runs.
- [x] Store enhancements.
- [x] Store conversations.
- [x] Add recent conversions panel.
- [x] Add full history page.
- [x] Add search over saved conversions.
- [x] Add project APIs.
- [x] Add project assignment support.

## Code Execution

- [x] Add execution API.
- [x] Support Python execution.
- [x] Support R execution.
- [x] Add Databricks Jobs backend.
- [x] Add Docker backend.
- [x] Add asynchronous Databricks polling with signed execution token.
- [x] Capture stdout, stderr, exit code, timeout, duration, packages, policy mode, images, and artifacts.
- [x] Persist execution results.
- [x] Support uploaded input files.
- [x] Set `SAS2PY_INPUT_DIR` for translated code.
- [x] Add package blocklist/allowlist enforcement.
- [x] Add output truncation and artifact limits.

## File Handoff and Artifacts

- [x] Add Azure Blob upload support.
- [x] Support large Databricks payload offload.
- [x] Support execution input file handoff.
- [x] Support artifact materialization and download URLs.
- [x] Support managed-identity/direct blob access pattern where configured.

## Documentation

- [x] Add README.
- [x] Add user manual under `public/docs`.
- [x] Add generated documentation PDF/DOCX.
- [x] Add intro PowerPoint under `public/docs`.
- [x] Add `SPEC.md`.
- [x] Add `ARCHITECTURE.md`.
- [x] Add `TASKS.md`.

## Recommended Next Tasks

- [ ] Add automated tests for core API routes.
- [ ] Add regression tests using examples in the `examples` folder.
- [ ] Add a small prompt-regression harness for known SAS/SUDAAN cases.
- [ ] Add a deployment checklist for Azure Web App + Databricks + Blob Storage.
- [ ] Add admin documentation for Databricks table creation and migrations.
- [ ] Add role-based project sharing if multiple users need to collaborate on the same conversion.
- [ ] Add exportable validation reports comparing SAS intent, generated code, execution output, and reviewer notes.
- [ ] Add health checks for Azure OpenAI, Databricks SQL, Databricks Jobs, and Blob Storage.
- [ ] Add CI checks for lint/build and selected route tests.
- [ ] Add stricter production error handling and audit logging.

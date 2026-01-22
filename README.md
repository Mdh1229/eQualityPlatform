# eQualityPlatform (Quality Tier Classifier)

## Overview (What this tool does)

**eQualityPlatform** is a workflow-driven web app for turning raw performance exports (CSV) into **actionable traffic-quality decisions** at the `sub_id` (and related) level. It helps you **audit**, **classify**, **compare against targets**, and **recommend next actions** (e.g., promote/demote/maintain/review/pause) while preserving the reasoning and outcomes of each run for later analysis.

### Key capabilities
- **CSV → Decisions pipeline**: upload a CSV, map columns, run classification, and review results in a single guided flow.
- **Target-based tiering**: compares traffic performance metrics against **quality targets** (thresholds/benchmarks) to produce a recommended tier/state.
- **Explainable recommendations**: outputs the recommended action along with threshold context (why something is promoted/demoted/flagged for review).
- **Interactive results dashboard**: filter, inspect, and visualize results at scale (charts/tables and drill-down style analysis supported by dependencies).
- **Persisted run history**: stores each analysis run and its per-row classification outputs in **PostgreSQL** via **Prisma**, enabling repeatability and historical comparison.
- **Action tracking**: records actions taken over time (who did what, when, and why) to support governance and accountability.
- **Operational tooling support**: includes utilities such as a **SQL generator** for translating selections/results into query snippets for downstream workflows.

### Approach
- **App Router (Next.js)** UI flow with a client-heavy classifier experience (the primary classifier UI is loaded with SSR disabled to support browser-only parsing/visualization).
- **Separation of concerns**: UI in `components/`, domain logic in `lib/`, persistence in `prisma/`.
- **Configurable UI system** using Tailwind + shadcn-style components for consistent, composable interfaces.

---

A Next.js (App Router) web application for auditing and classifying traffic sources (e.g., `sub_id`s) against quality targets, generating **reclassification recommendations** (promote/demote/maintain/review), and tracking actions/results over time.

This repository contains the UI workflow (upload → mapping → classification → dashboard), the classification/analytics logic, and a PostgreSQL/Prisma data model to persist runs and outcomes.

---

## Tech Stack

- **Next.js** (App Router) + **React** + **TypeScript**
- **Tailwind CSS** (+ `tailwindcss-animate`) and a component system configured via **shadcn/ui** (`components.json`)
- **Prisma** ORM + **PostgreSQL**
- Client-side data utilities for CSV parsing/analysis and visualizations (Chart.js / Plotly / Recharts present in dependencies)

---

## Repository Structure

### Top level
- `app/` — Next.js App Router pages, layout, and API routes
- `components/` — application UI components + `components/ui/` (shadcn-style UI primitives)
- `hooks/` — reusable React hooks (e.g., toast utilities)
- `lib/` — core business logic (classification engine, analytics, SQL generator, theme config, shared types)
- `prisma/` — Prisma schema (database models)
- `public/` — static assets (favicon, OG image) and sample CSV (`example_data.csv`)

### Notable files
- `app/page.tsx` — loads the main classifier UI (`ClassifierClient`) via `next/dynamic` with `ssr:false`
- `app/layout.tsx` — global layout, theme provider, and metadata/OpenGraph config
- `prisma/schema.prisma` — database schema for analysis runs, classification results, and action history
- `package.json` — scripts and dependencies
- `next.config.js` — Next.js build/runtime configuration
- `tailwind.config.ts` — Tailwind theme + content paths

---

## How the App Works (High Level)

1. **Upload** a CSV (or start from sample data).
2. **Map** CSV columns to expected fields (e.g., `subId`, `vertical`, `trafficType`, metrics like calls/leads/clicks/revenue).
3. Run the **classification engine** to compare each row against **quality targets** and produce:
   - Recommended tier
   - Action label (promote/demote/maintain/review/below-min/etc.)
   - Reasoning/threshold context
4. Review results in the **dashboard** (tables, charts, filters).
5. Persist results and user actions into the database for history and reporting.

---

## Database (Prisma)

The Prisma schema defines:

- **`AnalysisRun`**
  - One record per run/upload (date range, file name, counts, timestamps)
  - Has many `ClassificationResult`

- **`ClassificationResult`**
  - Stores the per-`subId` output of the classification (recommended tier, action, metrics, reason)
  - Indexed by `runId`, `action`, `vertical`

- **`ActionHistory`**
  - Tracks actions taken over time (promote/demote/pause/maintain/review) with optional notes and metrics
  - Indexed for filtering by subId, actionTaken, createdAt, etc.

Schema file: `prisma/schema.prisma`.

---

## Getting Started (Local Development)

### 1) Install dependencies
This project uses Node + Yarn.

```bash
yarn install
```

### 2) Configure environment variables
There is a `.env` file in the repo root. At minimum you will need a PostgreSQL connection string:

- `DATABASE_URL=postgresql://...`

If you’re using authentication/features that rely on NextAuth, you may also need:

- `NEXTAUTH_URL=http://localhost:3000`
- (and any other NextAuth secrets/providers if added)

> Note: The app’s metadata uses `NEXTAUTH_URL` as the base URL fallback.

### 3) Set up the database
Generate Prisma client and apply migrations (depending on your workflow):

```bash
yarn prisma generate
yarn prisma migrate dev
```

(Optional) Seed if configured:

```bash
yarn prisma db seed
```

### 4) Run the dev server
```bash
yarn dev
```

Then open: http://localhost:3000

---

## Scripts

From `package.json`:

- `yarn dev` — run Next.js dev server
- `yarn build` — production build
- `yarn start` — run built app
- `yarn lint` — lint

---

## UI / Components

- Main interactive flow is implemented in `components/classifier-client.tsx` and related step components:
  - `components/upload-step.tsx`
  - `components/mapping-step.tsx`
  - `components/results-dashboard.tsx`
  - `components/sql-modal.tsx`
- Theme support is provided via:
  - `components/theme-context.tsx`
  - `components/theme-provider.tsx`
  - `lib/theme-config.ts`

---

## Core Logic

Located in `lib/`:

- `lib/classification-engine.ts` — classification rules/engine
- `lib/ml-analytics.ts` — analytics/quality computations (metrics + derived insights)
- `lib/quality-targets.ts` — target thresholds/benchmarks
- `lib/sql-generator.ts` — builds SQL snippets/queries from selections/results (used by UI modal)
- `lib/db.ts` — Prisma client / DB helper
- `lib/types.ts` — shared types
- `lib/utils.ts` — utility helpers

---

## Sample Data

A small example CSV is available at:

- `public/example_data.csv`

You can use it to validate the upload/mapping/classification flow.

---

## Notes / Caveats

- The home page intentionally loads the main classifier client with **SSR disabled** (`ssr:false`) to keep CSV parsing/visualization and browser-only dependencies on the client.
- `next.config.js` sets `images: { unoptimized: true }`, which can be useful for static hosting scenarios.

---

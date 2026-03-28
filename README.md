# eQualityPlatform (Quality Compass)

## Overview (What this tool does)

**eQualityPlatform** is a workflow-driven web app for turning raw performance exports (CSV) or A/B/C aggregated feeds into **actionable traffic-quality decisions** at the `sub_id` (and related) level. It helps you **audit**, **classify**, **compare against targets**, and **recommend next actions** (e.g., promote/demote/maintain/review/pause) while preserving the reasoning and outcomes of each run for later analysis.

The **Quality Compass** transformation extends the original CSV-to-decisions workflow with:
- **Supabase PostgreSQL** as the system-of-record for A/B/C daily aggregated facts
- **FastAPI backend** service layer for compute pipelines, insights, and trend analysis
- **"WOW" Insights** including change-point detection, driver analysis, and buyer salvage simulations
- **Performance History** tab with time series visualization and anomaly detection
- **Daily automation** via Google Drive memos and Slack digest notifications

> **IMPORTANT**: The system only recommends actions; humans confirm via Log Action. No autonomous pausing, routing, or bidding is performed.

### Key capabilities
- **CSV → Decisions pipeline**: upload a CSV, map columns, run classification, and review results in a single guided flow.
- **A/B/C Feed Ingestion**: ingest daily aggregated feeds from BigQuery tables or CSV uploads with identical schema.
- **Target-based tiering**: compares traffic performance metrics against **quality targets** (thresholds/benchmarks) to produce a recommended tier/state.
- **Explainable recommendations**: outputs the recommended action along with threshold context (why something is promoted/demoted/flagged for review).
- **Interactive results dashboard**: filter, inspect, and visualize results at scale (charts/tables and drill-down style analysis supported by dependencies).
- **8-Tab Expanded Row**: Summary, Explain, Drivers, Buyer/Path to Life, Performance History, History, Notes, Log Action.
- **Smart Insights**: Z-score anomaly detection, behavioral clustering, priority scoring, and portfolio health metrics.
- **WOW Insights**: Change-point detection (CUSUM), driver decomposition (mix vs performance), buyer salvage simulations.
- **Macro Insights**: Clustering across dimensions (buyer, domain, keyword bucket) using MiniBatchKMeans.
- **Persisted run history**: stores each analysis run and its per-row classification outputs in **PostgreSQL** via **Prisma**, enabling repeatability and historical comparison.
- **Action tracking**: records actions taken over time (who did what, when, and why) with outcome tracking via difference-in-differences analysis.
- **Operational tooling support**: includes utilities such as a **SQL generator** for translating selections/results into query snippets for downstream workflows.
- **Daily automation**: Google Drive memo generation and Slack digest notifications per vertical/date.

### Approach
- **App Router (Next.js)** UI flow with a client-heavy classifier experience (the primary classifier UI is loaded with SSR disabled to support browser-only parsing/visualization).
- **FastAPI Backend** for compute-heavy analytics, Python-based insights, and trend analysis.
- **Separation of concerns**: UI in `components/`, frontend logic in `lib/`, Python services in `backend/services/`, persistence via Prisma and Supabase.
- **Configurable UI system** using Tailwind + shadcn-style components for consistent, composable interfaces.

---

A Next.js (App Router) web application backed by a FastAPI service layer for auditing and classifying traffic sources (e.g., `sub_id`s) against quality targets, generating **reclassification recommendations** (promote/demote/maintain/review), and tracking actions/results over time.

This repository contains the UI workflow (upload → mapping → classification → dashboard), the classification/analytics logic, a PostgreSQL/Prisma data model to persist runs and outcomes, and a Python backend for advanced analytics.

---

## Tech Stack

### Frontend
- **Next.js 14.2.28** (App Router) + **React 18.2.0** + **TypeScript 5.2.2**
- **Tailwind CSS 3.3.3** (+ `tailwindcss-animate`) and a component system configured via **shadcn/ui** (`components.json`)
- **Prisma 6.7.0** ORM + **PostgreSQL** (Supabase)
- Client-side data utilities for CSV parsing/analysis and visualizations (Recharts 2.15.0)

### Backend (FastAPI)
- **Python 3.11.8** runtime
- **FastAPI 0.115.6** + **Uvicorn 0.30.6** ASGI server
- **Pydantic 2.10.4** for data validation
- **Pandas 2.2.3** + **NumPy 2.1.3** for data manipulation
- **scikit-learn 1.5.2** for MiniBatchKMeans clustering
- **Google Cloud BigQuery 3.29.0** for direct table feeds
- **Google API Python Client 2.156.0** for Google Drive integration
- **Slack SDK 3.33.5** for Slack notifications
- **asyncpg 0.30.0** for async PostgreSQL operations

---

## Repository Structure

### Top level
- `app/` — Next.js App Router pages, layout, and API routes (proxy to FastAPI)
- `backend/` — FastAPI Python backend service layer
- `components/` — application UI components + `components/ui/` (shadcn-style UI primitives)
- `hooks/` — reusable React hooks (e.g., toast utilities)
- `lib/` — core frontend business logic (classification engine, analytics, SQL generator, theme config, shared types)
- `prisma/` — Prisma schema (database models)
- `sql/` — SQL artifacts (migrations and BigQuery templates)
- `public/` — static assets (favicon, OG image) and sample CSV (`example_data.csv`)

### Backend structure (`backend/`)
```
backend/
├── main.py                    # FastAPI app entry point with CORS and routers
├── requirements.txt           # Production Python dependencies (exact versions)
├── requirements-dev.txt       # Development/test dependencies
├── api/                       # API endpoint handlers
│   ├── runs.py               # POST/GET /runs endpoints
│   ├── actions.py            # POST/GET /actions with outcome tracking
│   ├── insights.py           # Smart + WOW insights endpoints
│   ├── performance_history.py # Trend series endpoints
│   └── macro_insights.py     # Macro clustering endpoints
├── core/                      # Core infrastructure
│   ├── config.py             # Settings via pydantic-settings
│   ├── database.py           # Async Postgres connection
│   └── dependencies.py       # FastAPI DI dependencies
├── models/                    # Data models
│   ├── schemas.py            # Pydantic request/response models
│   └── enums.py              # tx_family_enum, action_type_enum
├── services/                  # Business logic services
│   ├── ingestion.py          # A/B/C CSV + BigQuery ingestion
│   ├── rollup.py             # Windowed rollup computation
│   ├── classification.py     # Classification engine (ported from TypeScript)
│   ├── driver_analysis.py    # Mix shift vs true degradation
│   ├── buyer_salvage.py      # Path to Life simulations
│   ├── change_point.py       # CUSUM change-point detection
│   ├── smart_insights.py     # Smart Insights (ported from ml-analytics.ts)
│   ├── macro_clustering.py   # MiniBatchKMeans clustering
│   └── outcome_tracking.py   # Diff-in-diff analysis
├── jobs/                      # Daily automation jobs
│   ├── daily_memo.py         # Google Drive memo generation
│   └── slack_digest.py       # Slack daily digest
├── sql/                       # Parameterized SQL queries
│   ├── rollup_queries.py     # Windowed rollup SQL
│   ├── driver_queries.py     # Driver decomposition SQL
│   └── trend_queries.py      # Performance History SQL
└── tests/                     # Python test suite
    ├── conftest.py           # Pytest fixtures
    ├── test_ingestion.py     # A/B/C feed tests
    ├── test_classification.py # Classification parity tests
    ├── test_insights.py      # Insights accuracy tests
    ├── test_change_point.py  # Change-point detection tests
    └── test_jobs.py          # Daily job idempotency tests
```

### Notable files
- `app/page.tsx` — loads the main classifier UI (`ClassifierClient`) via `next/dynamic` with `ssr:false`
- `app/layout.tsx` — global layout, theme provider, and metadata/OpenGraph config
- `prisma/schema.prisma` — database schema for analysis runs, classification results, action history, and fact tables
- `next.config.mjs` — Next.js build/runtime configuration with FastAPI proxy rewrites
- `backend/main.py` — FastAPI application entry point
- `package.json` — scripts and frontend dependencies
- `tailwind.config.ts` — Tailwind theme + content paths

---

## How the App Works (High Level)

### Original CSV Flow (Preserved)
1. **Upload** a CSV (or start from sample data).
2. **Map** CSV columns to expected fields (e.g., `subId`, `vertical`, `trafficType`, metrics like calls/leads/clicks/revenue).
3. Run the **classification engine** to compare each row against **quality targets** and produce:
   - Recommended tier
   - Action label (promote/demote/maintain/review/below-min/etc.)
   - Reasoning/threshold context
4. Review results in the **dashboard** (tables, charts, filters).
5. Persist results and user actions into the database for history and reporting.

### A/B/C Feed Flow (New)
1. **Ingest** A/B/C feeds from BigQuery tables or CSV uploads:
   - Feed A: `fact_subid_day` (sub_id daily aggregates)
   - Feed B: `fact_subid_slice_day` (slice-level data)
   - Feed C: `fact_subid_buyer_day` (buyer-level data)
2. **Compute** windowed rollups via FastAPI backend.
3. **Classify** each sub_id using the preserved classification rules.
4. **Generate insights**:
   - Smart Insights (anomalies, clusters, portfolio health)
   - WOW Insights (change-points, driver decomposition, buyer salvage)
   - Macro Insights (cross-dimension clustering)
5. **Review** in enhanced dashboard with 8-tab expanded rows.
6. **Log Action** via modal confirmation (human-in-the-loop).
7. **Track outcomes** via difference-in-differences analysis.

### API Architecture
- Next.js API routes proxy requests to FastAPI backend
- Frontend contracts remain unchanged for backward compatibility
- FastAPI handles compute-heavy operations and Python-based analytics

---

## Backend Services (FastAPI)

### Overview
The FastAPI backend provides compute pipelines, Python-based analytics, and trend analysis capabilities. It runs as a separate service and is proxied through Next.js API routes to maintain frontend compatibility.

### Runtime Requirements
- **Python 3.11.8** (exact version required)
- Virtual environment recommended for isolation

### Setup Instructions

#### 1) Create and activate virtual environment
```bash
cd backend
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

#### 2) Install dependencies
```bash
pip install -r requirements.txt
```

For development/testing:
```bash
pip install -r requirements-dev.txt
```

#### 3) Start the FastAPI server
```bash
uvicorn backend.main:app --reload --port 8000
```

The API will be available at `http://localhost:8000`.

### API Documentation
When running, access:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Key Endpoints
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/runs` | POST | Create new analysis run |
| `/runs` | GET | List analysis runs |
| `/runs/{id}` | GET | Get run details |
| `/runs/{id}/compute` | POST | Execute classification compute |
| `/runs/{id}/subid/{subid}/detail` | GET | Full detail bundle |
| `/runs/{id}/subid/{subid}/performance-history` | GET | Trend series data |
| `/actions` | POST | Log new action |
| `/actions` | GET | List action history |
| `/insights/smart` | GET | Smart Insights results |
| `/insights/wow` | GET | WOW Insights (change-points, drivers, salvage) |
| `/macro-insights` | GET | Macro clustering results |

---

## Daily Jobs

The backend includes automated daily jobs for generating reports and notifications.

### Google Drive Daily Memo (`backend/jobs/daily_memo.py`)

Generates a daily memo document summarizing:
- Classification results by vertical
- Key metric movements
- Top recommendations requiring action
- Portfolio health summary

**Usage**:
```bash
python -m backend.jobs.daily_memo --vertical Medicare --date 2024-01-15
```

**Configuration**: Requires `GOOGLE_APPLICATION_CREDENTIALS` and `GOOGLE_DRIVE_FOLDER_ID` environment variables.

### Slack Daily Digest (`backend/jobs/slack_digest.py`)

Posts a daily summary to Slack including:
- Count of sub_ids by tier (Premium/Standard/Pause)
- High-priority recommendations
- Notable anomalies and change-points

**Usage**:
```bash
python -m backend.jobs.slack_digest --date 2024-01-15
```

**Configuration**: Requires `SLACK_WEBHOOK_URL` environment variable.

### Idempotency Behavior

Both daily jobs implement idempotency to prevent duplicate outputs:
- **Google Drive**: Checks for existing memo with same vertical/date before creation
- **Slack**: Tracks last successful digest per date to prevent re-posting

State is persisted in the database to ensure reliability across restarts.

---

## Environment Variables

All required environment variables for the Quality Compass system:

### Database (Required)
```bash
# Supabase PostgreSQL connection string
DATABASE_URL=postgresql://postgres:[password]@[host]:[port]/postgres
```

### FastAPI Backend (Required)
```bash
# FastAPI backend URL (used by Next.js proxy)
FASTAPI_URL=http://localhost:8000
```

### Google Cloud Platform (For BigQuery feeds)
```bash
# Path to GCP service account JSON file
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# BigQuery project ID for A/B/C feeds
BIGQUERY_PROJECT=dwh-production-352519
```

### Google Drive (For daily memos)
```bash
# Google Drive folder ID where daily memos are stored
GOOGLE_DRIVE_FOLDER_ID=your-folder-id
```

### Slack (For daily digests)
```bash
# Slack webhook URL for posting daily digests
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/xxx/xxx
```

### AI Integration (For AI insights)
```bash
# Abacus AI API key for AI-generated summaries
ABACUS_API_KEY=your-api-key
```

### Optional Configuration
```bash
# Next.js base URL (for metadata)
NEXTAUTH_URL=http://localhost:3000
```

### Example `.env` file
```bash
# Database
DATABASE_URL=postgresql://postgres:password@db.example.supabase.co:5432/postgres

# FastAPI
FASTAPI_URL=http://localhost:8000

# Google Cloud
GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/service-account.json
BIGQUERY_PROJECT=dwh-production-352519

# Google Drive
GOOGLE_DRIVE_FOLDER_ID=1ABC123xyz

# Slack
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00/B00/xxxx

# AI
ABACUS_API_KEY=ak_xxxxx
```

---

## A/B/C Feed Ingestion

The Quality Compass system supports three complementary data feeds that provide the foundation for classification and insights.

### Feed A: `fact_subid_day`

**Grain**: `date_et` + `vertical` + `traffic_type` + `tier` + `subid`

**Purpose**: Daily aggregated metrics at the sub_id level.

**Required Columns**:
- `date_et` — Date in Eastern Time
- `vertical` — Business vertical (Medicare, Health, Life, Auto, Home)
- `traffic_type` — Traffic classification (Full O&O, Partial O&O, Non O&O)
- `tier` — Current tier assignment
- `subid` — Sub ID identifier
- `calls`, `paid_calls`, `qual_paid_calls` — Call metrics
- `transfer_count`, `leads` — Lead metrics
- `clicks`, `redirects` — Click/redirect metrics
- `call_rev`, `lead_rev`, `click_rev`, `redirect_rev`, `rev` — Revenue metrics

### Feed B: `fact_subid_slice_day`

**Grain**: `date_et` + `vertical` + `traffic_type` + `tier` + `subid` + `tx_family` + `slice_name` + `slice_value`

**Purpose**: Slice-level data for driver analysis and mix decomposition.

**Additional Columns**:
- `tx_family` — Transaction family (Call, Lead, Click, Redirect)
- `slice_name` — Dimension name (e.g., age_bucket, keyword)
- `slice_value` — Dimension value
- `fill_rate_by_rev` — Data coverage metric

**Slice Value Limits**:
- Top 50 `slice_value` per (`date_et`, `subid`, `tx_family`, `slice_name`) by `rev` DESC
- Smart Unspecified: exclude `slice_value='Unspecified'` when `fill_rate_by_rev >= 0.90`

### Feed C: `fact_subid_buyer_day`

**Grain**: `date_et` + `vertical` + `traffic_type` + `tier` + `subid` + `buyer_key_variant` + `buyer_key`

**Purpose**: Buyer-level metrics for buyer sensitivity and salvage analysis.

**Additional Columns**:
- `buyer_key_variant` — Type of buyer key (carrier_name, concatenated variants)
- `buyer_key` — Buyer identifier

### Ingestion Options

#### Option 1: BigQuery Tables
Configure BigQuery credentials and project, then use the `/runs` endpoint with `source_type: "bigquery"`:
```json
{
  "source_type": "bigquery",
  "date_from": "2024-01-01",
  "date_to": "2024-01-15",
  "verticals": ["Medicare", "Health"]
}
```

#### Option 2: CSV Upload
Upload CSV files matching the Feed A/B/C schemas via the UI or API:
```json
{
  "source_type": "csv",
  "feed_a_data": [...],
  "feed_b_data": [...],
  "feed_c_data": [...]
}
```

Both options produce identical downstream behavior.

---

## SQL Artifacts

SQL artifacts for database schema management and BigQuery feed generation.

### Schema Migrations (`sql/migrations/`)

Supabase PostgreSQL schema migrations in sequential order:

| File | Purpose |
|------|---------|
| `001_create_enums.sql` | Create `tx_family_enum` and `action_type_enum` |
| `002_create_fact_tables.sql` | Create `fact_subid_day`, `fact_subid_slice_day`, `fact_subid_buyer_day` |
| `003_create_config_tables.sql` | Create `config_quality_thresholds`, `config_platform` |
| `004_create_run_tables.sql` | Create `analysis_run`, `rollup_subid_window` |
| `005_create_output_tables.sql` | Create `classification_result`, `action_history` |
| `006_create_insight_tables.sql` | Create `insight_change_point`, `insight_driver_summary`, `insight_buyer_salvage`, `insight_action_outcome` |

**Running Migrations**:
```bash
# Using psql
psql $DATABASE_URL -f sql/migrations/001_create_enums.sql
psql $DATABASE_URL -f sql/migrations/002_create_fact_tables.sql
# ... continue for all migration files
```

### BigQuery Templates (`sql/bigquery/`)

Templates for generating A/B/C feeds from BigQuery:

| File | Purpose |
|------|---------|
| `feed_a_subid_day.sql` | Template for Feed A (daily sub_id aggregates) |
| `feed_b_slice_day.sql` | Template for Feed B (slice-level data) |
| `feed_c_buyer_day.sql` | Template for Feed C (buyer-level data) |
| `trend_series.sql` | Template for Performance History extraction |

**Usage with SQL Generator**:
The UI's SQL tab generates ready-to-run BigQuery queries using these templates with parameterized date ranges and vertical filters.

---

## Development Setup

Unified instructions for setting up both frontend (Next.js) and backend (FastAPI) development environments.

### Prerequisites
- **Node.js 20.x** + **Yarn 1.22.x**
- **Python 3.11.8**
- **PostgreSQL** (Supabase or local instance)

### Step 1: Clone and Install Frontend
```bash
git clone <repository-url>
cd eQualityPlatform

# Install Node dependencies
yarn install

# Generate Prisma client
yarn prisma generate
```

### Step 2: Set Up Backend
```bash
cd backend

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Step 3: Configure Environment
Create a `.env` file in the project root with required variables:
```bash
# Minimum required
DATABASE_URL=postgresql://...
FASTAPI_URL=http://localhost:8000

# For full functionality (optional for local dev)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
BIGQUERY_PROJECT=your-project-id
GOOGLE_DRIVE_FOLDER_ID=your-folder-id
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
ABACUS_API_KEY=your-api-key
```

### Step 4: Initialize Database
```bash
# Apply Prisma migrations
yarn prisma migrate dev

# (Optional) Apply SQL migrations for fact tables
psql $DATABASE_URL -f sql/migrations/001_create_enums.sql
# ... continue for all migration files
```

### Step 5: Start Development Servers

**Terminal 1 — FastAPI Backend**:
```bash
cd backend
source venv/bin/activate
uvicorn backend.main:app --reload --port 8000
```

**Terminal 2 — Next.js Frontend**:
```bash
yarn dev
```

### Access Points
- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs

### Running Tests

**Frontend (if applicable)**:
```bash
yarn lint
```

**Backend**:
```bash
cd backend
source venv/bin/activate
pytest -v --tb=short
```

---

## Database (Prisma)

The Prisma schema defines the core models that coexist with Supabase fact tables:

- **`AnalysisRun`**
  - One record per run/upload (date range, file name, counts, timestamps)
  - Has many `ClassificationResult`

- **`ClassificationResult`**
  - Stores the per-`subId` output of the classification (recommended tier, action, metrics, reason)
  - Indexed by `runId`, `action`, `vertical`

- **`ActionHistory`**
  - Tracks actions taken over time (promote/demote/pause/maintain/review) with optional notes and metrics
  - Enhanced with outcome tracking (quality delta, revenue impact)
  - Indexed for filtering by subId, actionTaken, createdAt, etc.

Schema file: `prisma/schema.prisma`.

### Supabase Fact Tables
The following tables are managed via SQL migrations (not Prisma):
- `fact_subid_day` — Feed A daily aggregates
- `fact_subid_slice_day` — Feed B slice data
- `fact_subid_buyer_day` — Feed C buyer data
- `config_quality_thresholds` — Locked quality thresholds
- `config_platform` — Editable platform parameters
- `insight_*` tables — WOW insights storage

---

## Scripts

From `package.json`:

- `yarn dev` — run Next.js dev server (port 3000)
- `yarn build` — production build
- `yarn start` — run built app
- `yarn lint` — lint

Backend scripts (from `backend/` directory):
- `uvicorn backend.main:app --reload --port 8000` — run FastAPI dev server
- `pytest -v` — run Python tests
- `python -m backend.jobs.daily_memo` — run daily memo job
- `python -m backend.jobs.slack_digest` — run Slack digest job

---

## UI / Components

- Main interactive flow is implemented in `components/classifier-client.tsx` and related step components:
  - `components/csv-upload-component.tsx` — CSV upload with A/B/C schema validation
  - `components/column-mapper-component.tsx` — Column mapping for A/B/C feeds
  - `components/results-dashboard.tsx` — Results table with 8-tab expanded rows
- New tab components:
  - `components/performance-history-tab.tsx` — Time series visualization
  - `components/driver-analysis-tab.tsx` — Mix vs performance decomposition
  - `components/buyer-salvage-tab.tsx` — Buyer metrics and Path to Life simulations
  - `components/explain-tab.tsx` — Audit packet visualization
  - `components/log-action-modal.tsx` — Action confirmation dialog
- Theme support is provided via:
  - `components/theme-context.tsx`
  - `components/theme-provider.tsx`
  - `lib/theme-config.ts`

---

## Core Logic

Located in `lib/`:

- `lib/classification-engine.ts` — classification rules/engine (preserved "2026 Rules")
- `lib/ml-analytics.ts` — Smart Insights (z-score anomalies, behavioral clusters, priority scoring, portfolio health)
- `lib/quality-targets.ts` — target thresholds/benchmarks per vertical
- `lib/sql-generator.ts` — builds SQL snippets/queries including A/B/C feed templates
- `lib/db.ts` — Prisma client / DB helper
- `lib/api-client.ts` — FastAPI proxy helper for frontend-backend communication
- `lib/types.ts` — shared types including PerformanceHistoryData, DriverAnalysis, BuyerSalvage
- `lib/utils.ts` — utility helpers

Python services in `backend/services/`:
- `classification.py` — Python port of classification engine (produces identical results)
- `smart_insights.py` — Python port of Smart Insights
- `change_point.py` — CUSUM change-point detection
- `driver_analysis.py` — Mix shift vs true degradation decomposition
- `buyer_salvage.py` — Path to Life salvage simulations
- `macro_clustering.py` — MiniBatchKMeans clustering across dimensions

---

## Sample Data

A small example CSV is available at:

- `public/example_data.csv`

You can use it to validate the upload/mapping/classification flow.

---

## Notes / Caveats

- The home page intentionally loads the main classifier client with **SSR disabled** (`ssr:false`) to keep CSV parsing/visualization and browser-only dependencies on the client.
- `next.config.mjs` sets `images: { unoptimized: true }` and includes FastAPI proxy rewrites.
- All cohort comparisons and driver analysis are scoped to `vertical + traffic_type`.
- Classification logic is preserved exactly from the original "2026 Rules" to ensure backward compatibility.
- The system only recommends actions — no autonomous pausing, routing, or bidding is performed.
- Performance History tab loads lazily on row expand to avoid slowing the main table.

---

## Getting Started (Quick Reference)

### Minimal Setup (Frontend Only)
```bash
yarn install
yarn prisma generate
yarn prisma migrate dev
yarn dev
```

### Full Setup (Frontend + Backend)
```bash
# Frontend
yarn install
yarn prisma generate
yarn prisma migrate dev

# Backend (separate terminal)
cd backend
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn backend.main:app --reload --port 8000

# Frontend (separate terminal)
yarn dev
```

Then open: http://localhost:3000

---

# Technical Specification

# 0. Agent Action Plan

## 0.1 Intent Clarification

Based on the prompt, the Blitzy platform understands that the refactoring objective is to transform the existing "eQualityPlatform" (Quality Tier Classifier) from a client-side CSV-to-decisions workflow into a comprehensive **Quality Compass** system with Supabase PostgreSQL as the system-of-record, backed by A/B/C daily aggregated feeds from BigQuery or CSV uploads, while maintaining all existing classification logic, Smart Insights, and UI conventions.

### 0.1.1 Core Refactoring Objective

The transformation involves:

- **Refactoring Type**: Tech stack migration + Architecture expansion + Feature enhancement
- **Target Repository**: Same repository (in-place refactor)
- **Primary Goals**:
  - Migrate from ephemeral CSV-based analysis to persistent Supabase PostgreSQL facts tables
  - Add FastAPI backend service layer for compute pipelines and insights
  - Implement A/B/C feed ingestion (BigQuery tables OR CSV uploads with identical schema)
  - Add "WOW" insights layer (change-point detection, driver analysis, buyer salvage, etc.)
  - Implement Performance History tab with time series visualization
  - Add Daily Google Drive memo and Slack digest capabilities
  - Maintain 100% backward compatibility with existing UI contracts and API endpoints

### 0.1.2 Special Instructions and Constraints

**CRITICAL DIRECTIVES**:

- **No autonomous pausing/routing/bidding** — system only recommends; humans confirm via Log Action
- **System-of-record**: Supabase Postgres facts fed by A/B/C daily aggregated feeds (not raw event rows in-app)
- **All cohort comparisons and driver analysis must be scoped to vertical + traffic_type**
- **Preserve existing behavior exactly** unless explicitly changed in the specification
- **Minimal change clause**: Make only changes necessary for this refactor; avoid unrelated refactors
- **Version lock**: No substitutions on runtime versions or dependencies

**Migration Requirements**:

- Keep Next.js App Router frontend and existing API routes for compatibility
- Next.js API routes proxy to FastAPI so frontend contracts remain unchanged
- Add new endpoints only where required (Performance History, Macro Insights, Runs/Compute, Detail bundle)

**Technology Stack Lock** (User Example — EXACT versions required):

```
Runtime:
- Python 3.11.8
- Node.js 20.x

Frontend (no changes):
- React 18.2.0
- Next.js 14.2.28 (App Router)
- TypeScript 5.2.2
- Tailwind CSS 3.3.3
- Prisma 6.7.0

Backend Python deps:
- fastapi==0.115.6
- uvicorn[standard]==0.30.6
- pydantic==2.10.4
- httpx==0.27.2
- tenacity==9.0.0
- python-dateutil==2.9.0.post0
- numpy==2.1.3
- pandas==2.2.3
- scikit-learn==1.5.2
- google-cloud-bigquery==3.29.0
- google-auth==2.37.0
- google-api-python-client==2.156.0
- slack-sdk==3.33.5

Test deps:
- pytest==8.3.4
- pytest-asyncio==0.25.0
```

### 0.1.3 Technical Interpretation

This refactoring translates to the following technical transformation strategy:

| Current State | Target State |
|---------------|--------------|
| CSV upload → in-memory classification | A/B/C feeds → Supabase facts → FastAPI compute → classification |
| Client-side analytics (ml-analytics.ts) | Python-based analytics with Smart + WOW + Macro insights |
| Single analysis run | Windowed rollups + trend analysis + cohort benchmarking |
| Action history in Prisma | Enhanced action_history with outcome tracking |
| Static SQL generator | Parameterized SQL for A/B/C feeds + trend extraction |
| No daily reporting | Google Drive memo + Slack digest per vertical/day |
| Basic expanded row (Summary/History/Notes) | 8-tab expanded row (Summary/Explain/Drivers/Buyer/Perf History/History/Notes/Log Action) |

### 0.1.4 Implicit Requirements Surfaced

- **Maintain all public API contracts**: Existing `/api/classify`, `/api/runs`, `/api/actions`, `/api/sql`, `/api/ai-insights` must continue working
- **Preserve test coverage**: Classification parity tests ensure existing logic is maintained
- **Database coexistence**: Prisma models must coexist with new Supabase facts schema
- **Idempotency**: Daily jobs must not duplicate Drive memos or Slack digests
- **Performance**: Performance History tab must load lazily; must not slow main table

## 0.2 Source Analysis

### 0.2.1 Comprehensive Source File Discovery

The following patterns identify ALL files requiring transformation based on the user's instructions:

**Core Application Files** (existing logic to preserve + extend):
```
app/
├── page.tsx                    # Main entry (dynamic import of ClassifierClient)
├── layout.tsx                  # Root layout with providers
├── globals.css                 # Global styles
├── api/
│   ├── classify/route.ts       # Core classification endpoint (extend for A/B/C)
│   ├── runs/route.ts           # Analysis runs list (extend for new schema)
│   ├── runs/[id]/route.ts      # Run details (extend for detail bundle)
│   ├── actions/route.ts        # Action history (extend for outcome tracking)
│   ├── sql/route.ts            # SQL generation (extend for A/B/C templates)
│   └── ai-insights/route.ts    # AI summaries (preserve as-is)
├── history/
│   └── page.tsx                # History page (update for new action schema)
├── settings/
│   └── page.tsx                # Settings page (add config_platform controls)
└── sql/
    └── page.tsx                # SQL tab (extend for A/B/C BigQuery templates)
```

**Library Files** (core logic to preserve + extend):
```
lib/
├── classification-engine.ts    # Deterministic rules (preserve exactly)
├── quality-targets.ts          # Threshold configs (map to config_quality_thresholds)
├── sql-generator.ts            # BigQuery SQL generator (extend for A/B/C)
├── theme-config.ts             # Brand colors (preserve)
├── ml-analytics.ts             # Smart Insights (preserve + port to Python)
├── db.ts                       # Prisma singleton (preserve)
├── utils.ts                    # Utility functions (preserve)
└── types.ts                    # Type definitions (extend)
```

**Component Files** (UI to extend):
```
components/
├── classifier-client.tsx       # Main orchestrator (add nav tabs, proxy to FastAPI)
├── results-dashboard.tsx       # Results table (add badges, expanded row tabs)
├── csv-upload-component.tsx    # CSV wizard (preserve for A/B/C ingestion)
├── column-mapper-component.tsx # Column mapping (extend for A/B/C schemas)
├── ui/                         # Shadcn components (preserve)
└── theme-provider.tsx          # Theme system (preserve)
```

**Database Schema** (Prisma to extend):
```
prisma/
├── schema.prisma               # Existing models (preserve + add new tables)
└── migrations/                 # Existing migrations (preserve + add new)
```

**Configuration Files**:
```
.
├── package.json                # Frontend deps (preserve versions)
├── tsconfig.json               # TypeScript config (preserve)
├── tailwind.config.ts          # Tailwind config (preserve)
├── next.config.mjs             # Next.js config (add proxy to FastAPI)
└── .env                        # Environment variables (extend)
```

### 0.2.2 Current Structure Mapping

```
Current Repository Structure:
eQualityPlatform/
├── app/                        # Next.js App Router pages and API routes
│   ├── api/                    # API endpoints (5 route files)
│   ├── history/                # History page
│   ├── settings/               # Settings page
│   ├── sql/                    # SQL page
│   ├── globals.css
│   ├── layout.tsx
│   └── page.tsx
├── components/                 # React components
│   ├── ui/                     # Shadcn UI primitives (15+ components)
│   ├── classifier-client.tsx   # Main client component (400+ lines)
│   ├── results-dashboard.tsx   # Results visualization (300+ lines)
│   ├── csv-upload-component.tsx
│   ├── column-mapper-component.tsx
│   └── theme-provider.tsx
├── lib/                        # Core business logic
│   ├── classification-engine.ts # Tier classification rules (400+ lines)
│   ├── quality-targets.ts      # Vertical thresholds (200+ lines)
│   ├── sql-generator.ts        # BigQuery SQL templates (300+ lines)
│   ├── ml-analytics.ts         # Smart Insights engine (500+ lines)
│   ├── db.ts                   # Prisma client
│   ├── theme-config.ts         # Brand theming
│   ├── types.ts                # Type definitions
│   └── utils.ts                # Utilities
├── prisma/
│   ├── schema.prisma           # Database models
│   └── migrations/
├── public/                     # Static assets
└── [config files]              # package.json, tsconfig.json, etc.
```

### 0.2.3 Source File Inventory by Transformation Requirement

| File Category | Files Count | Transformation Mode |
|---------------|-------------|---------------------|
| API Routes (extend) | 5 | UPDATE - Add FastAPI proxy + new endpoints |
| Library Core (preserve + port) | 7 | UPDATE - Maintain contracts, port logic to Python |
| React Components (extend UI) | 6 | UPDATE - Add tabs, badges, Performance History |
| Shadcn UI Primitives | 15+ | REFERENCE - Use patterns for new components |
| Prisma Schema | 1 | UPDATE - Add new fact tables + config tables |
| Configuration | 5 | UPDATE - Add FastAPI proxy, env vars |
| **New Backend** | 20+ | CREATE - FastAPI service layer |
| **New SQL Artifacts** | 5+ | CREATE - A/B/C templates, migrations |

### 0.2.4 Key Source Files for Logic Preservation

The following files contain authoritative logic that MUST be preserved exactly:

**Classification Engine** (`lib/classification-engine.ts`):
- 2026 Rules for tiering (Premium/Standard/Pause)
- Metric relevance gating (10% revenue share threshold)
- Traffic-type premium constraints
- Warning window logic (14 days)
- Volume thresholds (min_calls=50, min_leads=100)

**Quality Targets** (`lib/quality-targets.ts`):
- Per-vertical thresholds for 5 verticals (Medicare, Health, Life, Auto, Home)
- Premium/Standard/Pause threshold definitions
- Metric definitions (call_quality_rate, lead_transfer_rate, etc.)

**SQL Generator** (`lib/sql-generator.ts`):
- BigQuery SQL for unified revenue events
- Duration thresholds per vertical (Medicare >= 2700s)
- Score window (30 days ending yesterday)
- Outbound dial quality joins

**ML Analytics** (`lib/ml-analytics.ts`):
- Smart Insights: z-score anomalies (|z| >= 2.0)
- 5 behavioral clusters with deterministic composite scoring
- Priority scoring (Impact × Urgency × Confidence)
- Portfolio health score formula

## 0.3 Target Design

### 0.3.1 Refactored Structure Planning

The target architecture introduces a FastAPI backend service layer while preserving the existing Next.js frontend:

```
Target Repository Structure:
eQualityPlatform/
├── app/                              # Next.js App Router (preserved + extended)
│   ├── api/                          # API routes (proxy to FastAPI)
│   │   ├── classify/route.ts         # UPDATE - Proxy to FastAPI /runs/compute
│   │   ├── runs/route.ts             # UPDATE - Proxy to FastAPI /runs
│   │   ├── runs/[id]/route.ts        # UPDATE - Proxy to FastAPI /runs/:id
│   │   ├── actions/route.ts          # UPDATE - Proxy to FastAPI /actions
│   │   ├── sql/route.ts              # UPDATE - Extend with A/B/C templates
│   │   ├── ai-insights/route.ts      # PRESERVE - Abacus AI integration
│   │   ├── performance-history/      # CREATE - Proxy to FastAPI perf history
│   │   │   └── route.ts
│   │   └── macro-insights/           # CREATE - Proxy to FastAPI macro insights
│   │       └── route.ts
│   ├── history/page.tsx              # UPDATE - Enhanced action outcome view
│   ├── settings/page.tsx             # UPDATE - Add config_platform controls
│   ├── sql/page.tsx                  # UPDATE - A/B/C BigQuery templates
│   ├── globals.css                   # PRESERVE
│   ├── layout.tsx                    # PRESERVE
│   └── page.tsx                      # PRESERVE
├── components/                       # React components (extended)
│   ├── ui/                           # Shadcn primitives (preserve)
│   ├── classifier-client.tsx         # UPDATE - Add nav tabs structure
│   ├── results-dashboard.tsx         # UPDATE - Add badges, 8-tab expanded row
│   ├── csv-upload-component.tsx      # UPDATE - Support A/B/C schema
│   ├── column-mapper-component.tsx   # UPDATE - A/B/C column validation
│   ├── theme-provider.tsx            # PRESERVE
│   ├── performance-history-tab.tsx   # CREATE - Time series charts
│   ├── driver-analysis-tab.tsx       # CREATE - Mix vs performance decomposition
│   ├── buyer-salvage-tab.tsx         # CREATE - Path to Life simulations
│   ├── explain-tab.tsx               # CREATE - Audit packet visualization
│   └── log-action-modal.tsx          # CREATE - Action confirmation UX
├── lib/                              # Core logic (preserved + extended)
│   ├── classification-engine.ts      # PRESERVE - Deterministic rules
│   ├── quality-targets.ts            # PRESERVE - Map to config_quality_thresholds
│   ├── sql-generator.ts              # UPDATE - Add A/B/C feed templates
│   ├── ml-analytics.ts               # PRESERVE - Smart Insights (also port to Python)
│   ├── db.ts                         # PRESERVE - Prisma singleton
│   ├── theme-config.ts               # PRESERVE
│   ├── types.ts                      # UPDATE - Add new type definitions
│   ├── utils.ts                      # PRESERVE
│   └── api-client.ts                 # CREATE - FastAPI proxy helper
├── prisma/
│   ├── schema.prisma                 # UPDATE - Add fact/config tables
│   └── migrations/
│       └── [new migrations]/         # CREATE - Supabase schema migrations
├── backend/                          # CREATE - FastAPI service (NEW)
│   ├── main.py                       # FastAPI app entry point
│   ├── requirements.txt              # Python deps (exact versions)
│   ├── requirements-dev.txt          # Test deps
│   ├── api/
│   │   ├── __init__.py
│   │   ├── runs.py                   # POST/GET /runs endpoints
│   │   ├── actions.py                # POST/GET /actions endpoints
│   │   ├── insights.py               # Smart + WOW insights endpoints
│   │   ├── performance_history.py    # Trend series endpoints
│   │   └── macro_insights.py         # Macro clustering endpoints
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py                 # Settings and env management
│   │   ├── database.py               # Supabase Postgres connection
│   │   └── dependencies.py           # FastAPI dependencies
│   ├── models/
│   │   ├── __init__.py
│   │   ├── schemas.py                # Pydantic request/response models
│   │   └── enums.py                  # tx_family_enum, action_type_enum
│   ├── services/
│   │   ├── __init__.py
│   │   ├── ingestion.py              # A/B/C CSV + BigQuery ingestion
│   │   ├── rollup.py                 # Windowed rollup computation
│   │   ├── classification.py         # Classification engine (ported)
│   │   ├── driver_analysis.py        # Mix shift vs true degradation
│   │   ├── buyer_salvage.py          # Path to Life simulations
│   │   ├── change_point.py           # CUSUM change-point detection
│   │   ├── smart_insights.py         # Smart Insights (ported from ml-analytics.ts)
│   │   ├── macro_clustering.py       # Macro dimension clustering
│   │   └── outcome_tracking.py       # Difference-in-differences analysis
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── daily_memo.py             # Google Drive memo generation
│   │   └── slack_digest.py           # Slack daily digest
│   ├── sql/
│   │   ├── __init__.py
│   │   ├── rollup_queries.py         # Parameterized rollup SQL
│   │   ├── driver_queries.py         # Driver decomposition SQL
│   │   └── trend_queries.py          # Performance History SQL
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py               # Pytest fixtures
│       ├── test_ingestion.py         # A/B/C feed tests
│       ├── test_classification.py    # Classification parity tests
│       ├── test_insights.py          # Insights accuracy tests
│       ├── test_change_point.py      # Change-point detection tests
│       └── test_jobs.py              # Daily job idempotency tests
├── sql/                              # CREATE - SQL artifacts
│   ├── migrations/
│   │   ├── 001_create_enums.sql      # tx_family_enum, action_type_enum
│   │   ├── 002_create_fact_tables.sql # fact_subid_day, fact_subid_slice_day, fact_subid_buyer_day
│   │   ├── 003_create_config_tables.sql # config_quality_thresholds, config_platform
│   │   ├── 004_create_run_tables.sql # analysis_run, rollup_subid_window
│   │   ├── 005_create_output_tables.sql # classification_result, action_history
│   │   └── 006_create_insight_tables.sql # insight_change_point, insight_driver_summary, etc.
│   └── bigquery/
│       ├── feed_a_subid_day.sql      # A/B/C BigQuery template for Feed A
│       ├── feed_b_slice_day.sql      # A/B/C BigQuery template for Feed B
│       ├── feed_c_buyer_day.sql      # A/B/C BigQuery template for Feed C
│       └── trend_series.sql          # Performance History extraction
├── next.config.mjs                   # UPDATE - Add FastAPI proxy rewrites
├── package.json                      # PRESERVE - Existing deps
├── .env.example                      # UPDATE - Document required env vars
└── README.md                         # UPDATE - Setup and operations docs
```

### 0.3.2 Design Pattern Applications

**Repository Pattern for Data Access**:
- `backend/core/database.py` handles all Postgres connections
- `backend/sql/*.py` contains parameterized SQL queries
- Clean separation between business logic and data access

**Service Layer for Business Logic**:
- `backend/services/` encapsulates all compute logic
- Each service has single responsibility (classification, rollups, insights)
- Services are stateless and testable

**Dependency Injection for Loose Coupling**:
- FastAPI dependencies in `backend/core/dependencies.py`
- Database sessions, config, and auth injected into endpoints
- Enables easy testing with mocks

**Proxy Pattern for API Compatibility**:
- Next.js API routes proxy to FastAPI
- Frontend contracts unchanged
- Gradual migration path

### 0.3.3 Data Model Design

**Fact Tables** (Input A/B/C):
```
fact_subid_day         — Grain: date_et + vertical + traffic_type + tier + subid
fact_subid_slice_day   — Grain: + tx_family + slice_name + slice_value
fact_subid_buyer_day   — Grain: + buyer_key_variant + buyer_key
```

**Config Tables**:
```
config_quality_thresholds — Locked thresholds per vertical
config_platform           — Editable platform parameters
```

**Run/Output Tables**:
```
analysis_run              — Run metadata with status
rollup_subid_window       — Windowed aggregations
classification_result     — Per-subid decisions
action_history            — Audit trail with outcomes
```

**Insight Tables**:
```
insight_change_point      — CUSUM break detection
insight_driver_summary    — Mix shift decomposition
insight_buyer_salvage     — Path to Life simulations
insight_action_outcome    — Difference-in-differences tracking
```

### 0.3.4 UI Design for Expanded Row Tabs

The expanded row in the results table will have **8 tabs** in this order:

1. **Summary** — Key metrics, classification decision, badges
2. **Explain** — Audit packet: thresholds used, rules fired, why warning vs pause
3. **Drivers** — Mix shift vs true degradation decomposition with charts
4. **Buyer / Path to Life** — Buyer-level metrics + salvage simulations
5. **Performance History** — Time series charts with anomaly markers (NEW)
6. **History** — Action history with outcome tracking
7. **Notes** — User notes (existing)
8. **Log Action** — Human confirmation with rationale (moves to modal)

### 0.3.5 Web Search Research Conducted

Research conducted on best practices for:
- **FastAPI + Next.js Integration**: Proxy pattern via Next.js rewrites is standard; preserves frontend contracts while enabling Python backend
- **PostgreSQL Fact Tables**: Star schema design with dimension tables (vertical, traffic_type) and fact tables (daily aggregates) is optimal for analytics workloads
- **Change-Point Detection**: CUSUM algorithm is industry standard for detecting mean shifts in time series
- **Driver Decomposition**: Mix vs rate decomposition (Oaxaca-Blinder style) is used in marketing analytics
- **Supabase Patterns**: Row-level security not required for internal analytics; use service role for backend access

## 0.4 Transformation Mapping

### 0.4.1 File-by-File Transformation Plan

**Transformation Mode Key**:
- **UPDATE** — Modify existing file to add/extend functionality
- **CREATE** — Create new file from scratch or using reference patterns
- **REFERENCE** — Use as pattern source (no direct modification)
- **PRESERVE** — Keep file unchanged

#### Frontend Files (Next.js App Router)

| Target File | Mode | Source File | Key Changes |
|-------------|------|-------------|-------------|
| `app/page.tsx` | PRESERVE | `app/page.tsx` | No changes needed |
| `app/layout.tsx` | PRESERVE | `app/layout.tsx` | No changes needed |
| `app/globals.css` | PRESERVE | `app/globals.css` | No changes needed |
| `app/api/classify/route.ts` | UPDATE | `app/api/classify/route.ts` | Add proxy to FastAPI `/runs` + `/runs/:id/compute` |
| `app/api/runs/route.ts` | UPDATE | `app/api/runs/route.ts` | Proxy to FastAPI `/runs` |
| `app/api/runs/[id]/route.ts` | UPDATE | `app/api/runs/[id]/route.ts` | Proxy to FastAPI `/runs/:id` |
| `app/api/actions/route.ts` | UPDATE | `app/api/actions/route.ts` | Proxy to FastAPI `/actions` |
| `app/api/sql/route.ts` | UPDATE | `app/api/sql/route.ts` | Add A/B/C BigQuery template generation |
| `app/api/ai-insights/route.ts` | PRESERVE | `app/api/ai-insights/route.ts` | Keep Abacus AI integration unchanged |
| `app/api/performance-history/route.ts` | CREATE | `app/api/runs/route.ts` | Create proxy to FastAPI `/runs/:id/subid/:subid/performance-history` |
| `app/api/macro-insights/route.ts` | CREATE | `app/api/ai-insights/route.ts` | Create proxy to FastAPI `/macro-insights` |
| `app/history/page.tsx` | UPDATE | `app/history/page.tsx` | Add outcome tracking columns, action details |
| `app/settings/page.tsx` | UPDATE | `app/settings/page.tsx` | Add config_platform editable controls |
| `app/sql/page.tsx` | UPDATE | `app/sql/page.tsx` | Add A/B/C feed SQL template tabs |

#### Component Files

| Target File | Mode | Source File | Key Changes |
|-------------|------|-------------|-------------|
| `components/classifier-client.tsx` | UPDATE | `components/classifier-client.tsx` | Add primary nav tabs (New Analysis, SQL, Results, History, Smart Insights, Macro Insights) |
| `components/results-dashboard.tsx` | UPDATE | `components/results-dashboard.tsx` | Add badges, 8-tab expanded row, lazy loading |
| `components/csv-upload-component.tsx` | UPDATE | `components/csv-upload-component.tsx` | Support A/B/C feed schema validation |
| `components/column-mapper-component.tsx` | UPDATE | `components/column-mapper-component.tsx` | A/B/C required columns validation |
| `components/theme-provider.tsx` | PRESERVE | `components/theme-provider.tsx` | No changes needed |
| `components/performance-history-tab.tsx` | CREATE | `components/results-dashboard.tsx` | Create time series charts with Recharts |
| `components/driver-analysis-tab.tsx` | CREATE | `components/results-dashboard.tsx` | Create mix vs performance visualization |
| `components/buyer-salvage-tab.tsx` | CREATE | `components/results-dashboard.tsx` | Create buyer metrics + salvage simulations |
| `components/explain-tab.tsx` | CREATE | `components/results-dashboard.tsx` | Create audit packet JSON visualization |
| `components/log-action-modal.tsx` | CREATE | `components/ui/dialog.tsx` | Create action confirmation dialog |
| `components/ui/*.tsx` | PRESERVE | `components/ui/*.tsx` | Shadcn components unchanged |

#### Library Files

| Target File | Mode | Source File | Key Changes |
|-------------|------|-------------|-------------|
| `lib/classification-engine.ts` | PRESERVE | `lib/classification-engine.ts` | Keep deterministic rules unchanged |
| `lib/quality-targets.ts` | PRESERVE | `lib/quality-targets.ts` | Keep thresholds unchanged (will sync to config table) |
| `lib/sql-generator.ts` | UPDATE | `lib/sql-generator.ts` | Add A/B/C feed generation templates |
| `lib/ml-analytics.ts` | PRESERVE | `lib/ml-analytics.ts` | Keep Smart Insights (also ported to Python) |
| `lib/db.ts` | PRESERVE | `lib/db.ts` | Prisma singleton unchanged |
| `lib/theme-config.ts` | PRESERVE | `lib/theme-config.ts` | Brand colors unchanged |
| `lib/types.ts` | UPDATE | `lib/types.ts` | Add PerformanceHistoryData, DriverAnalysis, BuyerSalvage types |
| `lib/utils.ts` | PRESERVE | `lib/utils.ts` | Utilities unchanged |
| `lib/api-client.ts` | CREATE | `lib/db.ts` | Create FastAPI proxy helper |

#### Database Schema

| Target File | Mode | Source File | Key Changes |
|-------------|------|-------------|-------------|
| `prisma/schema.prisma` | UPDATE | `prisma/schema.prisma` | Add fact tables, config tables, insight tables |

#### Configuration Files

| Target File | Mode | Source File | Key Changes |
|-------------|------|-------------|-------------|
| `next.config.mjs` | UPDATE | `next.config.mjs` | Add FastAPI proxy rewrites |
| `package.json` | PRESERVE | `package.json` | Keep versions exactly as-is |
| `tsconfig.json` | PRESERVE | `tsconfig.json` | No changes needed |
| `tailwind.config.ts` | PRESERVE | `tailwind.config.ts` | No changes needed |
| `.env.example` | UPDATE | `.env` | Document all required env vars |
| `README.md` | UPDATE | `README.md` | Add backend setup, jobs, operations docs |

#### Backend Files (NEW - FastAPI)

| Target File | Mode | Source/Reference | Key Changes |
|-------------|------|------------------|-------------|
| `backend/main.py` | CREATE | — | FastAPI app with CORS, routers |
| `backend/requirements.txt` | CREATE | — | Exact pinned Python deps |
| `backend/requirements-dev.txt` | CREATE | — | Exact pinned test deps |
| `backend/api/__init__.py` | CREATE | — | Router initialization |
| `backend/api/runs.py` | CREATE | `app/api/runs/route.ts` | POST/GET /runs, /runs/:id, /runs/:id/compute |
| `backend/api/actions.py` | CREATE | `app/api/actions/route.ts` | POST/GET /actions with outcome tracking |
| `backend/api/insights.py` | CREATE | `lib/ml-analytics.ts` | Smart + WOW insights endpoints |
| `backend/api/performance_history.py` | CREATE | — | Trend series endpoints |
| `backend/api/macro_insights.py` | CREATE | `lib/ml-analytics.ts` | Macro clustering endpoints |
| `backend/core/__init__.py` | CREATE | — | Package init |
| `backend/core/config.py` | CREATE | — | Settings via pydantic-settings |
| `backend/core/database.py` | CREATE | `lib/db.ts` | Async Postgres connection |
| `backend/core/dependencies.py` | CREATE | — | FastAPI DI dependencies |
| `backend/models/__init__.py` | CREATE | — | Package init |
| `backend/models/schemas.py` | CREATE | `lib/types.ts` | Pydantic request/response models |
| `backend/models/enums.py` | CREATE | — | tx_family_enum, action_type_enum |
| `backend/services/__init__.py` | CREATE | — | Package init |
| `backend/services/ingestion.py` | CREATE | `app/api/classify/route.ts` | A/B/C CSV + BigQuery ingestion |
| `backend/services/rollup.py` | CREATE | — | Windowed rollup computation |
| `backend/services/classification.py` | CREATE | `lib/classification-engine.ts` | Classification engine (ported to Python) |
| `backend/services/driver_analysis.py` | CREATE | — | Mix shift decomposition |
| `backend/services/buyer_salvage.py` | CREATE | — | Path to Life simulations |
| `backend/services/change_point.py` | CREATE | — | CUSUM change-point detection |
| `backend/services/smart_insights.py` | CREATE | `lib/ml-analytics.ts` | Smart Insights (ported to Python) |
| `backend/services/macro_clustering.py` | CREATE | `lib/ml-analytics.ts` | MiniBatchKMeans clustering |
| `backend/services/outcome_tracking.py` | CREATE | — | Diff-in-diff analysis |
| `backend/jobs/__init__.py` | CREATE | — | Package init |
| `backend/jobs/daily_memo.py` | CREATE | — | Google Drive memo generation |
| `backend/jobs/slack_digest.py` | CREATE | — | Slack daily digest |
| `backend/sql/__init__.py` | CREATE | — | Package init |
| `backend/sql/rollup_queries.py` | CREATE | `lib/sql-generator.ts` | Parameterized rollup SQL |
| `backend/sql/driver_queries.py` | CREATE | — | Driver decomposition SQL |
| `backend/sql/trend_queries.py` | CREATE | `lib/sql-generator.ts` | Performance History SQL |
| `backend/tests/conftest.py` | CREATE | — | Pytest fixtures |
| `backend/tests/test_*.py` | CREATE | — | All required test modules |

#### SQL Artifacts

| Target File | Mode | Source/Reference | Key Changes |
|-------------|------|------------------|-------------|
| `sql/migrations/001_create_enums.sql` | CREATE | — | tx_family_enum, action_type_enum |
| `sql/migrations/002_create_fact_tables.sql` | CREATE | — | fact_subid_day, fact_subid_slice_day, fact_subid_buyer_day |
| `sql/migrations/003_create_config_tables.sql` | CREATE | `lib/quality-targets.ts` | config_quality_thresholds, config_platform |
| `sql/migrations/004_create_run_tables.sql` | CREATE | `prisma/schema.prisma` | analysis_run, rollup_subid_window |
| `sql/migrations/005_create_output_tables.sql` | CREATE | `prisma/schema.prisma` | classification_result, action_history |
| `sql/migrations/006_create_insight_tables.sql` | CREATE | — | insight_* tables |
| `sql/bigquery/feed_a_subid_day.sql` | CREATE | `lib/sql-generator.ts` | BigQuery template for Feed A |
| `sql/bigquery/feed_b_slice_day.sql` | CREATE | `lib/sql-generator.ts` | BigQuery template for Feed B |
| `sql/bigquery/feed_c_buyer_day.sql` | CREATE | `lib/sql-generator.ts` | BigQuery template for Feed C |
| `sql/bigquery/trend_series.sql` | CREATE | `lib/sql-generator.ts` | Performance History extraction |

### 0.4.2 Cross-File Dependencies

**Import Statement Updates**:

Frontend files referencing new API endpoints:
```typescript
// FROM (existing)
const res = await fetch('/api/classify', { method: 'POST', ... })

// TO (preserved - proxy handles routing)
const res = await fetch('/api/classify', { method: 'POST', ... })
// Note: API contract unchanged; route.ts proxies to FastAPI internally
```

New component imports in results-dashboard.tsx:
```typescript
// ADD these imports
import { PerformanceHistoryTab } from './performance-history-tab'
import { DriverAnalysisTab } from './driver-analysis-tab'
import { BuyerSalvageTab } from './buyer-salvage-tab'
import { ExplainTab } from './explain-tab'
import { LogActionModal } from './log-action-modal'
```

Type imports for new features:
```typescript
// lib/types.ts additions
export interface PerformanceHistoryData { ... }
export interface DriverAnalysis { ... }
export interface BuyerSalvage { ... }
export interface ExplainPacket { ... }
```

### 0.4.3 Configuration Updates

**next.config.mjs** — Add FastAPI proxy:
```javascript
async rewrites() {
  return [
    {
      source: '/backend-api/:path*',
      destination: 'http://localhost:8000/:path*',
    },
  ]
}
```

**.env additions**:
```
# FastAPI Backend

FASTAPI_URL=http://localhost:8000

#### BigQuery (for direct table feeds)

GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
BIGQUERY_PROJECT=dwh-production-352519

#### Google Drive (for daily memos)

GOOGLE_DRIVE_FOLDER_ID=your-folder-id

#### Slack (for daily digests)

SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx
```

### 0.4.4 One-Phase Execution

**CRITICAL**: The entire refactor will be executed by Blitzy in ONE phase. ALL files are included in this single transformation phase. No splitting into multiple phases.

## 0.5 Dependency Inventory

### 0.5.1 Key Private and Public Packages

#### Frontend Dependencies (PRESERVE — No Changes)

| Registry | Package | Version | Purpose |
|----------|---------|---------|---------|
| npm | react | 18.2.0 | UI framework |
| npm | react-dom | 18.2.0 | React DOM bindings |
| npm | next | 14.2.28 | App Router framework |
| npm | typescript | 5.2.2 | Type checking |
| npm | tailwindcss | 3.3.3 | Utility-first CSS |
| npm | @prisma/client | 6.7.0 | Database ORM |
| npm | prisma | 6.7.0 | Prisma CLI |
| npm | papaparse | 5.4.1 | CSV parsing |
| npm | lucide-react | 0.263.1 | Icon library |
| npm | recharts | 2.15.0 | Chart library |
| npm | class-variance-authority | 0.7.1 | Styling utilities |
| npm | clsx | 2.1.1 | Classname utilities |
| npm | tailwind-merge | 2.6.0 | Tailwind class merging |
| npm | @radix-ui/react-* | various | Shadcn UI primitives |

#### Backend Dependencies (CREATE — Exact Versions Required)

**backend/requirements.txt** (Production):

| Registry | Package | Version | Purpose |
|----------|---------|---------|---------|
| PyPI | fastapi | 0.115.6 | API framework |
| PyPI | uvicorn[standard] | 0.30.6 | ASGI server |
| PyPI | pydantic | 2.10.4 | Data validation |
| PyPI | httpx | 0.27.2 | Async HTTP client |
| PyPI | tenacity | 9.0.0 | Retry logic |
| PyPI | python-dateutil | 2.9.0.post0 | Date utilities |
| PyPI | numpy | 2.1.3 | Numerical computing |
| PyPI | pandas | 2.2.3 | Data manipulation |
| PyPI | scikit-learn | 1.5.2 | ML algorithms (MiniBatchKMeans) |
| PyPI | google-cloud-bigquery | 3.29.0 | BigQuery client |
| PyPI | google-auth | 2.37.0 | Google auth |
| PyPI | google-api-python-client | 2.156.0 | Google Drive API |
| PyPI | slack-sdk | 3.33.5 | Slack integration |
| PyPI | asyncpg | 0.30.0 | Async Postgres driver |
| PyPI | psycopg2-binary | 2.9.9 | Postgres driver (sync) |

**backend/requirements-dev.txt** (Development/Test):

| Registry | Package | Version | Purpose |
|----------|---------|---------|---------|
| PyPI | pytest | 8.3.4 | Test framework |
| PyPI | pytest-asyncio | 0.25.0 | Async test support |

### 0.5.2 Dependency Updates

#### Import Refactoring

**Files requiring import updates** (use wildcards):

| Pattern | Update Type | Description |
|---------|-------------|-------------|
| `app/api/**/route.ts` | ADD | Add FastAPI proxy imports, httpx-style fetch |
| `components/*.tsx` | ADD | Add new component imports for tabs |
| `lib/types.ts` | ADD | Add new type definitions |

**Import transformation rules**:

```typescript
// Frontend: No breaking changes to external imports
// Internal: Add new imports where needed

// Example: results-dashboard.tsx
// OLD imports (preserved):
import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'

// NEW imports (added):
import { PerformanceHistoryTab } from './performance-history-tab'
import { DriverAnalysisTab } from './driver-analysis-tab'
import { BuyerSalvageTab } from './buyer-salvage-tab'
import { ExplainTab } from './explain-tab'
import { LogActionModal } from './log-action-modal'
```

#### External Reference Updates

**Configuration files requiring updates**:

| Pattern | Files | Update Description |
|---------|-------|-------------------|
| `next.config.mjs` | 1 | Add FastAPI proxy rewrites |
| `.env*` | 2 | Add FASTAPI_URL, Google credentials, Slack webhook |
| `README.md` | 1 | Add backend setup, job operations |
| `package.json` | 0 | NO CHANGES — preserve versions |

### 0.5.3 Runtime Requirements

**Node.js Runtime**:
- Version: 20.x (match existing repo)
- Package Manager: Yarn 1.22.x

**Python Runtime**:
- Version: 3.11.8 (exact)
- Virtual environment: Required for isolation

### 0.5.4 Database Dependencies

**Supabase PostgreSQL**:
- Connection: Via DATABASE_URL environment variable
- Driver (Python): asyncpg for async, psycopg2-binary for sync migrations
- Driver (Node): Prisma Client 6.7.0

**BigQuery** (optional — for direct table feeds):
- Project: dwh-production-352519
- Auth: Service account JSON credentials
- Tables:
  - `dwh-production-352519.unified.unifiedrevenue` (event-level)
  - `dwh-production-352519.reference.subids` (reference snapshot)

### 0.5.5 Third-Party Service Dependencies

| Service | Purpose | Authentication |
|---------|---------|----------------|
| Supabase | PostgreSQL database | DATABASE_URL connection string |
| Google BigQuery | A/B/C feed source (optional) | Service account JSON |
| Google Drive | Daily memo storage | Service account JSON |
| Slack | Daily digest notifications | Webhook URL |
| Abacus AI | AI-generated summaries | API key (existing) |

### 0.5.6 Exact requirements.txt Content

**backend/requirements.txt**:
```
fastapi==0.115.6
uvicorn[standard]==0.30.6
pydantic==2.10.4
httpx==0.27.2
tenacity==9.0.0
python-dateutil==2.9.0.post0
numpy==2.1.3
pandas==2.2.3
scikit-learn==1.5.2
google-cloud-bigquery==3.29.0
google-auth==2.37.0
google-api-python-client==2.156.0
slack-sdk==3.33.5
asyncpg==0.30.0
psycopg2-binary==2.9.9
```

**backend/requirements-dev.txt**:
```
pytest==8.3.4
pytest-asyncio==0.25.0
```

## 0.6 Scope Boundaries

### 0.6.1 Exhaustively In Scope

#### Source Transformations

| Pattern | Description |
|---------|-------------|
| `app/api/**/route.ts` | All Next.js API routes (proxy to FastAPI) |
| `app/**/page.tsx` | All Next.js pages (history, settings, sql) |
| `components/classifier-client.tsx` | Main orchestrator component |
| `components/results-dashboard.tsx` | Results table with expanded rows |
| `components/csv-upload-component.tsx` | CSV wizard for A/B/C feeds |
| `components/column-mapper-component.tsx` | Column mapping for A/B/C |
| `lib/sql-generator.ts` | BigQuery SQL templates |
| `lib/types.ts` | Type definitions |
| `prisma/schema.prisma` | Database schema |

#### New Backend Creation

| Pattern | Description |
|---------|-------------|
| `backend/**/*.py` | All FastAPI backend files |
| `backend/api/*.py` | API endpoint handlers |
| `backend/services/*.py` | Business logic services |
| `backend/jobs/*.py` | Daily automation jobs |
| `backend/sql/*.py` | Parameterized SQL queries |
| `backend/tests/*.py` | All test files |
| `backend/requirements*.txt` | Python dependencies |

#### New Component Creation

| Pattern | Description |
|---------|-------------|
| `components/performance-history-tab.tsx` | Time series visualization |
| `components/driver-analysis-tab.tsx` | Mix vs performance decomposition |
| `components/buyer-salvage-tab.tsx` | Buyer metrics and salvage |
| `components/explain-tab.tsx` | Audit packet visualization |
| `components/log-action-modal.tsx` | Action confirmation dialog |
| `lib/api-client.ts` | FastAPI proxy helper |

#### SQL Artifacts

| Pattern | Description |
|---------|-------------|
| `sql/migrations/*.sql` | Supabase schema migrations |
| `sql/bigquery/*.sql` | A/B/C feed generation templates |

#### Configuration Updates

| Pattern | Description |
|---------|-------------|
| `next.config.mjs` | FastAPI proxy rewrites |
| `.env.example` | Environment variable documentation |
| `README.md` | Operations and setup documentation |

#### Test Updates

| Pattern | Description |
|---------|-------------|
| `backend/tests/test_ingestion.py` | A/B/C feed ingestion tests |
| `backend/tests/test_classification.py` | Classification parity tests |
| `backend/tests/test_insights.py` | Insights accuracy tests |
| `backend/tests/test_change_point.py` | Change-point detection tests |
| `backend/tests/test_jobs.py` | Daily job idempotency tests |

### 0.6.2 Explicitly Out of Scope

Based on user directives, the following are **NOT** part of this refactor:

| Out of Scope Item | Reason |
|-------------------|--------|
| **Autonomous pausing/routing/bidding** | System only recommends; humans confirm via Log Action |
| **Raw event row storage in-app** | System-of-record is A/B/C daily aggregates, not raw events |
| **Next.js/React version upgrades** | Version lock: maintain 14.2.28 and 18.2.0 |
| **New npm dependencies** | Frontend deps frozen; backend deps pinned |
| **Frontend design overhaul** | Preserve existing UI conventions |
| **Authentication/authorization changes** | Keep existing access patterns |
| **Mobile-specific optimizations** | Desktop-first analytics dashboard |
| **Internationalization (i18n)** | English-only interface |
| **Real-time streaming updates** | Batch analytics, not real-time |
| **Multi-tenant architecture** | Single-tenant deployment |

### 0.6.3 Preservation Requirements

The following MUST be preserved exactly as-is:

| Preservation Item | File(s) | Reason |
|-------------------|---------|--------|
| Classification rules | `lib/classification-engine.ts` | Authoritative business logic |
| Quality thresholds | `lib/quality-targets.ts` | Vertical-specific thresholds |
| Smart Insights logic | `lib/ml-analytics.ts` | Deterministic behavioral clusters |
| Brand theming | `lib/theme-config.ts` | UI consistency |
| Shadcn components | `components/ui/*.tsx` | UI primitives |
| Prisma singleton | `lib/db.ts` | Database connection |
| Abacus AI integration | `app/api/ai-insights/route.ts` | Existing AI summaries |
| API contracts | All `/api/` routes | Frontend compatibility |

### 0.6.4 Boundary Conditions

**Metric Relevance Gating**:
- call_presence = call_rev / rev
- lead_presence = lead_rev / rev
- Metric relevant if presence >= 0.10 (metric_presence_threshold)
- Metric actionable if calls >= 50 OR leads >= 100

**Traffic-Type Premium Constraints**:
- Full O&O: Premium allowed
- Partial O&O: Premium allowed only for Health + Life
- Non O&O: Premium not allowed

**Warning Window**:
- warning_until = as_of_date + 14 days (warning_window_days)
- No auto-pause during warning period

**Slice Value Limits**:
- Top 50 slice_value per (date_et, subid, tx_family, slice_name) by rev DESC
- Smart Unspecified: exclude when fill_rate_by_rev >= 0.90

### 0.6.5 Data Flow Boundaries

```
┌─────────────────────────────────────────────────────────────────┐
│                         IN SCOPE                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  BigQuery Tables ─────┐                                         │
│         OR            ├──► A/B/C Feeds ──► Supabase Facts       │
│  CSV Uploads ─────────┘                                         │
│                                                                  │
│  Supabase Facts ──► Rollups ──► Classification ──► Results     │
│                                                                  │
│  Results ──► Insights (Smart + WOW + Macro)                     │
│                                                                  │
│  Results ──► Daily Jobs ──► Drive Memo + Slack Digest           │
│                                                                  │
│  Results ──► UI ──► Human Review ──► Log Action ──► Audit      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        OUT OF SCOPE                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Results ──X──► Auto-Pause/Route/Bid                            │
│                                                                  │
│  Raw Events ──X──► In-App Storage                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 0.6.6 API Contract Preservation

**Existing endpoints (MUST maintain identical contracts)**:

| Endpoint | Method | Contract |
|----------|--------|----------|
| `/api/classify` | POST | Accept CSV data, return classification results |
| `/api/runs` | GET | Return list of analysis runs |
| `/api/runs/[id]` | GET | Return specific run details |
| `/api/actions` | GET | Return action history list |
| `/api/actions` | POST | Create new action record |
| `/api/sql` | POST | Generate BigQuery SQL |
| `/api/ai-insights` | POST | Generate AI summaries |

**New endpoints (ADD without breaking existing)**:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/performance-history` | GET | Trend series data |
| `/api/macro-insights` | GET | Macro clustering results |
| `/api/runs/[id]/subid/[subid]/detail` | GET | Full detail bundle |

## 0.7 Special Analysis

### 0.7.1 "WOW" Insights Implementation Analysis

The user requires a comprehensive "WOW" Insights layer with seven distinct capabilities. Each requires specific algorithmic implementation:

#### Change-Point Detection ("It Broke Here")

**Algorithm**: CUSUM (Cumulative Sum Control Charts) backed by rolling z-score

**Implementation Requirements**:
- Compute daily metric deltas over trend window
- Apply CUSUM to detect mean shifts
- Output: `break_date`, affected `metrics`, `confidence` level
- Persistence: `insight_change_point` table

**Detection Logic**:
```python
# Pseudocode for CUSUM implementation

def detect_change_point(daily_metrics, threshold=5.0):
    mean = np.mean(daily_metrics[:30])  # Baseline period
    std = np.std(daily_metrics[:30])
    
    cusum_pos = cusum_neg = 0
    for i, value in enumerate(daily_metrics):
        z_score = (value - mean) / std
        cusum_pos = max(0, cusum_pos + z_score - 0.5)
        cusum_neg = min(0, cusum_neg + z_score + 0.5)
        
        if cusum_pos > threshold or abs(cusum_neg) > threshold:
            return i  # Break date index
```

#### Driver Analysis (Mix Shift vs True Degradation)

**Algorithm**: Oaxaca-Blinder style decomposition using slice data (Feed B)

**Period Definitions**:
- Baseline period: days -30 to -16 (relative to as_of_date)
- Bad period: days -15 to -1

**Decomposition Logic**:
- For each slice_name:
  - Compute baseline share by rev vs bad share by rev
  - Compute baseline metric vs bad metric
  - Rank contributions by absolute impact
- Split total delta into:
  - **Mix effect**: Change due to shift in traffic composition
  - **Performance effect**: Change due to metric degradation within same mix

**Output**: Top slice_names and top slice_values with mix vs performance attribution
**Persistence**: `insight_driver_summary` table

#### Buyer Sensitivity & "Path to Life" Salvage

**Algorithm**: Simulation-based buyer removal analysis using Feed C

**Implementation Requirements**:
- Compute buyer-level metrics from `fact_subid_buyer_day`
- Identify bottom-performing buyers by call_quality_rate and lead_transfer_rate
- Simulate removing bottom buyer(s)
- Calculate expected quality improvement and revenue impact

**Output**: Top 3 salvage options with:
- Expected quality delta
- Revenue impact (loss from removal)
- Net recommendation score

**Persistence**: `insight_buyer_salvage` table

#### Audit-Grade Explain Packet

**Requirements**:
- Thresholds used for classification
- Relevancy check results (metric presence >= 10%)
- Volume check results (calls >= 50 OR leads >= 100)
- Rule fired (which threshold triggered tier assignment)
- Why warning vs pause vs keep
- Return as JSON in detail endpoint

#### Action Outcome Tracking

**Algorithm**: Difference-in-differences (DiD) analysis

**Implementation**:
- Pre-period: 14 days before action
- Post-period: 14 days after action
- Matched cohort: Similar sub_ids that did NOT receive action
- Calculate: quality delta, revenue impact, outcome label

**Persistence**: `insight_action_outcome` table

#### Data Coverage Monitor

**Source**: `fact_subid_slice_day.fill_rate_by_rev`

**Logic**:
- Track fill rate trends
- Suppress driver claims when missingness worsens significantly
- Show UI banner when data coverage is concerning

#### Guardrail Tagging ("Do Not Touch")

**Tags Applied**:
- `low_volume`: Below min_calls_window or min_leads_window
- `high_revenue_concentration`: Single buyer > 50% of revenue
- `recently_acted`: Action within last 7 days
- `in_warning_window`: Currently in warning period

### 0.7.2 Smart Insights Parity Analysis

The existing `lib/ml-analytics.ts` implements Smart Insights that MUST be preserved and ported to Python:

#### Anomaly Detection (Z-Score)

**Scope**: Cohort = vertical + traffic_type

**Metrics**:
- call_quality_rate
- lead_transfer_rate  
- total_revenue

**Threshold**: |z| >= 2.0 triggers anomaly flag

**Current Implementation** (from ml-analytics.ts):
```typescript
const anomalies = metricNames.filter(metric => {
  const mean = cohortStats[metric].mean;
  const stdDev = cohortStats[metric].stdDev;
  const value = record[metric];
  const zScore = stdDev > 0 ? Math.abs((value - mean) / stdDev) : 0;
  return zScore >= 2.0;
});
```

#### Behavioral Clusters (Deterministic)

**Algorithm**: Composite score with fixed thresholds

**Cluster Ranges**:
- 80-100: Star Performers
- 60-80: Solid Contributors
- 40-60: Growth Potential
- 20-40: Watch List
- 0-20: Critical Attention

**Score Calculation**: Weighted combination of quality metrics, revenue, and stability

#### Priority Scoring

**Formula**: Score = Impact × Urgency Multiplier × Confidence

**Urgency Multipliers**:
- Immediate: 1.5
- Short-term: 1.2
- Medium-term: 1.0

#### Portfolio Health Score

**Components**:
- Revenue at Risk (pause/warning recommendations)
- Diversification: (1 - HHI) × 100 where HHI = Σ(share²)
- Weighted formula documented in existing code

### 0.7.3 Macro Insights Implementation Analysis

**Goal**: Detect patterns beyond individual sub_id using A/B/C at scale

#### Macro Dimensions

Available only if derivable from data (no guessing):
- buyer/account manager (if derivable from repo-defined mappings)
- marketing_angle (if present in slices)
- domain (hostname extracted from ad_source in Feed B)
- keyword_bucket (deterministic rules)
- most frequent buyer(s) (from Feed C)

#### Macro Clustering Algorithm

**Framework**: Python deterministic clustering

**Steps**:
1. Build feature table for trend window:
   - Rates: call_quality_rate, lead_transfer_rate
   - Revenue per unit: rp_lead, rp_qcall, rp_click, rp_redirect
   - Total revenue, volume
   - Stability/momentum indicators
   - Categorical features

2. Preprocessing:
   - OneHotEncode categorical features
   - StandardScaler for numeric features

3. Clustering:
   - Algorithm: MiniBatchKMeans (for scalability)
   - Fixed random_state for reproducibility
   - k selection: bounded search k=4..12 using silhouette score on sample

4. Cluster labeling:
   - Template-based labels using top differentiating features
   - Deterministic label generation (no LLM)

#### Keyword Bucketing Rules

**Normalization**:
- Lowercase
- Trim whitespace
- Collapse multiple spaces to single
- Remove punctuation (keep digits)

**Bucketing** (ordered regex/contains rules, first match wins):
- Brand terms → "brand"
- Competitor terms → "competitor"
- Product terms → "product"
- Price/cost terms → "price-sensitive"
- Informational terms → "informational"
- Fallback → "other"

**Unit Tests Required**: Verify bucketing determinism

### 0.7.4 Performance History Analysis

**Data Requirements**:
- Trend window: default 180 days ending yesterday
- Exclude today from all calculations
- Lazy loading on row expand (must not slow main table)

**UI Elements** (minimum):

| Chart Type | Metrics |
|------------|---------|
| Line chart | call_quality_rate |
| Line chart | lead_transfer_rate |
| Line chart | total_revenue |
| Line chart | paid_calls + calls (overlay) |
| Line chart | lead_volume / click_volume / redirect_volume |

**Rolling Summaries**:
- Last 7 vs prior 7 deltas for key metrics
- Last 30 vs prior 30 deltas

**Anomaly Markers**:
- Show on time series where z-score >= 2.0
- Hover shows affected metric(s) and z-score values

**Stability/Momentum Panel**:
- Volatility (standard deviation over trend window)
- Momentum (slope of last 14 days via linear regression)

**Peer Benchmark Overlay**:
- Cohort median lines for vertical + traffic_type

### 0.7.5 Bounded What-If Simulator

**Allowed Simulations**:
- Remove specific slice_value from analysis
- Remove specific buyer_key from analysis

**Output**:
- Expected quality delta (improvement/degradation)
- Revenue delta (loss from removal)
- Confidence level based on data coverage

## 0.8 Refactoring Rules

### 0.8.1 Absolute Core Rules (User-Specified)

The following rules are explicitly emphasized by the user and MUST be enforced:

| Rule | Description |
|------|-------------|
| **System-of-record** | Supabase Postgres facts fed by A/B/C daily aggregated feeds (not raw event rows in-app) |
| **No autonomous actions** | System only recommends; humans confirm via Log Action. No auto-pause/route/bidding |
| **Cohort scoping** | All cohort comparisons and driver analysis MUST be scoped to vertical + traffic_type |
| **Version lock** | No substitutions on runtime versions or dependencies. Exact versions required |
| **Minimal change** | Make only changes necessary for this refactor. Avoid unrelated refactors |
| **Preserve behavior** | Preserve existing behavior exactly unless explicitly changed in specification |
| **Isolate new logic** | Isolate new logic in new modules. Document key changes in comments |

### 0.8.2 Technical Preservation Rules

**API Contract Preservation**:
- All existing frontend-used API endpoints MUST return identical schema as before
- No breaking changes to request/response formats
- Add new endpoints only; never remove or modify existing contracts

**Classification Logic Preservation**:
- The "2026 Rules" in `lib/classification-engine.ts` are authoritative
- Python port must produce bit-identical results for same inputs
- Classification parity tests must pass for all vertical + traffic_type combinations

**Smart Insights Preservation**:
- Z-score anomaly detection (|z| >= 2.0) unchanged
- Behavioral cluster thresholds (0-20, 20-40, 40-60, 60-80, 80-100) unchanged
- Priority scoring formula (Impact × Urgency × Confidence) unchanged
- Portfolio health score formula unchanged

**Threshold Preservation**:
- All thresholds from `lib/quality-targets.ts` must be maintained
- Thresholds seeded into `config_quality_thresholds` table must match exactly
- No modification to Premium/Standard/Pause boundaries

### 0.8.3 Data Integrity Rules

**Feed A (fact_subid_day)**:
- Grain: date_et + vertical + traffic_type + tier + subid
- All required measures MUST be present: calls, paid_calls, qual_paid_calls, transfer_count, leads, clicks, redirects, call_rev, lead_rev, click_rev, redirect_rev, rev
- Derived metrics computed in rollups, NOT stored in fact table

**Feed B (fact_subid_slice_day)**:
- Grain: date_et + vertical + traffic_type + tier + subid + tx_family + slice_name + slice_value
- Slice value cap: Top 50 per (date_et, subid, tx_family, slice_name) by rev DESC
- Smart Unspecified: exclude slice_value='Unspecified' when fill_rate_by_rev >= 0.90

**Feed C (fact_subid_buyer_day)**:
- Grain: date_et + vertical + traffic_type + tier + subid + buyer_key_variant + buyer_key
- buyer_key_variant must support: carrier_name and concatenated variants

### 0.8.4 Metric Calculation Rules

**Derived Metrics** (AUTHORITATIVE formulas):

| Metric | Formula |
|--------|---------|
| qr_rate | paid_calls / calls |
| call_quality_rate | qual_paid_calls / paid_calls |
| lead_transfer_rate | transfer_count / leads |
| rp_lead | lead_rev / leads |
| rp_qcall | call_rev / paid_calls |
| rp_click | click_rev / clicks |
| rp_redirect | redirect_rev / redirects |

**Metric Presence Gating**:
- call_presence = call_rev / rev
- lead_presence = lead_rev / rev
- Metric relevant if presence >= metric_presence_threshold (default 0.10)

**Volume Gating**:
- Metric actionable for calls if calls >= min_calls_window (default 50)
- Metric actionable for leads if leads >= min_leads_window (default 100)
- If not relevant OR insufficient volume: metric tier = 'na', cannot trigger Pause

### 0.8.5 Classification Decision Rules

**Per-Metric Tier Evaluation**:
- Premium if metric >= premium_threshold
- Standard if metric >= standard_threshold
- Else Pause

**Traffic-Type Premium Constraints** (AUTHORITATIVE):

| Traffic Type | Premium Allowed |
|--------------|-----------------|
| Full O&O | Yes (all verticals) |
| Partial O&O | Yes (Health + Life only) |
| Non O&O | No |

**Decision Outputs** (minimum required fields):
- recommended_class: Premium / Standard / Pause / Warn / Watch
- action_recommendation: pause | warn_14d | keep | promote | demote
- confidence: High / Med / Low
- reason_codes: array of strings
- warning_until: datetime if warning (as_of_date + warning_window_days)

**Original Platform Semantics** (for badges/labels):
- pauseimmediate
- warning14day
- demotewithwarning
- insufficientvolume
- review

These are interpreted as recommendations + UI badges; never auto-executed.

### 0.8.6 Idempotency Rules

**Daily Jobs**:
- Google Drive memo: Never duplicate memos for same vertical/date
- Slack digest: Never duplicate digests for same date
- Persisted state tracks last successful run per vertical/date

**Analysis Runs**:
- Each run creates unique `analysis_run` record
- Re-running for same date creates new run, preserves history
- Rollups idempotent: same inputs → same outputs

### 0.8.7 Performance Rules

**Main Table**:
- Default sort by rev DESC
- Pagination for large result sets
- Badges computed at query time, not stored

**Performance History Tab**:
- MUST load lazily on row expand
- MUST NOT slow main table rendering
- API response within 2 seconds for typical data volumes

**Macro Clustering**:
- Use MiniBatchKMeans for scalability
- Sample-based silhouette scoring to cap computation cost
- Cluster computation runs async, results cached

### 0.8.8 Testing Requirements

All tests MUST pass before deployment:

| Test Category | Coverage |
|---------------|----------|
| Contract tests | All existing API endpoints return identical schema |
| A/B/C ingestion tests | Required columns/types, grain uniqueness, upsert correctness |
| Metric parity tests | Rollup metrics correct, call_quality_rate formula, presence/volume gating |
| Classification parity tests | Representative cases by vertical + traffic_type, warning_until behavior |
| Driver decomposition tests | Baseline/bad periods anchored to as_of_date, mix vs performance split stable |
| Buyer salvage tests | Deterministic removal simulation, top 3 options correct ordering |
| Change-point tests | Detects known break date on synthetic series |
| Smart Insights parity tests | Z-score anomalies, cluster assignment, priority scoring, portfolio health |
| Performance History tests | Series excludes today, cohort baselines returned, anomaly markers included |
| Daily jobs tests | Idempotency state prevents duplicates for Drive and Slack |

## 0.9 References

### 0.9.1 Source Repository Files Analyzed

The following files were comprehensively analyzed to derive conclusions for this Agent Action Plan:

#### API Routes Examined

| File Path | Analysis Summary |
|-----------|------------------|
| `app/api/classify/route.ts` | Core classification endpoint; parses CSV, aggregates by dimension, applies classification rules, persists to Prisma |
| `app/api/runs/route.ts` | Analysis runs list endpoint; fetches latest 10 runs ordered by createdAt DESC |
| `app/api/runs/[id]/route.ts` | Run details endpoint; fetches specific run with results |
| `app/api/actions/route.ts` | Action history endpoint; POST creates new action, GET lists history |
| `app/api/sql/route.ts` | SQL generation endpoint; delegates to lib/sql-generator.ts |
| `app/api/ai-insights/route.ts` | AI summaries endpoint; integrates with Abacus AI gpt-4.1-mini |

#### Library Files Examined

| File Path | Analysis Summary |
|-----------|------------------|
| `lib/classification-engine.ts` | Contains "2026 Rules" for Premium/Standard/Pause tiering; metric relevance gating; traffic-type premium constraints |
| `lib/quality-targets.ts` | Threshold definitions for 5 verticals (Medicare, Health, Life, Auto, Home); Premium/Standard boundaries |
| `lib/sql-generator.ts` | BigQuery SQL template for unified revenue events; duration thresholds per vertical; 30-day rolling windows |
| `lib/ml-analytics.ts` | Smart Insights engine; z-score anomalies; behavioral clusters; priority scoring; portfolio health |
| `lib/db.ts` | Prisma client singleton pattern |
| `lib/theme-config.ts` | Brand colors (Excel Green, Purple, Orange); theme configuration |
| `lib/types.ts` | AggregationDimension types; configuration structures |
| `lib/utils.ts` | Utility functions including cn() for className merging |

#### Component Files Examined

| File Path | Analysis Summary |
|-----------|------------------|
| `components/classifier-client.tsx` | Main orchestrator; Upload→Map Columns→Results workflow; localStorage persistence; papaparse CSV parsing |
| `components/results-dashboard.tsx` | Results visualization; formatPct helper; deriveActionFromMetric logic; expanded row rendering |
| `components/csv-upload-component.tsx` | Drag-and-drop CSV upload; file validation |
| `components/column-mapper-component.tsx` | Column mapping interface; schema validation |
| `components/theme-provider.tsx` | Next-themes provider wrapper |
| `components/ui/*.tsx` | Shadcn UI primitives (Button, Card, Badge, Dialog, Input, Label, Select, Tabs, Table, etc.) |

#### Configuration Files Examined

| File Path | Analysis Summary |
|-----------|------------------|
| `package.json` | Frontend dependencies with exact versions; React 18.2.0, Next.js 14.2.28, Prisma 6.7.0 |
| `tsconfig.json` | TypeScript configuration; path aliases (@/*) |
| `tailwind.config.ts` | Tailwind CSS configuration; custom colors; CSS variables |
| `next.config.mjs` | Next.js configuration; ESLint/TypeScript ignore during builds |
| `prisma/schema.prisma` | Database models; AnalysisRun, ClassificationResult, ActionHistory |
| `.env` | Environment variables; DATABASE_URL connection string |
| `README.md` | Project overview; setup instructions; workflow description |

#### Database Schema Examined

| Model | Fields Summary |
|-------|----------------|
| AnalysisRun | id, name, description, status, runDate, createdAt, updatedAt, results |
| ClassificationResult | id, runId, dimension, subId, source, vertical, trafficType, metrics (JSON), classification, rawData, qualityTier, actionNeeded, reason |
| ActionHistory | id, runId, action, resultId, subId, reason, takenBy, createdAt |

#### Folders Traversed

| Folder Path | Contents Summary |
|-------------|------------------|
| `/` (root) | Next.js project root with App Router structure |
| `app/` | Pages (page.tsx, layout.tsx) and API routes |
| `app/api/` | 5 API route handlers (classify, runs, actions, sql, ai-insights) |
| `app/history/` | History page component |
| `app/settings/` | Settings page component |
| `app/sql/` | SQL page component |
| `components/` | React components including classifier-client, results-dashboard |
| `components/ui/` | 15+ Shadcn UI primitive components |
| `lib/` | Core business logic modules |
| `prisma/` | Database schema and migrations |
| `public/` | Static assets |

### 0.9.2 User-Provided Attachments

**No file attachments were provided with this request.**

### 0.9.3 User-Provided URLs

**No Figma URLs or external URLs were provided with this request.**

### 0.9.4 External References Consulted

| Resource | Purpose |
|----------|---------|
| Supabase Documentation | PostgreSQL fact table design patterns; testing approaches |
| FastAPI Documentation | Backend service architecture patterns |
| scikit-learn Documentation | MiniBatchKMeans clustering algorithm |
| CUSUM Algorithm References | Change-point detection methodology |

### 0.9.5 Key BigQuery References (from sql-generator.ts)

| Reference | Purpose |
|-----------|---------|
| `dwh-production-352519.unified.unifiedrevenue` | Event-level unified revenue data |
| `dwh-production-352519.reference.subids` | Reference snapshot (latest snapshotdate) |
| Outbound dial-on-leads join via sessionid | Calls back to leads linkage |
| ad_source | Landing page URL (for domain extraction); NOT landing_page |
| Score window | Rolling 30 days ending yesterday (exclude today) |

### 0.9.6 Vertical Duration Thresholds (from sql-generator.ts)

| Vertical | Duration Threshold |
|----------|-------------------|
| Medicare | >= 2700 seconds |
| Health | >= 300 seconds |
| Life | >= 180 seconds |
| Auto | >= 120 seconds |
| Home | >= 90 seconds |

### 0.9.7 Environment Variables Required

| Variable | Purpose | Required |
|----------|---------|----------|
| DATABASE_URL | Supabase PostgreSQL connection string | Yes |
| FASTAPI_URL | FastAPI backend URL (default: http://localhost:8000) | Yes |
| GOOGLE_APPLICATION_CREDENTIALS | Path to GCP service account JSON | For BigQuery feeds |
| BIGQUERY_PROJECT | BigQuery project ID | For BigQuery feeds |
| GOOGLE_DRIVE_FOLDER_ID | Drive folder for daily memos | For daily jobs |
| SLACK_WEBHOOK_URL | Slack webhook for daily digests | For daily jobs |
| ABACUS_API_KEY | Abacus AI API key | For AI insights |

### 0.9.8 config_platform Default Values

| Key | Default Value | Description |
|-----|---------------|-------------|
| min_calls_window | 50 | Minimum calls for actionable metric |
| min_leads_window | 100 | Minimum leads for actionable metric |
| metric_presence_threshold | 0.10 | Minimum revenue share for metric relevance |
| warning_window_days | 14 | Days in warning period |
| unspecified_keep_fillrate_threshold | 0.90 | Fill rate below which to keep 'Unspecified' slices |


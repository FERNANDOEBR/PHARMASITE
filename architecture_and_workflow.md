# PharmaSite Intelligence — Architecture & Workflow Reference

> **Version:** 2.0 (Phase 2 + AI Agents complete)
> **Last updated:** 2026-03-03
> **Status:** Production-ready for local deployment via Docker Compose

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Technology Stack & Architecture](#2-technology-stack--architecture)
   - 2.1 [Data Ingestion — Agent 1 (Python ETL)](#21-data-ingestion--agent-1-python-etl)
   - 2.2 [Self-Healing AI — Agents 3 & 4 (LangChain)](#22-self-healing-ai--agents-3--4-langchain)
   - 2.3 [Backend API — Phase 2 (FastAPI)](#23-backend-api--phase-2-fastapi)
   - 2.4 [Frontend — Agent 3 (Next.js + Deck.gl)](#24-frontend--next.js--deckgl)
   - 2.5 [Infrastructure (Docker Compose + PostGIS)](#25-infrastructure-docker-compose--postgis)
3. [Core Workflows](#3-core-workflows)
   - 3.1 [Workflow A: Data Ingestion & Scoring](#31-workflow-a-data-ingestion--scoring)
   - 3.2 [Workflow B: LLM Self-Healing Pipeline](#32-workflow-b-llm-self-healing-pipeline)
   - 3.3 [Workflow C: Trade Area Intelligence (UI → AI)](#33-workflow-c-trade-area-intelligence-ui--ai)
4. [Database Schema](#4-database-schema)
5. [API Endpoint Reference](#5-api-endpoint-reference)
6. [Key Code Signatures & Payloads](#6-key-code-signatures--payloads)
7. [Known Limitations & Operational Notes](#7-known-limitations--operational-notes)

---

## 1. Executive Summary

**PharmaSite Intelligence** is a geospatial market intelligence platform designed to help pharmaceutical companies, distributors, and pharmacy chains identify and prioritize Brazilian municipalities for new store openings, distribution network expansion, or commercial partnership targeting.

The platform ingests data on all **5,570 Brazilian municipalities** from public government APIs (IBGE, DataSUS/CNES, IPEADATA, ANS, Receita Federal), computes a composite pharmaceutical market score using a **hybrid PCA + Entropy Weight Method**, classifies each municipality into four market tiers (A–D), and exposes this intelligence through:

- A **FastAPI REST backend** with spatial queries, ranking, filtering, site optimization, and trade area (Huff gravity model) simulation.
- A **Next.js + Deck.gl interactive dashboard** with a WebGL map, real-time score visualization, and Claude-powered AI narrative generation.
- A **LangChain self-healing pipeline** that uses DuckDuckGo web search and Claude to interpolate missing healthcare establishment counts when DataSUS APIs are unavailable.

**Primary business value:** A pharmaceutical site-selection analyst can open the web dashboard, browse the color-coded Brazil map, filter by state, click any municipality to see its demographic/economic/healthcare KPIs and tier classification, run a Huff gravity model trade area simulation, and receive a full AI-generated strategic narrative — all without writing a single query.

---

## 2. Technology Stack & Architecture

### 2.1 Data Ingestion — Agent 1 (Python ETL)

**Container:** `agent1` (Python 3.11-slim, Docker)
**Entry point:** `agent1/main.py`
**Run command:** `docker compose up --build agent1`

#### Language & Libraries

| Library | Version | Purpose |
|---|---|---|
| `pandas` | 2.1.4 | DataFrame manipulation, data cleaning |
| `sqlalchemy` | 2.0.25 | ORM + raw SQL execution |
| `psycopg2-binary` | 2.9.9 | PostgreSQL adapter |
| `geopandas` / `shapely` / `pyproj` | latest | Coordinate extraction from GeoJSON polygons |
| `scikit-learn` | 1.3.2 | `PCA`, `MinMaxScaler`, `SimpleImputer` |
| `scipy` | latest | Statistical utilities |
| `requests` / `tenacity` | latest | HTTP calls with exponential-backoff retry |
| `loguru` | 0.7.2 | Structured logging to stdout + `data/pipeline.log` |
| `anthropic` | 0.52.0 | Claude API (used in self-healing agents) |
| `langchain` / `langchain-anthropic` / `langchain-community` | ≥0.3 | ReAct agent orchestration |
| `duckduckgo-search` | ≥6.3.7 | Web search tool for self-healing agents |

#### Pipeline Modules (`agent1/pipeline/`)

| Module | Data source | Key output columns |
|---|---|---|
| `municipios.py` | IBGE Municipios API + IBGE Malhas GeoJSON | `codigo_ibge`, `nome`, `uf`, `regiao`, `mesorregiao`, `microrregiao`, `latitude`, `longitude`, `area_km2` |
| `demograficos.py` | IBGE SIDRA (Census 2022) | `populacao_total`, `populacao_urbana`, `populacao_rural`, `taxa_urbanizacao`, `populacao_alvo` (30–64 yrs), `pct_populacao_alvo`, `indice_envelhecimento`, age bands (`pop_0_4` … `pop_65_plus`) |
| `cnes.py` | DataSUS CNES API | `farmacias`, `farmacias_magistrais`, `clinicas`, `hospitais`, `laboratorios`, `consultorios_medicos`, `consultorios_odonto`, `ubs_upa`, `total_estabelecimentos`, `farmacias_por_10k`, `estabelecimentos_saude_por_10k`, `leitos_total`, `leitos_sus` |
| `economicos.py` | IPEADATA (PIB, IDH), ANS (health insurance), Receita Federal (CNPJs) | `pib_per_capita`, `pib_total`, `idh`, `beneficiarios_planos`, `cobertura_planos_pct`, `cnpjs_farmacias`, `cnpjs_saude`, `cnpjs_distribuidores`, `empregos_saude` |
| `scores.py` | PostgreSQL (joins above tables) | `score_total`, `score_demografico`, `score_infraestrutura_saude`, `score_economico`, `score_logistico`, `score_competitividade`, `tier`, `ranking_nacional`, `ranking_estadual`, `pca_component_1/2/3` |

#### External APIs

| API | Endpoint pattern | Data |
|---|---|---|
| IBGE Municipios | `https://servicodados.ibge.gov.br/api/v1/localidades/municipios` | All 5,570 municipalities (code, name, UF, region hierarchy) |
| IBGE Malhas | `https://servicodados.ibge.gov.br/api/v3/malhas/municipios/{code}?formato=application/vnd.geo+json` | GeoJSON polygon per municipality (centroid used for coordinates) |
| IBGE SIDRA | `https://servicodados.ibge.gov.br/api/v3/agregados/{id}/periodos/{year}/variaveis/{vars}?...` | Demographic census tables (aggregate IDs: 4714, 4716, etc.) |
| DataSUS CNES | `https://apidadosabertos.saude.gov.br/cnes/estabelecimentos?co_uf={uf}&...` | Healthcare establishment registry by UF |
| IPEADATA | `http://www.ipeadata.gov.br/api/odata4/Metadados('IBRE_PIBPERCAP12')/Valores` | PIB per capita series |
| ANS | `https://dadosabertos.ans.gov.br/api/3/action/datastore_search?resource_id=...` | Health insurance beneficiary counts |

---

### 2.2 Self-Healing AI — Agents 3 & 4 (LangChain)

**Location:** `agent1/pipeline/healers/`
**Activation:** Called from `cnes.py` when CNES API returns zero counts for a municipality with non-trivial population.
**Cost control:** Env var `HEALER_MAX_PER_RUN` (default: 20 municipalities per pipeline run); `HEALER_ENABLED=false` disables entirely.

#### Architecture (Four-Component Flow)

```
CNES raw counts (zeros detected)
          │
          ▼
  SuspiciousZeroValidator        ◄── validator.py
  (population-scaled heuristic)
          │ anomaly detected
          ▼
  DataResearcher (Agent 3)       ◄── researcher.py
  LangChain ReAct + DuckDuckGo
  → web search for pharmacy counts
  → returns: (estimated_count, [sources])
          │
          ▼
  DataVerifier (Agent 4)         ◄── verifier.py
  Claude cross-reference check
  → returns: (final_value, confidence %, "accept"|"flag")
          │
    ┌─────┴──────────────────┐
    │ confidence ≥ 80%        │ confidence < 80%
    ▼                         ▼
  Accept healed value      Flag for manual review
  (overwrite 0 in DB)      (keep original 0)
```

#### Healing Priority

The healer targets only the highest-value fields (in priority order):
1. `farmacias` — primary pharmaceutical market indicator
2. `clinicas` — key correlate for pharmaceutical demand
3. `hospitais` — hospital pharmacy demand
4. `laboratorios` — diagnostic/reagent demand

#### Key Classes

```python
# orchestrator.py
class HealingOrchestrator:
    def heal_municipality(codigo_ibge, nome, uf, counts, population, api_failed) -> HealingResult
    def heal_batch(municipalities: list[dict], api_failed_ufs: set) -> dict[str, dict]

class HealingResult:
    healed: dict          # final counts (may differ from original)
    healed_fields: list   # which fields were changed
    confidence_scores: dict[str, float]
    was_healed: bool
    flagged_for_review: list
```

---

### 2.3 Backend API — Phase 2 (FastAPI)

**Container:** `api` (Python 3.11-slim, Docker)
**Port:** 8000
**Entry point:** `api/main.py` → `uvicorn main:app --reload`
**Interactive docs:** `http://localhost:8000/docs`

#### Libraries

| Library | Version | Purpose |
|---|---|---|
| `fastapi` | 0.110.0 | ASGI framework |
| `uvicorn[standard]` | 0.29.0 | ASGI server (with `watchfiles` hot reload) |
| `sqlalchemy` | 2.0.25 | Sync connection pool (`pool_size=10`, `max_overflow=20`) |
| `psycopg2-binary` | 2.9.9 | PostgreSQL adapter |
| `redis` | 5.0.3 | Redis client (singleton via `cache.py`) |
| `anthropic` | ≥0.35.0 | Claude Sonnet 4.6 API |
| `pydantic` | 2.6.4 | Request/response validation (v2) |
| `loguru` | 0.7.2 | Structured logging |
| `tenacity` | 8.2.3 | Retry logic |

#### Composite Scoring (Hybrid PCA + EWM)

The scoring engine (`agent1/pipeline/scores.py`) uses a two-stage approach:

**Stage 1 — Feature Engineering:**
- 14 input features across three domains (demographic, health infrastructure, economic)
- Missing values imputed with column median (`SimpleImputer(strategy="median")`)
- All-NaN columns dropped before imputation to avoid length mismatches
- Features normalized to [0, 1] with `MinMaxScaler`

**Stage 2 — Hybrid Weights (α = 0.5):**

```
W_hybrid = 0.5 × W_PCA + 0.5 × W_entropy

W_PCA     = |loadings of PC1| / sum(|loadings of PC1|)
W_entropy = d / sum(d)   where d = 1 - E  (information utility)
             E = -k * Σ(p × log(p))        (Shannon entropy per feature)
             k = 1 / log(n)                 (n = number of municipalities)
```

**Stage 3 — Score Calculation:**
```
score_total = Σ(X_normalized × W_hybrid) × 100   → range [0, 100]
```

**Tier Thresholds** (percentile-based, recalculated on each pipeline run):

| Tier | Percentile | Interpretation |
|---|---|---|
| **A** | ≥ 75th | Highest priority — large urban, high purchasing power |
| **B** | 50th–75th | Good opportunity — growth markets |
| **C** | 25th–50th | Secondary — selective targeting |
| **D** | < 25th | Low priority — small/rural, low economic capacity |

#### Huff Gravity Model (`GET /tradearea/{lat}/{lon}`)

The trade area endpoint implements the **Huff Gravity Model** (probabilistic retail trade area):

```
Attractiveness(i) = Score(i) / Distance(i)^β

Probability(i) = Attractiveness(i) / Σ Attractiveness(j)   for all j in radius

EstimatedCustomers(i) = Probability(i) × PopulaçãoAlvo(i)
```

Parameters:
- `raio_km` (default: 200, max: 1000) — search radius
- `beta` (default: 2.0, range: 0.5–5.0) — distance decay exponent

Spatial computation uses PostGIS `ST_DWithin` and `ST_Distance` on `geography` type (great-circle distances in metres).

#### Redis Cache TTLs

| Endpoint | TTL | Rationale |
|---|---|---|
| `GET /stats` | 5 minutes | Pipeline status changes frequently |
| `GET /ranking` | 10 minutes | Frequently queried, short-lived |
| `GET /score/{id}` | 1 hour | Score changes only on pipeline re-run |
| `GET /municipios/{id}` | 1 hour | Detail data stable |
| `POST /insights/{id}` | 24 hours | Expensive LLM call; narrative doesn't change |
| `POST /insights/tradearea` | 24 hours | Same reasoning; keyed by (ibge_code, radius_km) |

---

### 2.4 Frontend — Next.js + Deck.gl

**Technology:** Next.js 16.1.6 + React 19.2.3 + TypeScript 5 + Tailwind CSS 4
**Map:** Deck.gl 9.2.10 (WebGL layers) + MapLibre 5.19.0 (raster tiles) + react-map-gl 8.1.0
**Base map tiles:** OpenFreeMap Positron (`https://tiles.openfreemap.org/styles/positron`)
**Icons:** lucide-react
**No SSR for Map:** Dynamic import (`next/dynamic` with `ssr: false`) — WebGL requires browser.

#### Component Tree

```
app/page.tsx (Dashboard)
├── RankingSidebar           ← left panel
│   • Top 20 ranked municipalities
│   • UF filter dropdown (27 states)
│   • Tier badges, score bars
│   • Skeleton loaders
│
├── PharmaSiteMap (Map.tsx)  ← center canvas (WebGL)
│   • GeoJsonLayer   — IBGE municipality/state borders
│   • ScatterplotLayer — scatter fallback (no GeoJSON)
│   • ArcLayer       — trade area flow lines
│   • ScatterplotLayer — trade area bubbles
│   • Hover tooltips
│   • Mercator fit-bounds (custom: no external dep)
│
└── CityDetailPanel          ← right slide-in panel
    • Header: name, tier badge, national rank
    • 2×2 stat cards (population, PIB, pharmacies, insurance)
    • Health infrastructure grid (6 facility types)
    • Score breakdown bars (5 component scores)
    • Trade Area section (top 5 attracted cities)
    • AI Insights (Claude — municipal narrative)
    • Trade Area Strategic Analysis (Claude — site strategy)
```

#### Score → Color Mapping (`lib/colors.ts`)

```
Score 0   → purple  [128, 0, 128]
Score 25  → blue    [0, 0, 255]
Score 50  → teal    [0, 200, 200]
Score 75  → amber   [255, 165, 0]
Score 100 → red     [255, 0, 0]
Score null→ slate   [100, 116, 139]  (no data)
```

Linear interpolation (`lerp`) between breakpoints.

#### Map Border Color Logic

| Condition | Color | RGBA |
|---|---|---|
| Selected municipality | White | `[255, 255, 255, 255]` |
| UF view (state mesh loaded) — non-selected | Cyan | `[56, 189, 248, 80]` |
| National view (Brazil mesh) — non-selected | Dark navy | `[10, 25, 65, 110]` |

#### Zoom Behavior

| Trigger | Action |
|---|---|
| UF filter set (sidebar) | Load IBGE state mesh → `flyToBbox(all UF features)` |
| City clicked in UF view | `flyToBbox(all UF features)` — zoom to entire state |
| City clicked in national view | Auto-load IBGE state mesh for city's UF → `flyToBbox(state features)` |
| UF filter cleared | Restore Brazil mesh → fly to `{lon: -50, lat: -14, zoom: 4}` |

---

### 2.5 Infrastructure (Docker Compose + PostGIS)

**File:** `docker-compose.yml` at project root.

| Service | Image | Port | Notes |
|---|---|---|---|
| `db` | `postgis/postgis:15-3.4-alpine` | 5432 | PostGIS spatial extensions; persistent `pgdata` volume; health-checked |
| `redis` | `redis:7-alpine` | 6379 | In-memory cache; no persistence (ephemeral between restarts) |
| `agent1` | `python:3.11-slim` (built) | — | Runs pipeline once and exits; depends on `db` healthy |
| `api` | `python:3.11-slim` (built) | 8000 | FastAPI; hot-reload; depends on `db` healthy |

All services share a Docker network. Environment variables loaded from `.env` at project root (see `.env.example`).

**Required `.env` keys:**

```env
POSTGRES_DB=pharmasite
POSTGRES_USER=pharma
POSTGRES_PASSWORD=<secret>
DATABASE_URL=postgresql://pharma:<secret>@db:5432/pharmasite
REDIS_URL=redis://redis:6379/0
ANTHROPIC_API_KEY=sk-ant-...        # required for /insights endpoints
HEALER_ENABLED=true                  # optional, default true
HEALER_MAX_PER_RUN=20               # optional, default 20
```

---

## 3. Core Workflows

### 3.1 Workflow A: Data Ingestion & Scoring

```
docker compose up -d db redis
docker compose up --build agent1
```

**Step-by-step execution** (`agent1/main.py`):

```
1. DB INIT      → shared/init.sql creates tables if not exist
                  (municipios, demograficos, estabelecimentos_saude,
                   indicadores_economicos, scores, pipeline_log)

2. MUNICIPIOS   → pipeline/municipios.py
   a. Fetch all 5,570 municipalities from IBGE API
   b. For each: resolve mesorregião/microrregião hierarchy
      (None guard: None microrregiao → fallback to regiao-imediata → UF)
   c. Fetch GeoJSON centroid per municipality for lat/lon
   d. Upsert into `municipios` (ON CONFLICT DO UPDATE)

3. DEMOGRAFICOS → pipeline/demograficos.py
   a. Fetch Census 2022 aggregates from IBGE SIDRA per table
   b. Compute derived cols: taxa_urbanizacao, pct_populacao_alvo
      (zero-division guard: .replace(0, float('nan')))
   c. Insert into `demograficos`

4. CNES         → pipeline/cnes.py
   a. Fetch health establishments per UF from DataSUS API
   b. If API returns HTTP error: trigger HealingOrchestrator for that UF
   c. Map CNES type codes → facility categories
   d. Calculate per-10k densities
   e. Insert into `estabelecimentos_saude`

5. ECONOMICOS   → pipeline/economicos.py
   a. Fetch PIB + IDH from IPEADATA
   b. Fetch insurance beneficiaries from ANS
   c. Fetch CNPJ counts from Receita Federal
   d. Insert into `indicadores_economicos`

6. SCORES       → pipeline/scores.py
   a. JOIN all 4 tables (LATERAL to avoid Cartesian product from duplicates)
   b. Build 14-feature matrix X
   c. Drop all-NaN columns; impute remaining NaN → median
   d. MinMaxScale to [0,1]
   e. Calculate entropy weights W_entropy
   f. Run PCA (skipped if all features constant) → W_PCA from PC1 loadings
   g. Combine: W_hybrid = 0.5 × W_PCA + 0.5 × W_entropy
   h. score_total = Σ(X × W_hybrid) × 100
   i. Calculate 5 component scores (demographic, health, economic, logistic, comp.)
   j. Classify tier (quantile-based: A/B/C/D)
   k. Compute national + state rankings
   l. Insert into `scores`

7. LOG          → Write pipeline_log entry with counts and status
```

**Expected output:** 5,570 rows in each table. Pipeline exits (container stops).

---

### 3.2 Workflow B: LLM Self-Healing Pipeline

Triggered automatically during step 4 (CNES) when DataSUS returns errors or suspicious zero counts.

```
cnes.py detects API failure for UF "SP"
         │
         ▼
HealingOrchestrator.heal_batch(municipalities=[...], api_failed_ufs={"SP"})
         │
         │  Sort by population DESC (heal high-impact cities first)
         │  Process up to HEALER_MAX_PER_RUN=20 municipalities
         │
         ▼ (for each municipality)
SuspiciousZeroValidator.is_suspicious(codigo_ibge, nome, uf, counts, population)
  Rule: population > 50,000 AND farmacias == 0 AND api_failed == True
  → returns AnomalyReport if suspicious, None if healthy
         │
    (if anomaly)
         ▼
DataResearcher (LangChain ReAct Agent)
  Tools: DuckDuckGo web search
  Prompt: "How many pharmacies are in {nome}, {uf}, Brazil?"
  Chain: search → parse → estimate → return (int, [source_urls])
         │
         ▼
DataVerifier (Claude cross-reference)
  Prompt: "Verify this estimate of {n} pharmacies for {nome} (pop {pop})"
  → returns (final_value: int, confidence: float 0-1, disposition: "accept"|"flag")
         │
    ┌────┴───────────────────────┐
    │ confidence >= 0.80          │ confidence < 0.80
    ▼                             ▼
healed["farmacias"] = final_value  flagged_for_review.append("farmacias")
result.was_healed = True           (zero kept in DB)
         │
         ▼
Recalculate total_estabelecimentos
Log: "✅ Campinas-SP: healed {'farmacias': 45}"
```

**Cost guardrails:**
- Maximum 20 municipalities per run (`HEALER_MAX_PER_RUN`)
- Maximum 4 fields per municipality (`HEALABLE_FIELDS`)
- Lazy agent initialization (no LLM call if no anomaly)
- `HEALER_ENABLED=false` bypasses the entire subsystem
- 80% confidence threshold prevents low-quality estimates from entering the DB

---

### 3.3 Workflow C: Trade Area Intelligence (UI → AI)

Full end-to-end flow from user interaction to Claude narrative:

```
USER: Clicks "Simular Abertura de Loja" on São Paulo detail panel
         │
         ▼
CityDetailPanel.handleSimulate()
  → api.getTradeArea(lat=-23.55, lon=-46.63, raioKm=200)
  → GET /tradearea/-23.55/-46.63?raio_km=200&beta=2.0
         │
         ▼
FastAPI: tradearea.py
  PostGIS query:
    ST_DWithin(geom, point, 200_000 meters)   ← finds all municipalities in 200km radius
    ST_Distance → distance_km                  ← great-circle distance
  Huff model:
    attractiveness = score / distance^2.0
    probability    = attractiveness / Σ(attractiveness)
    customers      = probability × populacao_alvo
  Sort by probability DESC → return top results
         │
         ▼
Frontend renders:
  • Arc layers on map (blue → amber arcs showing customer flows)
  • Bubble layer (amber dots sized by probability)
  • "Área de Influência — Top 5 cidades atraídas" list in panel
  • NEW button appears: "Gerar Análise Estratégica da Área"

USER: Clicks "Gerar Análise Estratégica da Área"
         │
         ▼
CityDetailPanel.handleTradeAreaInsights()
  → api.postTradeAreaInsights({
      codigo_ibge: "3550308",
      center_lat: -23.55, center_lon: -46.63,
      radius_km: 200,
      total_estimated_customers: 84320,
      items: [{ nome: "Guarulhos", distance_km: 28.4, probability: 0.12, ... }, ...]
    })
  → POST /insights/tradearea
         │
         ▼
FastAPI: tradearea_insights.py
  1. Check Redis cache (key: "tradearea_insights:3550308:200")
     → cache hit: return immediately (24h TTL)
     → cache miss: continue
  2. Fetch municipality context from DB (LATERAL JOINs for latest row)
  3. Build Claude prompt (5 strategic sections):
     • Market Viability — population, score, economic indicators
     • Trade Area Analysis — top 10 attracted municipalities with probabilities
     • Cannibalization Risks — existing farmacias density
     • Sizing Recommendation — revenue potential from estimated customers
     • Strategic Decision — ABRIR / ABRIR COM RESSALVAS / NÃO ABRIR
  4. Call claude-sonnet-4-6 (MAX_TOKENS=1600, temperature=0.3)
  5. Cache result in Redis (24h)
  6. Return { narrative: "...", model_used: "...", generated_at: "..." }
         │
         ▼
Frontend:
  tradeAreaNarrative state set
  <MarkdownBlock text={narrative} /> renders:
    ## headings → <h3> / <h4>
    **bold** → <strong>
    - bullets → styled div rows
    --- → <hr>
  Narrative displayed in scrollable amber-themed panel
```

---

## 4. Database Schema

**Database:** PostgreSQL 15 with PostGIS 3.4
**Bootstrap:** `shared/init.sql` (run on first `docker compose up`)

### Tables

```sql
-- Core geographic table
CREATE TABLE municipios (
    id              SERIAL PRIMARY KEY,
    codigo_ibge     VARCHAR(7) UNIQUE NOT NULL,
    nome            TEXT NOT NULL,
    uf              CHAR(2) NOT NULL,
    regiao          TEXT,
    mesorregiao     TEXT,
    microrregiao    TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    area_km2        DOUBLE PRECISION,
    geom            GEOMETRY(POINT, 4326),  -- PostGIS spatial index
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Census 2022 demographics
CREATE TABLE demograficos (
    id                      SERIAL PRIMARY KEY,
    codigo_ibge             VARCHAR(7) NOT NULL,
    populacao_total         INTEGER,
    populacao_urbana        INTEGER,
    populacao_rural         INTEGER,
    taxa_urbanizacao        NUMERIC(5,2),
    populacao_alvo          INTEGER,          -- 30-64 years (pharmaceutical core market)
    pct_populacao_alvo      NUMERIC(5,2),
    renda_per_capita        NUMERIC(12,2),
    indice_envelhecimento   NUMERIC(8,2),
    pop_0_4 ... pop_65_plus INTEGER,          -- age band columns
    ano_referencia          INTEGER,
    created_at              TIMESTAMP DEFAULT NOW()
    -- Note: No UNIQUE on codigo_ibge — multiple pipeline runs create duplicate rows
    -- Use LATERAL JOIN (ORDER BY id DESC LIMIT 1) to get latest
);

-- DataSUS CNES healthcare establishments
CREATE TABLE estabelecimentos_saude (
    id                              SERIAL PRIMARY KEY,
    codigo_ibge                     VARCHAR(7) NOT NULL,
    farmacias                       INTEGER DEFAULT 0,
    farmacias_magistrais            INTEGER DEFAULT 0,
    consultorios_medicos            INTEGER DEFAULT 0,
    consultorios_odonto             INTEGER DEFAULT 0,
    laboratorios                    INTEGER DEFAULT 0,
    clinicas                        INTEGER DEFAULT 0,
    hospitais                       INTEGER DEFAULT 0,
    ubs_upa                         INTEGER DEFAULT 0,
    total_estabelecimentos          INTEGER DEFAULT 0,
    farmacias_por_10k               NUMERIC(8,2),
    estabelecimentos_saude_por_10k  NUMERIC(8,2),
    leitos_total                    INTEGER,
    leitos_sus                      INTEGER,
    ano_referencia                  INTEGER,
    created_at                      TIMESTAMP DEFAULT NOW()
);

-- Economic indicators
CREATE TABLE indicadores_economicos (
    id                          SERIAL PRIMARY KEY,
    codigo_ibge                 VARCHAR(7) NOT NULL,
    pib_per_capita              NUMERIC(14,2),
    pib_total                   NUMERIC(20,2),
    cnpjs_farmacias             INTEGER,
    cnpjs_saude                 INTEGER,
    cnpjs_instrumentos_medicos  INTEGER,
    cnpjs_distribuidores        INTEGER,
    beneficiarios_planos        INTEGER,
    cobertura_planos_pct        NUMERIC(6,2),
    idh                         NUMERIC(5,3),
    empregos_saude              INTEGER,
    ano_referencia              INTEGER,
    created_at                  TIMESTAMP DEFAULT NOW()
);

-- Composite scores (output of scoring engine)
CREATE TABLE scores (
    id                          SERIAL PRIMARY KEY,
    codigo_ibge                 VARCHAR(7) NOT NULL,
    score_demografico           NUMERIC(6,2),
    score_infraestrutura_saude  NUMERIC(6,2),
    score_economico             NUMERIC(6,2),
    score_logistico             NUMERIC(6,2),   -- currently fixed at 50.0
    score_competitividade       NUMERIC(6,2),   -- currently fixed at 50.0
    score_total                 NUMERIC(6,2),
    tier                        CHAR(1),         -- A / B / C / D
    ranking_nacional            INTEGER,
    ranking_estadual            INTEGER,
    pca_component_1             NUMERIC(10,6),
    pca_component_2             NUMERIC(10,6),
    pca_component_3             NUMERIC(10,6),
    created_at                  TIMESTAMP DEFAULT NOW()
    -- Note: ON CONFLICT DO NOTHING without UNIQUE constraint
    -- Multiple pipeline runs accumulate rows
);

-- Pipeline audit log
CREATE TABLE pipeline_log (
    id                      SERIAL PRIMARY KEY,
    etapa                   TEXT,
    status                  TEXT,
    municipios_processados  INTEGER,
    mensagem                TEXT,
    started_at              TIMESTAMP,
    finished_at             TIMESTAMP
);
```

**Spatial index:** `CREATE INDEX municipios_geom_idx ON municipios USING GIST(geom);`

---

## 5. API Endpoint Reference

Base URL: `http://localhost:8000`
Interactive docs: `http://localhost:8000/docs`

| Method | Path | Description | Cache |
|---|---|---|---|
| `GET` | `/health` | Liveness check | — |
| `GET` | `/municipios` | Paginated list (filters: uf, regiao, tier, q, page, limit) | — |
| `GET` | `/municipios/{codigo_ibge}` | Full detail (demographics + establishments + economics + score) | 1h |
| `GET` | `/score/{codigo_ibge}` | Score + tier + full breakdown | 1h |
| `GET` | `/ranking` | Top-N ranked (filters: uf, tier, regiao, limit ≤ 500) | 10min |
| `POST` | `/optimize` | Filter + rank by custom criteria; optional P-Median clustering | — |
| `GET` | `/tradearea/{lat}/{lon}` | Huff gravity model (params: raio_km, beta) | — |
| `GET` | `/stats` | Pipeline summary + data quality | 5min |
| `POST` | `/insights/{codigo_ibge}` | Claude PT-BR market narrative | 24h |
| `POST` | `/insights/tradearea` | Claude strategic site-selection narrative | 24h |

---

## 6. Key Code Signatures & Payloads

### 6.1 `TradeAreaInsightsRequest` (POST body)

```python
# api/schemas.py
class TradeAreaInsightsRequest(BaseModel):
    codigo_ibge: str          # 7-digit IBGE code of the center municipality
    center_lat: float         # latitude of proposed store
    center_lon: float         # longitude of proposed store
    radius_km: float          # search radius used in Huff simulation
    total_estimated_customers: Optional[float] = None   # Σ probability × pop_alvo
    items: List[TradeAreaInsightsItem]   # per-municipality Huff results

class TradeAreaInsightsItem(BaseModel):
    codigo_ibge: str
    nome: str
    uf: str
    distance_km: float
    probability: float
    estimated_customers: Optional[float] = None
    model_config = {"extra": "ignore"}   # drops lat/lon/attractiveness silently
```

**Example JSON payload:**
```json
{
  "codigo_ibge": "3550308",
  "center_lat": -23.5505,
  "center_lon": -46.6333,
  "radius_km": 200,
  "total_estimated_customers": 84320.0,
  "items": [
    { "codigo_ibge": "3518800", "nome": "Guarulhos", "uf": "SP",
      "distance_km": 28.4, "probability": 0.1217, "estimated_customers": 10278.0 },
    { "codigo_ibge": "3509502", "nome": "Campinas", "uf": "SP",
      "distance_km": 96.1, "probability": 0.0843, "estimated_customers": 7120.0 }
  ]
}
```

---

### 6.2 PCA + Entropy Weight Output (example log)

When the scoring engine runs successfully with full data:

```
  Pesos Híbridos Calculados:
  Feature                  W_Entropy   W_PCA    W_Hybrid
  populacao_total          0.1423      0.1812   0.1618
  populacao_alvo           0.1389      0.1743   0.1566
  farmacias                0.0981      0.1204   0.1093
  pib_per_capita           0.0876      0.0923   0.0900
  renda_per_capita         0.0834      0.0891   0.0863
  idh                      0.0712      0.0754   0.0733
  cobertura_planos_pct     0.0634      0.0681   0.0658
  taxa_urbanizacao         0.0589      0.0612   0.0601
  clinicas                 0.0534      0.0421   0.0478
  laboratorios             0.0489      0.0398   0.0444
  ...
  PCA variância explicada: ['41.2%', '18.7%', '11.3%']
```

---

### 6.3 Huff Model — `GET /tradearea/{lat}/{lon}`

**Example call:** `GET /tradearea/-23.5505/-46.6333?raio_km=200&beta=2.0`

**Core computation:**
```python
# api/routers/tradearea.py (simplified)
for r in rows:
    d_km = max(float(r['distance_km']), 1.0)       # floor at 1 km
    score = float(r['score_total'] or 1.0)
    attractiveness = score / (d_km ** beta)          # Huff gravity
    ...
prob = attractiveness / total_attractiveness
customers = prob * populacao_alvo
```

**Example response:**
```json
{
  "center_lat": -23.5505,
  "center_lon": -46.6333,
  "radius_km": 200.0,
  "total_estimated_customers": 84320.0,
  "results": [
    { "nome": "São Paulo", "uf": "SP", "distance_km": 0.0,
      "attractiveness": 99.0, "probability": 0.3421,
      "estimated_customers": 28854.0 },
    { "nome": "Guarulhos", "uf": "SP", "distance_km": 28.4,
      "attractiveness": 1.227, "probability": 0.1217,
      "estimated_customers": 10278.0 }
  ]
}
```

---

### 6.4 `OptimizeRequest` — P-Median Clustering

```python
# api/schemas.py
class OptimizeRequest(BaseModel):
    min_score:     Optional[float] = None   # 0–100
    min_populacao: Optional[int]   = None
    ufs:           Optional[List[str]] = None   # ["SP", "RJ", "MG"]
    tier:          Optional[List[str]] = None   # ["A", "B"]
    limit:         int = 20                     # max 500
    n_pontos:      Optional[int] = None         # 1–50: triggers P-Median clustering
```

When `n_pontos` is provided, the optimize endpoint runs a **P-Median clustering** algorithm on the filtered result set, returning `n_pontos` optimal center locations (minimizing weighted distance to all demand points in the cluster) plus cluster assignments for each municipality.

---

## 7. Known Limitations & Operational Notes

### Data Quality (as of 2026-03-03)

| API | Status | Impact |
|---|---|---|
| IBGE SIDRA (demographics) | Intermittent HTTP errors | `demograficos` table may have NULL columns |
| DataSUS CNES | HTTP 503 for all states | `farmacias` etc. default to 0; healer compensates |
| IPEADATA (PIB, IDH) | No response | `pib_per_capita`, `idh` = NULL |
| ANS (insurance) | 404 | `cobertura_planos_pct` = NULL |

When external APIs are down, PCA cannot distinguish municipalities (all features = 0/constant → zero variance). The scoring engine handles this gracefully:
- PCA is skipped with a `logger.warning`
- Hybrid weights fall back to `w_entropy` (which also becomes zero for constant features)
- All municipalities receive `score_total = 0.0` and land in **Tier A** (equal rank)
- Real scores require live API data

### Duplicate Rows

`demograficos`, `estabelecimentos_saude`, and `indicadores_economicos` lack `UNIQUE` constraints on `codigo_ibge`. Multiple pipeline runs create duplicate rows. All queries that JOIN these tables use `LEFT JOIN LATERAL (... ORDER BY id DESC LIMIT 1)` to safely fetch the latest row. Before re-running the pipeline, reset with:

```bash
docker compose down -v     # drops pgdata volume
docker compose up -d db redis
docker compose up --build agent1
```

### API Service Activation

The `api` service in `docker-compose.yml` is defined but may need to be started explicitly:

```bash
docker compose up --build api
```

Requires `ANTHROPIC_API_KEY` in `.env` for the `/insights` endpoints.

### Frontend Environment

```bash
cd frontend
npm install
NEXT_PUBLIC_API_URL=http://127.0.0.1:8000 npm run dev
# → http://localhost:3000
```

### Scores Table Accumulation

`INSERT INTO scores ... ON CONFLICT DO NOTHING` without a UNIQUE constraint means multiple runs accumulate rows rather than upsert. The API uses `LATERAL (ORDER BY id DESC LIMIT 1)` to always serve the latest score. To add a proper upsert constraint:

```sql
ALTER TABLE scores ADD CONSTRAINT scores_ibge_unique UNIQUE (codigo_ibge);
-- Then change INSERT to ON CONFLICT (codigo_ibge) DO UPDATE SET ...
```

"""
Pydantic v2 response schemas for PHARMASITE Intelligence API.

Convention: all DB-nullable columns are typed Optional[X] = None.
Null fields are returned as null in JSON — never coerced to 0 or "".
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ── Sub-models (nested in detail endpoints) ───────────────────────────────────

class ScoreBreakdown(BaseModel):
    score_demografico: Optional[float] = None
    score_infraestrutura_saude: Optional[float] = None
    score_economico: Optional[float] = None
    score_logistico: Optional[float] = None       # haversine-based, 0–100 (100 = Campinas HQ)
    score_competitividade: Optional[float] = None
    distance_campinas_km: Optional[float] = None  # km from Campinas HQ (B2B logistics)
    pca_component_1: Optional[float] = None
    pca_component_2: Optional[float] = None
    pca_component_3: Optional[float] = None


class DemograficosOut(BaseModel):
    populacao_total: Optional[int] = None
    populacao_urbana: Optional[int] = None
    populacao_rural: Optional[int] = None
    taxa_urbanizacao: Optional[float] = None
    populacao_alvo: Optional[int] = None
    pct_populacao_alvo: Optional[float] = None
    renda_per_capita: Optional[float] = None
    indice_envelhecimento: Optional[float] = None
    pop_0_4: Optional[int] = None
    pop_5_14: Optional[int] = None
    pop_15_29: Optional[int] = None
    pop_30_44: Optional[int] = None
    pop_45_64: Optional[int] = None
    pop_65_plus: Optional[int] = None
    ano_referencia: Optional[int] = None


class EstabelecimentosOut(BaseModel):
    farmacias: Optional[int] = None
    farmacias_magistrais: Optional[int] = None
    consultorios_medicos: Optional[int] = None
    consultorios_odonto: Optional[int] = None
    laboratorios: Optional[int] = None
    clinicas: Optional[int] = None
    hospitais: Optional[int] = None
    ubs_upa: Optional[int] = None
    total_estabelecimentos: Optional[int] = None
    farmacias_por_10k: Optional[float] = None
    estabelecimentos_saude_por_10k: Optional[float] = None
    leitos_total: Optional[int] = None
    leitos_sus: Optional[int] = None
    ano_referencia: Optional[int] = None


class EconomicosOut(BaseModel):
    pib_per_capita: Optional[float] = None
    pib_total: Optional[float] = None
    cnpjs_farmacias: Optional[int] = None
    cnpjs_saude: Optional[int] = None
    cnpjs_instrumentos_medicos: Optional[int] = None
    cnpjs_distribuidores: Optional[int] = None
    beneficiarios_planos: Optional[int] = None
    cobertura_planos_pct: Optional[float] = None
    idh: Optional[float] = None
    empregos_saude: Optional[int] = None
    ano_referencia: Optional[int] = None


# ── /municipios ───────────────────────────────────────────────────────────────

class MunicipioListItem(BaseModel):
    """Lightweight item for list endpoint — no nested sub-data."""
    codigo_ibge: str
    nome: str
    uf: str
    regiao: Optional[str] = None
    mesorregiao: Optional[str] = None
    microrregiao: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    score_total: Optional[float] = None
    tier: Optional[str] = None
    ranking_nacional: Optional[int] = None


class MunicipioListResponse(BaseModel):
    total: int
    page: int
    limit: int
    results: List[MunicipioListItem]


class MunicipioDetail(BaseModel):
    """Full detail — all joined tables as nested objects."""
    codigo_ibge: str
    nome: str
    uf: str
    regiao: Optional[str] = None
    mesorregiao: Optional[str] = None
    microrregiao: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    area_km2: Optional[float] = None
    score_total: Optional[float] = None
    tier: Optional[str] = None
    ranking_nacional: Optional[int] = None
    ranking_estadual: Optional[int] = None
    demograficos: Optional[DemograficosOut] = None
    estabelecimentos: Optional[EstabelecimentosOut] = None
    economicos: Optional[EconomicosOut] = None
    score: Optional[ScoreBreakdown] = None


# ── /score ────────────────────────────────────────────────────────────────────

class ScoreResponse(BaseModel):
    codigo_ibge: str
    nome: str
    uf: str
    score_total: Optional[float] = None
    tier: Optional[str] = None
    ranking_nacional: Optional[int] = None
    ranking_estadual: Optional[int] = None
    breakdown: ScoreBreakdown


# ── /ranking ──────────────────────────────────────────────────────────────────

class RankingItem(BaseModel):
    ranking_nacional: Optional[int] = None
    codigo_ibge: str
    nome: str
    uf: str
    regiao: Optional[str] = None
    score_total: Optional[float] = None
    score_logistico: Optional[float] = None
    tier: Optional[str] = None
    populacao_total: Optional[int] = None
    pib_per_capita: Optional[float] = None
    farmacias: Optional[int] = None
    idh: Optional[float] = None
    distance_campinas_km: Optional[float] = None


class RankingResponse(BaseModel):
    total: int
    filters_applied: Dict[str, Any]
    results: List[RankingItem]


# ── /optimize ─────────────────────────────────────────────────────────────────

class OptimizeRequest(BaseModel):
    min_score: Optional[float] = Field(default=None, ge=0, le=100)
    min_populacao: Optional[int] = Field(default=None, ge=0)
    ufs: Optional[List[str]] = Field(default=None, max_length=27)
    tier: Optional[List[str]] = Field(default=None)
    max_distance_km: Optional[int] = Field(default=None, ge=1, description="Filtrar municípios dentro do raio (km) do CD em Campinas-SP")
    limit: int = Field(default=20, ge=1, le=500)
    n_pontos: Optional[int] = Field(default=None, ge=1, le=50, description="If provided, runs a P-Median clustering to select N optimal centers")


class OptimizeItem(BaseModel):
    codigo_ibge: str
    nome: str
    uf: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    score_total: Optional[float] = None
    tier: Optional[str] = None
    ranking_nacional: Optional[int] = None
    populacao_total: Optional[int] = None
    renda_per_capita: Optional[float] = None
    farmacias: Optional[int] = None
    cobertura_planos_pct: Optional[float] = None
    idh: Optional[float] = None
    distance_campinas_km: Optional[float] = None  # km from Campinas HQ
    fit_score: Optional[float] = None
    is_center: Optional[bool] = None
    distance_to_center_km: Optional[float] = None
    cluster_center_codigo: Optional[str] = None


class OptimizeResponse(BaseModel):
    total_matching: int
    criteria: Dict[str, Any]
    results: List[OptimizeItem]


# ── /stats ────────────────────────────────────────────────────────────────────

class PipelineLogEntry(BaseModel):
    etapa: str
    status: str
    municipios_processados: Optional[int] = None
    mensagem: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class TableCount(BaseModel):
    tabela: str
    total: int
    com_dados: Optional[int] = None


class StatsResponse(BaseModel):
    data_counts: List[TableCount]
    pipeline_log: List[PipelineLogEntry]
    last_pipeline_run: Optional[str] = None
    data_quality: Dict[str, Any]


# ── /insights ─────────────────────────────────────────────────────────────────

class InsightsResponse(BaseModel):
    model_config = {"protected_namespaces": ()}

    codigo_ibge: str
    nome: str
    uf: str
    tier: Optional[str] = None
    score_total: Optional[float] = None
    narrative: str
    model_used: str
    generated_at: str

# ── /tradearea ────────────────────────────────────────────────────────────────

class TradeAreaItem(BaseModel):
    codigo_ibge: str
    nome: str
    uf: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    distance_km: float
    attractiveness: float
    probability: float
    estimated_pdvs: Optional[int] = None           # B2B: farmácias capturáveis (Índice de Positivação)
    estimated_customers: Optional[float] = None    # kept for frontend backwards compatibility

class TradeAreaResponse(BaseModel):
    center_lat: float
    center_lon: float
    radius_km: float
    total_estimated_pdvs: Optional[float] = None   # B2B: total PDVs (farmácias) alcançáveis
    total_estimated_customers: Optional[float] = None  # deprecated
    results: List[TradeAreaItem]


# ── /insights/tradearea ───────────────────────────────────────────────────────

class TradeAreaInsightsItem(BaseModel):
    """Subset of TradeAreaItem used in the insights request payload."""
    codigo_ibge: str
    nome: str
    uf: str
    distance_km: float
    probability: float
    estimated_pdvs: Optional[int] = None           # B2B: PDVs alcançáveis neste município
    estimated_customers: Optional[float] = None    # deprecated

    model_config = {"extra": "ignore"}  # silently drop attractiveness / lat / lon


class TradeAreaInsightsRequest(BaseModel):
    codigo_ibge: str   # target municipality for which the simulation was run
    center_lat: float
    center_lon: float
    radius_km: float
    total_estimated_pdvs: Optional[float] = None   # B2B total
    total_estimated_customers: Optional[float] = None  # deprecated
    items: List[TradeAreaInsightsItem]


class TradeAreaInsightsResponse(BaseModel):
    model_config = {"protected_namespaces": ()}

    codigo_ibge: str
    nome: str
    uf: str
    narrative: str
    model_used: str


# ── Entrega 2: Microeconomia por Bairro ────────────────────────────────────────

class SetorResponse(BaseModel):
    codigo_setor:           str
    codigo_ibge:            str
    uf:                     str
    situacao:               Optional[str] = None
    populacao_total:        Optional[int] = None
    domicilios_total:       Optional[int] = None
    renda_media_domiciliar: Optional[float] = None
    area_km2:               Optional[float] = None
    # PDVs OSM
    total_pdvs:             int = 0
    farmacias:              int = 0
    clinicas:               int = 0
    dentistas:              int = 0
    hospitais:              int = 0
    laboratorios:           int = 0
    pdvs_por_km2:           Optional[float] = None
    farmacias_por_10k:      Optional[float] = None
    geom_geojson:           Optional[Dict[str, Any]] = None


class SetoresListResponse(BaseModel):
    codigo_ibge:          str
    nome:                 str
    uf:                   str
    total_setores:        int
    setores_urbanos:      int
    setores_rurais:       int
    populacao_total:      int
    total_farmacias_osm:  int
    nota:                 str
    setores:              List[SetorResponse]
    generated_at: str

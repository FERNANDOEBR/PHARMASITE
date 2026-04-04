// ── Municipio list item (from GET /municipios) ───────────────────────────────
export interface Municipio {
  codigo_ibge: string;
  nome: string;
  uf: string;
  regiao: string | null;
  mesorregiao: string | null;
  microrregiao: string | null;
  latitude: number | null;
  longitude: number | null;
  score_total: number | null;
  tier: string | null;
  ranking_nacional: number | null;
}

export interface MunicipioListResponse {
  total: number;
  page: number;
  limit: number;
  results: Municipio[];
}

// ── Full municipality detail (from GET /municipios/{id}) ──────────────────────
export interface Demograficos {
  populacao_total: number | null;
  populacao_urbana: number | null;
  populacao_rural: number | null;
  taxa_urbanizacao: number | null;
  populacao_alvo: number | null;
  pct_populacao_alvo: number | null;
  renda_per_capita: number | null;
  indice_envelhecimento: number | null;
  pop_0_4: number | null;
  pop_5_14: number | null;
  pop_15_29: number | null;
  pop_30_44: number | null;
  pop_45_64: number | null;
  pop_65_plus: number | null;
  ano_referencia: number | null;
}

export interface Estabelecimentos {
  farmacias: number | null;
  farmacias_magistrais: number | null;
  consultorios_medicos: number | null;
  consultorios_odonto: number | null;
  laboratorios: number | null;
  clinicas: number | null;
  hospitais: number | null;
  ubs_upa: number | null;
  total_estabelecimentos: number | null;
  farmacias_por_10k: number | null;
  estabelecimentos_saude_por_10k: number | null;
  leitos_total: number | null;
  leitos_sus: number | null;
  ano_referencia: number | null;
}

export interface Economicos {
  pib_per_capita: number | null;
  pib_total: number | null;
  cnpjs_farmacias: number | null;
  cnpjs_saude: number | null;
  cnpjs_instrumentos_medicos: number | null;
  cnpjs_distribuidores: number | null;
  beneficiarios_planos: number | null;
  cobertura_planos_pct: number | null;
  idh: number | null;
  empregos_saude: number | null;
  ano_referencia: number | null;
}

export interface ScoreBreakdown {
  score_demografico: number | null;
  score_infraestrutura_saude: number | null;
  score_economico: number | null;
  score_logistico: number | null;
  score_competitividade: number | null;
  pca_component_1: number | null;
  pca_component_2: number | null;
  pca_component_3: number | null;
}

export interface MunicipioDetail {
  codigo_ibge: string;
  nome: string;
  uf: string;
  regiao: string | null;
  mesorregiao: string | null;
  microrregiao: string | null;
  latitude: number | null;
  longitude: number | null;
  area_km2: number | null;
  score_total: number | null;
  tier: string | null;
  ranking_nacional: number | null;
  ranking_estadual: number | null;
  demograficos: Demograficos | null;
  estabelecimentos: Estabelecimentos | null;
  economicos: Economicos | null;
  score: ScoreBreakdown | null;
}

// ── Ranking (from GET /ranking) ───────────────────────────────────────────────
export interface RankingItem {
  ranking_nacional: number | null;
  codigo_ibge: string;
  nome: string;
  uf: string;
  regiao: string | null;
  score_total: number | null;
  tier: string | null;
  populacao_total: number | null;
  pib_per_capita: number | null;
  farmacias: number | null;
  idh: number | null;
}

export interface RankingResponse {
  total: number;
  filters_applied: Record<string, unknown>;
  results: RankingItem[];
}

// ── Trade area (from GET /tradearea/{lat}/{lon}) ───────────────────────────────
export interface TradeAreaItem {
  codigo_ibge: string;
  nome: string;
  uf: string;
  latitude: number | null;
  longitude: number | null;
  distance_km: number;
  attractiveness: number;
  probability: number;
  estimated_customers: number | null;
}

export interface TradeAreaResponse {
  center_lat: number;
  center_lon: number;
  radius_km: number;
  total_estimated_customers: number | null;
  results: TradeAreaItem[];
}

// ── AI Insights (from POST /insights/{id}) ────────────────────────────────────
export interface InsightsResponse {
  codigo_ibge: string;
  nome: string;
  uf: string;
  tier: string | null;
  score_total: number | null;
  narrative: string;
  model_used: string;
  generated_at: string;
}

// ── Trade Area Insights (from POST /insights/tradearea) ───────────────────────
export interface TradeAreaInsightsRequest {
  codigo_ibge: string;
  center_lat: number;
  center_lon: number;
  radius_km: number;
  total_estimated_customers: number | null;
  items: TradeAreaItem[];
}

export interface TradeAreaInsightsResponse {
  codigo_ibge: string;
  nome: string;
  uf: string;
  narrative: string;
  model_used: string;
  generated_at: string;
}

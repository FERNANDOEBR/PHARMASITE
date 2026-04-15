import type {
  MunicipioListResponse,
  MunicipioDetail,
  RankingResponse,
  TradeAreaResponse,
  InsightsResponse,
  TradeAreaInsightsRequest,
  TradeAreaInsightsResponse,
  MicrobairrosResponse,
  MicrobairrosInsightsRequest,
} from './types';

const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://127.0.0.1:8000';

async function get<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const url = new URL(BASE + path);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined) url.searchParams.set(k, String(v));
    });
  }
  const res = await fetch(url.toString(), { cache: 'no-store' });
  if (!res.ok) throw new Error(`API ${res.status}: ${path}`);
  return res.json() as Promise<T>;
}

async function post<T>(path: string): Promise<T> {
  const res = await fetch(BASE + path, { method: 'POST', cache: 'no-store' });
  if (!res.ok) throw new Error(`API ${res.status}: POST ${path}`);
  return res.json() as Promise<T>;
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(BASE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    cache: 'no-store',
  });
  if (!res.ok) throw new Error(`API ${res.status}: POST ${path}`);
  return res.json() as Promise<T>;
}

export const api = {
  getMunicipios(params?: {
    uf?: string;
    limit?: number;
    page?: number;
    q?: string;
  }): Promise<MunicipioListResponse> {
    return get('/municipios', params as Record<string, string | number>);
  },

  getMunicipio(codigoIbge: string): Promise<MunicipioDetail> {
    return get(`/municipios/${codigoIbge}`);
  },

  getRanking(params?: {
    uf?: string;
    tier?: string;
    regiao?: string;
    limit?: number;
  }): Promise<RankingResponse> {
    return get('/ranking', params as Record<string, string | number>);
  },

  getTradeArea(lat: number, lon: number, raioKm = 200): Promise<TradeAreaResponse> {
    return get(`/tradearea/${lat}/${lon}`, { raio_km: raioKm });
  },

  postInsights(codigoIbge: string): Promise<InsightsResponse> {
    return post(`/insights/${codigoIbge}`);
  },

  postTradeAreaInsights(payload: TradeAreaInsightsRequest): Promise<TradeAreaInsightsResponse> {
    return postJson('/insights/tradearea', payload);
  },

  getMicrobairros(codigoIbge: string): Promise<MicrobairrosResponse> {
    return get(`/municipios/${codigoIbge}/microbairros`);
  },

  postMicrobairrosInsights(codigoIbge: string, payload: MicrobairrosInsightsRequest): Promise<{ narrative: string }> {
    return postJson(`/insights/microbairros/${codigoIbge}`, payload);
  },

  getActiveScenario(): Promise<{ weights?: any, config?: import('./types').ScenarioConfig }> {
    return get('/scenarios/active');
  },

  saveScenario(config: import('./types').ScenarioConfig): Promise<{ status: string }> {
    return postJson('/scenarios', config);
  },
};

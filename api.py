"""
PHARMASITE INTELLIGENCE — API Backend (Dia 2)
============================================
FastAPI com 2 endpoints:
  GET  /municipios         → retorna municipios_sp_scored.csv como JSON
  POST /analise/{codigo}   → gera análise IA via Claude Sonnet

Requisitos:
    pip install fastapi uvicorn anthropic pandas python-dotenv

Uso:
    python api.py
    (ou: uvicorn api:app --reload --port 8002)

Variáveis de ambiente:
    ANTHROPIC_API_KEY — obrigatória para /analise
    CSV_PATH          — caminho para o CSV (padrão: municipios_sp_scored.csv)
"""

import os
import json
import time
from pathlib import Path
from functools import lru_cache

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import anthropic
import math
import subprocess
from datetime import datetime
import scenario_manager

class ScenarioPayload(BaseModel):
    sales_data_path: str = ""
    max_viable_km: float = 200.0
    min_population: int = 0
    use_custom_weights: bool = False
    weights: dict | None = None

load_dotenv()

# ─── Config ──────────────────────────────────────────────────────────────────
CSV_PATH = Path(os.getenv("CSV_PATH", "municipios_sp_scored.csv"))
L2_CSV_PATH = Path(os.getenv("L2_CSV_PATH", "l2_regional_master.csv"))
API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

app = FastAPI(
    title="Pharmasite Intelligence API",
    description="Scores de potencial para distribuidores de correlatos farmacêuticos — SP",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Produção: restringir ao domínio do frontend
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Cache do CSV (recarrega se arquivo mudar) ───────────────────────────────
_csv_mtime: float = 0.0
_df_cache: pd.DataFrame | None = None


def load_csv() -> pd.DataFrame:
    global _csv_mtime, _df_cache
    if not CSV_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail=f"CSV não encontrado: {CSV_PATH}. Execute run_standalone.py primeiro."
        )
    mtime = CSV_PATH.stat().st_mtime
    if _df_cache is None or mtime != _csv_mtime:
        _df_cache  = pd.read_csv(CSV_PATH, dtype={"codigo_ibge": str})
        _df_cache["nome_upper"] = _df_cache["nome"].str.upper()
        _df_cache = _df_cache.drop_duplicates(subset=["nome_upper"], keep="first")
        _csv_mtime = mtime
    return _df_cache

_l2_csv_mtime: float = 0.0
_df_l2_cache = None

def load_l2_csv() -> pd.DataFrame:
    global _l2_csv_mtime, _df_l2_cache
    if not L2_CSV_PATH.exists():
        return pd.DataFrame()
    mtime = L2_CSV_PATH.stat().st_mtime
    if _df_l2_cache is None or mtime != _l2_csv_mtime:
        _df_l2_cache = pd.read_csv(L2_CSV_PATH)
        _l2_csv_mtime = mtime
    return _df_l2_cache


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    return {
        "api": "Pharmasite Intelligence",
        "version": "2.0.0",
        "endpoints": {
            "municipios":   "GET /municipios",
            "analise":      "POST /analise/{codigo_ibge}",
            "busca":        "GET /busca?q=Campinas",
            "microbairros": "GET /municipios/{codigo_ibge}/microbairros",
        }
    }


@app.get("/municipios")
def get_municipios():
    """Retorna todos os municípios SP com scores, tiers e indicadores."""
    df = load_csv().copy()
    df["score_total"] = df["score"]
    df["ranking_nacional"] = df["ranking"]
    records = df.to_dict(orient="records")
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                r[k] = None
    return {"total": len(records), "results": records}


@app.get("/municipios/{codigo_ibge}")
def get_municipio(codigo_ibge: str):
    """Retorna dados de um município específico."""
    df = load_csv()
    codigo_ibge = str(codigo_ibge).zfill(7)
    row = df[df["codigo_ibge"] == codigo_ibge]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Município {codigo_ibge} não encontrado")
    
    m = row.to_dict(orient="records")[0]
    for k, v in m.items():
        if isinstance(v, float) and math.isnan(v):
            m[k] = None
    
    # Mapper from flat to nested
    def safe_pct(val, total):
        if val is None or total is None or total == 0:
            return None
        return round((val / total) * 100, 2)
        
    return {
        "codigo_ibge": m.get("codigo_ibge"),
        "nome": m.get("nome"),
        "uf": m.get("uf", "SP"),
        "regiao": m.get("regiao"),
        "mesorregiao": m.get("mesorregiao"),
        "microrregiao": m.get("microrregiao"),
        "latitude": m.get("latitude"),
        "longitude": m.get("longitude"),
        "score_total": m.get("score"),
        "tier": m.get("tier"),
        "ranking_nacional": m.get("ranking"),
        "demograficos": {
            "populacao_total": m.get("populacao_total"),
            "taxa_urbanizacao": m.get("taxa_urbanizacao"),
            "indice_envelhecimento": m.get("indice_envelhecimento"),
            "elderly_pct": m.get("elderly_pct"),
            "populacao_alvo": m.get("populacao_alvo"),
            "pct_populacao_alvo": safe_pct(m.get("populacao_alvo"), m.get("populacao_total"))
        },
        "estabelecimentos": {
            "farmacias": m.get("farmacias"),
            "farmacias_por_10k": m.get("farmacias_por_10k"),
            "consultorios_medicos": m.get("consultorios_medicos"),
            "consultorios_odonto": m.get("consultorios_odonto"),
            "laboratorios": m.get("laboratorios"),
            "clinicas": m.get("clinicas"),
            "hospitais": m.get("hospitais"),
            "ubs_upa": m.get("ubs_upa")
        },
        "economicos": {
            "pib_per_capita": m.get("pib_per_capita"),
            "idh": m.get("idh"),
            "cobertura_planos_pct": m.get("cobertura_planos_pct"),
            "beneficiarios_planos": m.get("beneficiarios_planos")
        },
        "score": {
            "score_demografico": m.get("score_demografico"),
            "score_economico": m.get("score_economico"),
            "score_logistico": m.get("score_logistica"),
            "score_infraestrutura_saude": m.get("score_saude"),
            "score_competitividade": m.get("score_competitividade")
        }
    }


@app.get("/busca")
def buscar_municipio(q: str):
    """Busca municípios por nome (busca parcial, case-insensitive)."""
    df = load_csv().copy()
    df["score_total"] = df["score"]
    df["ranking_nacional"] = df["ranking"]
    import unicodedata
    import math
    def norm(s):
        return unicodedata.normalize("NFD", str(s).lower()).encode("ascii", "ignore").decode()

    q_norm = norm(q)
    mask   = df["nome"].apply(lambda n: q_norm in norm(n))
    result = df[mask].sort_values("score", ascending=False)
    records = result.to_dict(orient="records")
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                r[k] = None
    return {"total": len(records), "results": records}


@app.get("/ranking")
def get_ranking(tier: str = None, top: int = 50):
    """Retorna municípios rankeados, com filtro opcional por tier."""
    df = load_csv().copy()
    df["score_total"] = df["score"]
    df["ranking_nacional"] = df["ranking"]
    import math
    if tier:
        tier = tier.upper()
        if tier not in ("A","B","C","D"):
            raise HTTPException(status_code=400, detail="Tier deve ser A, B, C ou D")
        df = df[df["tier"] == tier]
    df = df.sort_values("score", ascending=False).head(top)
    records = df.to_dict(orient="records")
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                r[k] = None
    return {"total": len(records), "tier_filtro": tier, "results": records}


class TradeAreaInsightsRequest(BaseModel):
    codigo_ibge: str
    center_lat: float
    center_lon: float
    radius_km: float
    total_estimated_customers: float | None
    items: list

@app.post("/insights/tradearea")
def tradearea_insights(data: TradeAreaInsightsRequest):
    prompt = f"""Você é um consultor de expansão de farmácias.
Analise a seguinte Trade Area (Área de Influência) simulada:
Raio: {data.radius_km} km
Total de clientes estimados: {data.total_estimated_customers}
Número de cidades capturadas: {len(data.items)}

DIRETRIZ CRÍTICA: Seja frio e matematicamente rigoroso. Não seja artificialmente otimista para agradar o usuário. Áreas de baixa densidade devem receber recomendação explícita de NÃO ABRIR.

Responda em 3 parágrafos curtos:
1. Avaliação do volume de clientes e raio geográfico.
2. Atratividade para a instalação de uma âncora/loja física ou CD (seja realista sobre os riscos operacionais).
3. Recomendação final de investimento (seja direto: APROVADO, EM ESTUDO, ou REJEITADO)."""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=API_KEY)
        msg = client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-3-haiku-20240307"),
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        msg_text = msg.content[0].text
    except Exception as e:
        print(f"ANTHROPIC EXCEPTION TRADE AREA: {type(e).__name__} - {e}")
        msg_text = f"**[MOCK AI de Trade Area]**\n\nEssa área num raio de {data.radius_km}km detém {data.total_estimated_customers} clientes poteciais englobando as cidades vizinhas. Muito atraente para CD Regional!"

    return {"narrative": msg_text}

@app.post("/insights/{codigo_ibge}")
def gerar_insights(codigo_ibge: str):
    """
    Gera análise estratégica de um município via Claude Sonnet.
    Se não houver key, retorna um texto mock formatado corretamente.
    """
    df = load_csv()
    codigo_ibge = str(codigo_ibge).zfill(7)
    row = df[df["codigo_ibge"] == codigo_ibge]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Município {codigo_ibge} não encontrado")

    m = row.iloc[0]

    # Formatar indicadores para o prompt
    def fmt(val, prefix="", suffix="", default="N/D"):
        try:
            if pd.isna(val):
                return default
            return f"{prefix}{val:,.0f}{suffix}" if isinstance(val, (int, float)) else str(val)
        except Exception:
            return default

    nome            = m.get("nome", "")
    score           = m.get("score", 0)
    tier            = m.get("tier", "?")
    n_farmacias     = fmt(m.get("farmacias", 0))
    renda           = fmt(m.get("renda_per_capita", 0), "R$ ")
    pib_pc          = fmt(m.get("pib_per_capita", 0), "R$ ")
    idh             = m.get("idh", None)
    idh_str         = f"{idh:.3f}" if pd.notna(idh) else "N/D"
    cobertura       = fmt(m.get("cobertura_planos_pct", 0), suffix="%")
    pop             = fmt(m.get("populacao_total", 0))
    dist_campinas   = fmt(m.get("distance_campinas_km", 0), suffix=" km")
    score_demo      = fmt(m.get("score_demografico", 0), suffix="/100")
    score_log       = fmt(m.get("score_logistica", 0), suffix="/100")
    score_econ      = fmt(m.get("score_economico", 0), suffix="/100")
    score_saude     = fmt(m.get("score_saude", 0), suffix="/100")
    ranking         = fmt(m.get("ranking", 0), suffix=f"/645")
    odonto          = fmt(m.get("consultorios_odonto", 0))
    labs            = fmt(m.get("laboratorios", 0))

    prompt = f"""Você é consultor especialista em distribuição de correlatos farmacêuticos no Brasil.
Analise o perfil abaixo e responda em exatamente 3 parágrafos curtos e objetivos.

MUNICÍPIO: {nome} — SP
Score de potencial: {score}/100 | Tier {tier} | Ranking {ranking}

INDICADORES:
  Pilares:         Demo={score_demo} | Logística={score_log} | Economia={score_econ} | Saúde={score_saude}
  Farmácias:       {n_farmacias} estabelecimentos
  Odonto:          {odonto} consultórios
  Laboratórios:    {labs}
  População:       {pop} habitantes
  Renda per capita: {renda}
  PIB per capita:  {pib_pc}
  IDH:             {idh_str}
  Planos de saúde: {cobertura} de cobertura
  Dist. Campinas:  {dist_campinas}

DIRETRIZ CRÍTICA: Seja matematicamente frio e implacável. NÃO seja otimista apenas para agradar o usuário. 
- Para municípios de Tier C e D, a recomendação recomendada deve ser quase sempre EVITAR ou MONITORAR, apontando os baixos volumes que não justificam a logística.
- Para municípios Tier A e B, seja criterioso apontando alto custo de oportunidade e forte concorrência.

Responda em português, sem bullet points, em 3 parágrafos:
1. [Score] Por que este município tem score {score}/100 (Tier {tier}) — o que puxa para cima e o que limita severamente
2. [Oportunidade] Qual a realidade e teto da oportunidade para correlatos farmacêuticos (seja cético)
3. [Ação] Recomendação final explícita: ENTRAR AGORA / MONITORAR estruturalmente / EVITAR ativamente — com justificativa econômica"""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=API_KEY)

        msg = client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-3-haiku-20240307"),
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        msg_text = msg.content[0].text

    except Exception as e:
        print(f"ANTHROPIC EXCEPTION: {type(e).__name__} - {e}")
        msg_text = (
            f"**[MOCK AI - Sem Token]**\n\n"
            f"O município está no Tier {tier} com Destaque para Farmácias ({n_farmacias}).\n\n"
            f"**Oportunidades**\n"
            f"A renda média de {renda} suporta novos correlatos.\n\n"
            f"*(Adicione ANTHROPIC_API_KEY localmente para usar Claude Real)*"
        )

    return {
        "codigo_ibge": codigo_ibge,
        "nome":        nome,
        "score_total":       score,
        "tier":        tier,
        "narrative":     msg_text,
        "model_used":       "claude-sonnet-4-6",
        "generated_at": datetime.now().isoformat()
    }


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

@app.get("/tradearea/{lat}/{lon}")
def get_tradearea(lat: float, lon: float, raio_km: float = 200.0):
    df = load_csv().copy()
    import numpy as np
    
    # Vectorized Haversine
    df['lat_rad'] = np.radians(pd.to_numeric(df['latitude'], errors='coerce'))
    df['lon_rad'] = np.radians(pd.to_numeric(df['longitude'], errors='coerce'))
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    
    dphi = df['lat_rad'] - lat_rad
    dlambda = df['lon_rad'] - lon_rad
    
    a = np.sin(dphi/2.0)**2 + math.cos(lat_rad) * np.cos(df['lat_rad']) * np.sin(dlambda/2.0)**2
    df['dist'] = 6371.0 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    
    # Filter
    df.loc[df['latitude'].isna() | df['longitude'].isna(), 'dist'] = float('inf')
    df = df[df['dist'] <= raio_km].sort_values("dist")
    
    results = []
    total_customers = 0
    for _, row in df.iterrows():
        prob = max(0, 1 - (row['dist'] / raio_km))
        if prob == 0: continue
        pop_alvo = row.get("populacao_alvo", 0)
        pop_alvo = float(pop_alvo) if pd.notna(pop_alvo) else 0
        est_customers = int(pop_alvo * prob * 0.1)
        total_customers += est_customers
        
        results.append({
            "codigo_ibge": row["codigo_ibge"],
            "nome": row["nome"],
            "uf": row.get("uf", "SP"),
            "latitude": row["latitude"] if pd.notna(row["latitude"]) else None,
            "longitude": row["longitude"] if pd.notna(row["longitude"]) else None,
            "distance_km": round(row['dist'], 2),
            "attractiveness": round(prob * 100, 1),
            "probability": round(prob, 2),
            "estimated_customers": est_customers
        })
        
    return {"results": results, "total_estimated_customers": total_customers}

@app.get("/municipios/{codigo_ibge}/microbairros")
def get_municipio_microbairros(codigo_ibge: str):
    df = load_csv()
    match = df[df["codigo_ibge"] == codigo_ibge]
    if match.empty:
        raise HTTPException(status_code=404, detail="Município não encontrado")
    record = match.iloc[0]
    city_name = record["nome"]
    city_tier = record.get("tier", "Z")
    renda_proxy = record.get("renda_per_capita", 0)
    base_demand = record.get("score_economico", 0)

    df_l2 = load_l2_csv()
    if not df_l2.empty:
        # Check cache exact match
        cache_match = df_l2[df_l2["City"].str.upper() == city_name.upper()]
        if not cache_match.empty:
            return {"source": "cache", "microbairros": cache_match.to_dict(orient="records")}
            
    # Trigger live OSM
    from l2_regional_engine import get_city_bairros, get_city_pharmacies, haversine
    print(f"Triggering Live OSM search for {city_name} (L2 Cache Miss)")
    
    bairros = get_city_bairros(city_name)
    pharmacies = get_city_pharmacies(city_name)
    
    if not bairros:
        return {"source": "live", "microbairros": []}
        
    bairro_pdv_map = {b['bairro']: {'total': 0, 'big_chains': 0, 'independents': 0, 'lat': b['lat'], 'lon': b['lon']} for b in bairros}
    MAX_DIST_KM = 2.0
    for p in pharmacies:
        closest_bairro = None
        min_dist = float('inf')
        for b in bairros:
            d = haversine(p['lon'], p['lat'], b['lon'], b['lat'])
            if d < min_dist and d <= MAX_DIST_KM:
                min_dist = d
                closest_bairro = b['bairro']
        if closest_bairro:
            bairro_pdv_map[closest_bairro]['total'] += 1
            if p['chain_type'] == "Big Chain":
                bairro_pdv_map[closest_bairro]['big_chains'] += 1
            else:
                bairro_pdv_map[closest_bairro]['independents'] += 1
                
    master_bairro_data = []
    for name, data in bairro_pdv_map.items():
        saturation_penalty = (data['big_chains'] * 15) + (data['independents'] * 5)
        demand = base_demand
        name_lower = name.lower()
        if "centro" in name_lower or "jardim" in name_lower or "morumbi" in name_lower:
            demand *= 1.15
        
        opportunity = demand - saturation_penalty
        if opportunity < 0: opportunity = 0
        opportunity = min(opportunity, 100)
        
        if opportunity > 40 or data['total'] == 0:
            master_bairro_data.append({
                "City_Tier": city_tier,
                "City": city_name,
                "Microbairro": name,
                "Latitude": data['lat'],
                "Longitude": data['lon'],
                "City_Income_Proxy": renda_proxy,
                "Mapped_Pharmacies": data['total'],
                "Big_Chains_Mapped": data['big_chains'],
                "Opportunity_Score": round(opportunity, 1)
            })
            
    master_bairro_data = sorted(master_bairro_data, key=lambda x: x["Opportunity_Score"], reverse=True)
    return {"source": "live", "microbairros": master_bairro_data}

@app.post("/insights/microbairros/{codigo_ibge}")
def post_microbairros_insights(codigo_ibge: str, payload: dict):
    if not API_KEY:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY não configurada")
    
    city_name = payload.get("city")
    items = payload.get("items", [])
    if not items:
        return {"narrative": "Não há regiões gap suficientes mapeadas via OpenStreetMap L2 para formular um pitch."}
        
    prompt = (
        f"You are an expert Real Estate and Pharmaceutical Market Analyst acting on behalf of PharmaSite.\n"
        f"Your task is to draft a concise, executive pitch for pharmaceutical chain expansion teams based on our proprietary L2 Microbairro Opportunity data.\n\n"
        f"We have identified {len(items)} structural gaps (microbairros) in {city_name} where demand is high but formal pharmacy presence is abnormally low/non-existent.\n\n"
        f"DATA:\n"
    )
    for row in items[:15]:
        prompt += f"- {row['Microbairro']} | Score: {row['Opportunity_Score']} | Renda Base: R${row['City_Income_Proxy']} | PDVs Físicos: {row['Mapped_Pharmacies']} (Redes: {row['Big_Chains_Mapped']})\n"
        
    prompt += (
        f"\nDraft a highly persuasive, 3-paragraph executive summary recommending a land-grab strategy in these specific neighborhoods.\n"
        f"Explain why gaps in {city_name} represent uncontested blue oceans based on these metrics. Keep it short, actionable, and formatted in Markdown."
    )
    
    try:
        client = anthropic.Anthropic(api_key=API_KEY)
        resp = client.messages.create(
            model="claude-3-7-sonnet-20250219",
            max_tokens=600,
            system="Responda em português com tom executivo e persuasivo.",
            messages=[{"role": "user", "content": prompt}]
        )
        return {"narrative": resp.content[0].text}
    except Exception as e:
        print("Anthropic Error:", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scout")
def agent_scout_endpoint(bairro: str, cidade: str = "Campinas, SP"):
    """
    Roda o Agent Scout para um dado bairro usando web search + LLM.
    """
    try:
        from agent_scout import run_scout
        result = run_scout(bairro, cidade)
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return result
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="Faltam dependências do scout. Rode com: uv run --with duckduckgo-search"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no Scout: {str(e)}")


@app.get("/stats")
def get_stats():
    """Estatísticas gerais do dataset."""
    df = load_csv()
    tier_counts = df["tier"].value_counts().to_dict()

    def safe_stat(col):
        s = pd.to_numeric(df.get(col), errors="coerce")
        return {
            "min": round(float(s.min()), 1) if s.notna().any() else None,
            "max": round(float(s.max()), 1) if s.notna().any() else None,
            "mediana": round(float(s.median()), 1) if s.notna().any() else None,
            "preenchidos": int(s.notna().sum()),
        }

    return {
        "total_municipios": len(df),
        "tiers": tier_counts,
        "score":           safe_stat("score"),
        "farmacias":       safe_stat("farmacias"),
        "populacao":       safe_stat("populacao_total"),
        "renda":           safe_stat("renda_per_capita"),
        "cobertura_planos": safe_stat("cobertura_planos_pct"),
        "csv_path": str(CSV_PATH.resolve()),
    }


@app.get("/scenarios/active")
def get_active_scenario():
    """Retrieve custom override scenarios."""
    weights = scenario_manager.load_active_scenario()
    return {"weights": weights}

@app.post("/scenarios")
def save_active_scenario(payload: ScenarioPayload):
    """Save custom overrides and regenerate the model weights instantly."""
    import os
    scenarios_dir = Path("scenarios")
    if not scenarios_dir.exists():
        os.makedirs(scenarios_dir)
        
    active_path = scenarios_dir / "active_scenario.json"
    
    with open(active_path, "w", encoding="utf-8") as f:
        json.dump(payload.model_dump(), f, indent=4)
        
    # Trigger offline recalculation synchronously
    global _df_cache, _csv_mtime
    try:
        print("  [API] Triggering score_offline.py recalculation...")
        subprocess.run(["python", "score_offline.py"], check=True)
        # Invalidate buffer
        _df_cache = None
        _csv_mtime = 0.0
        return {"status": "success", "message": "Motor recalculado com novo cenário"}
    except Exception as e:
        print(f"Error running score_offline.py: {e}")
        raise HTTPException(status_code=500, detail="Erro no recálculo do motor.")

# ─── Run direto ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 55)
    print("  PHARMASITE INTELLIGENCE API v2.0")
    print("=" * 55)
    print(f"  CSV: {CSV_PATH.resolve()}")
    print(f"  Claude: {'[OK] configurado' if API_KEY else '[ERRO] ANTHROPIC_API_KEY não configurado'}")
    print("  Endpoints:")
    print("    GET  http://localhost:8000/municipios")
    print("    GET  http://localhost:8000/busca?q=Campinas")
    print("    GET  http://localhost:8000/ranking?tier=A")
    print("    POST http://localhost:8000/analise/3509502")
    print("    GET  http://localhost:8000/stats")
    print("=" * 55 + "\n")

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)

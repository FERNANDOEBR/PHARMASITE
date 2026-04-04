"""
ENTREGA 3 — Agent Scout: Sinais de Crescimento

Endpoints:
  GET  /growth/{codigo_ibge}          — sinal de crescimento de um município
  GET  /scout                         — ranking de oportunidades (mercado × crescimento)
  GET  /scout/white-spaces            — municípios com crescimento alto + mercado médio
  POST /scout/radar                   — dado lat/lon/raio, retorna municípios em ascensão

O insight do cliente: "Quem tem essa informação antecipadamente consegue
imóveis mais baratos ou chega antes da concorrência."
"""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Connection

from cache import TTL_LONG, cache_get, cache_set
from database import get_db

router = APIRouter()


# ── GET /growth/{codigo_ibge} ─────────────────────────────────────────────────

@router.get("/growth/{codigo_ibge}", tags=["Agent Scout"])
def get_growth_municipio(
    codigo_ibge: str,
    db: Connection = Depends(get_db),
):
    """
    Retorna os sinais de crescimento de um município:
    delta de aberturas de CNPJs (farmácias, construção, logística, imobiliário)
    entre 2022 e 2024, com growth_score e classificação qualitativa.
    """
    cache_key = f"growth:{codigo_ibge}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    mun = db.execute(
        text("SELECT nome, uf FROM municipios WHERE codigo_ibge = :c"),
        {"c": codigo_ibge}
    ).fetchone()
    if not mun:
        raise HTTPException(status_code=404, detail=f"Município {codigo_ibge} não encontrado")

    g = db.execute(
        text("SELECT * FROM growth_signals WHERE codigo_ibge = :c"),
        {"c": codigo_ibge}
    ).fetchone()

    if not g:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sem sinais de crescimento para {mun.nome}-{mun.uf}. "
                "Dados disponíveis após rodar 'make agent1' com pipeline completo."
            )
        )

    result = {
        "codigo_ibge":           codigo_ibge,
        "nome":                  mun.nome,
        "uf":                    mun.uf,
        "growth_score":          g.growth_score,
        "growth_tier":           g.growth_tier,
        "growth_label":          g.growth_label,
        "anos_analisados":       g.anos_analisados,
        "sinais": {
            "farmacias": {
                "descricao":   "Aberturas CNAE 4771 — Farmácias e drogarias",
                "2022":        g.farmacias_2022,
                "2023":        g.farmacias_2023,
                "2024":        g.farmacias_2024,
                "delta_pct":   g.farmacias_delta_pct,
                "interpretacao": _interpret_delta(g.farmacias_delta_pct, "farmácias"),
            },
            "construcao": {
                "descricao":   "Aberturas CNAE 41xx — Construção civil e incorporação",
                "2022":        g.construcao_2022,
                "2023":        g.construcao_2023,
                "2024":        g.construcao_2024,
                "delta_pct":   g.construcao_delta_pct,
                "interpretacao": _interpret_delta(g.construcao_delta_pct, "construção"),
            },
            "logistica": {
                "descricao":   "Aberturas CNAE 52xx — Armazéns e depósitos logísticos",
                "2022":        g.logistica_2022,
                "2023":        g.logistica_2023,
                "2024":        g.logistica_2024,
                "delta_pct":   g.logistica_delta_pct,
                "interpretacao": _interpret_delta(g.logistica_delta_pct, "logística"),
            },
            "imobiliario": {
                "descricao":   "Aberturas CNAE 68xx — Incorporação e gestão imobiliária",
                "2022":        g.imob_2022,
                "2023":        g.imob_2023,
                "2024":        g.imob_2024,
                "delta_pct":   g.imob_delta_pct,
                "interpretacao": _interpret_delta(g.imob_delta_pct, "mercado imobiliário"),
            },
        },
        "nota": (
            "Delta calculado sobre aberturas de CNPJs ativos na Receita Federal. "
            "Crescimento em construção + logística antecede crescimento em farmácias em ~18-24 meses."
        )
    }

    cache_set(cache_key, result, ttl=TTL_LONG)
    return result


def _interpret_delta(delta_pct: float | None, setor: str) -> str:
    if delta_pct is None:
        return f"Sem dados de {setor}"
    if delta_pct > 50:
        return f"🔥 Crescimento acelerado em {setor} (+{delta_pct:.0f}%)"
    if delta_pct > 15:
        return f"📈 Crescimento consistente em {setor} (+{delta_pct:.0f}%)"
    if delta_pct > -10:
        return f"➡️  Estável em {setor} ({delta_pct:+.0f}%)"
    return f"📉 Retração em {setor} ({delta_pct:.0f}%)"


# ── GET /scout ────────────────────────────────────────────────────────────────

@router.get("/scout", tags=["Agent Scout"])
def get_scout_ranking(
    limit:          int   = Query(50, ge=1, le=500),
    uf:             Optional[str] = Query(None),
    max_dist_km:    Optional[float] = Query(None, description="Raio máximo de Campinas em km"),
    growth_tier_min: Optional[str] = Query(None, description="Tier mínimo de crescimento: A, B, C ou D"),
    db: Connection = Depends(get_db),
):
    """
    Ranking de oportunidades: municípios ordenados pelo score composto
    (mercado atual × crescimento). Suporta filtros por UF, raio e tier de crescimento.

    Esta é a visão do Agent Scout: não só onde o mercado é grande hoje,
    mas onde ele está crescendo — combinando os dois sinais.
    """
    cache_key = f"scout:lim={limit}:uf={uf}:dist={max_dist_km}:tier={growth_tier_min}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    conditions = []
    params = {"limit": limit}

    if uf:
        conditions.append("sv.uf = :uf")
        params["uf"] = uf.upper()
    if max_dist_km:
        conditions.append("sv.distance_campinas_km <= :dist")
        params["dist"] = max_dist_km
    if growth_tier_min:
        tier_order = {"A": 1, "B": 2, "C": 3, "D": 4}
        min_order = tier_order.get(growth_tier_min.upper(), 4)
        tiers_ok = [t for t, o in tier_order.items() if o <= min_order]
        conditions.append(f"sv.growth_tier = ANY(:tiers)")
        params["tiers"] = tiers_ok

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = db.execute(text(f"""
        SELECT
            sv.codigo_ibge,
            sv.nome,
            sv.uf,
            sv.latitude,
            sv.longitude,
            sv.score_mercado_atual,
            sv.tier_mercado,
            sv.ranking_nacional,
            sv.distance_campinas_km,
            sv.growth_score,
            sv.growth_tier,
            sv.growth_label,
            sv.farmacias_delta_pct,
            sv.farmacias_2022,
            sv.farmacias_2024,
            sv.oportunidade_score,
            sv.is_white_space
        FROM scout_view sv
        {where}
        ORDER BY sv.oportunidade_score DESC NULLS LAST
        LIMIT :limit
    """), params).fetchall()

    municipios = []
    for r in rows:
        municipios.append({
            "codigo_ibge":        r.codigo_ibge,
            "nome":               r.nome,
            "uf":                 r.uf,
            "latitude":           float(r.latitude) if r.latitude else None,
            "longitude":          float(r.longitude) if r.longitude else None,
            "score_mercado_atual": float(r.score_mercado_atual) if r.score_mercado_atual else None,
            "tier_mercado":        r.tier_mercado,
            "ranking_nacional":    r.ranking_nacional,
            "distance_campinas_km": float(r.distance_campinas_km) if r.distance_campinas_km else None,
            "growth_score":        float(r.growth_score) if r.growth_score else None,
            "growth_tier":         r.growth_tier,
            "growth_label":        r.growth_label,
            "farmacias_delta_pct": float(r.farmacias_delta_pct) if r.farmacias_delta_pct else None,
            "farmacias_2022":      r.farmacias_2022,
            "farmacias_2024":      r.farmacias_2024,
            "oportunidade_score":  float(r.oportunidade_score) if r.oportunidade_score else None,
            "is_white_space":      r.is_white_space,
        })

    result = {
        "total":      len(municipios),
        "filtros": {
            "uf":             uf,
            "max_dist_km":    max_dist_km,
            "growth_tier_min": growth_tier_min,
        },
        "metodologia": (
            "oportunidade_score = 50% score_mercado_atual + 50% growth_score. "
            "White space = crescimento tier A/B com mercado ainda tier B/C."
        ),
        "municipios": municipios,
    }

    cache_set(cache_key, result, ttl=TTL_LONG)
    return result


# ── GET /scout/white-spaces ───────────────────────────────────────────────────

@router.get("/scout/white-spaces", tags=["Agent Scout"])
def get_white_spaces(
    max_dist_km: float = Query(300.0, description="Raio máximo de Campinas em km"),
    limit:       int   = Query(20, ge=1, le=100),
    db: Connection = Depends(get_db),
):
    """
    White spaces: municípios com crescimento acelerado mas mercado ainda não saturado.
    Estes são os alvos prioritários para expansão antes da concorrência.

    Definição: growth_tier A ou B + tier_mercado B ou C + dentro do raio logístico.
    """
    rows = db.execute(text("""
        SELECT
            sv.codigo_ibge, sv.nome, sv.uf,
            sv.latitude, sv.longitude,
            sv.score_mercado_atual, sv.tier_mercado,
            sv.distance_campinas_km,
            sv.growth_score, sv.growth_tier, sv.growth_label,
            sv.farmacias_delta_pct, sv.construcao_delta_pct,
            sv.farmacias_2024, sv.oportunidade_score
        FROM scout_view sv
        WHERE sv.is_white_space = TRUE
          AND sv.distance_campinas_km <= :dist
        ORDER BY sv.growth_score DESC, sv.oportunidade_score DESC
        LIMIT :limit
    """), {"dist": max_dist_km, "limit": limit}).fetchall()

    result = []
    for r in rows:
        result.append({
            "codigo_ibge":           r.codigo_ibge,
            "nome":                  r.nome,
            "uf":                    r.uf,
            "latitude":              float(r.latitude) if r.latitude else None,
            "longitude":             float(r.longitude) if r.longitude else None,
            "distancia_campinas_km": float(r.distance_campinas_km) if r.distance_campinas_km else None,
            "score_mercado_atual":   float(r.score_mercado_atual) if r.score_mercado_atual else None,
            "tier_mercado":          r.tier_mercado,
            "growth_score":          float(r.growth_score) if r.growth_score else None,
            "growth_tier":           r.growth_tier,
            "growth_label":          r.growth_label,
            "farmacias_delta_pct":   float(r.farmacias_delta_pct) if r.farmacias_delta_pct else None,
            "construcao_delta_pct":  float(r.construcao_delta_pct) if r.construcao_delta_pct else None,
            "farmacias_2024":        r.farmacias_2024,
            "oportunidade_score":    float(r.oportunidade_score) if r.oportunidade_score else None,
            "motivo": (
                f"Crescimento {r.growth_label} (+{r.farmacias_delta_pct:.0f}% farmácias 2022→2024) "
                f"com mercado ainda Tier {r.tier_mercado}. "
                f"Janela de entrada: antes da saturação."
            ),
        })

    return {
        "total":        len(result),
        "raio_km":      max_dist_km,
        "descricao":    "Municípios com crescimento acelerado e mercado ainda não saturado — janela de expansão.",
        "white_spaces": result,
    }


# ── POST /scout/radar ─────────────────────────────────────────────────────────

from pydantic import BaseModel

class RadarRequest(BaseModel):
    latitude:   float
    longitude:  float
    raio_km:    float = 200.0
    top_n:      int   = 10


@router.post("/scout/radar", tags=["Agent Scout"])
def scout_radar(
    req: RadarRequest,
    db: Connection = Depends(get_db),
):
    """
    Dado um ponto (lat/lon) e raio, retorna os municípios com maior
    oportunidade dentro desse raio — ordenados por oportunidade_score.

    Caso de uso: "Quero expandir para a região de Ribeirão Preto.
    Quais municípios em 150 km têm maior potencial de crescimento?"
    """
    rows = db.execute(text("""
        SELECT
            sv.codigo_ibge, sv.nome, sv.uf,
            sv.latitude, sv.longitude,
            sv.score_mercado_atual, sv.tier_mercado,
            sv.growth_score, sv.growth_tier, sv.growth_label,
            sv.farmacias_delta_pct,
            sv.farmacias_2024,
            sv.oportunidade_score,
            sv.is_white_space,
            -- distância do ponto informado (haversine via PostGIS)
            ROUND(
                ST_Distance(
                    ST_SetSRID(ST_MakePoint(sv.longitude, sv.latitude), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography
                ) / 1000.0
            , 1) AS dist_km
        FROM scout_view sv
        WHERE sv.latitude IS NOT NULL
          AND ST_DWithin(
              ST_SetSRID(ST_MakePoint(sv.longitude, sv.latitude), 4326)::geography,
              ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
              :raio_m
          )
        ORDER BY sv.oportunidade_score DESC NULLS LAST
        LIMIT :n
    """), {
        "lat":    req.latitude,
        "lon":    req.longitude,
        "raio_m": req.raio_km * 1000,
        "n":      req.top_n,
    }).fetchall()

    result = []
    for r in rows:
        result.append({
            "codigo_ibge":        r.codigo_ibge,
            "nome":               r.nome,
            "uf":                 r.uf,
            "distancia_km":       float(r.dist_km) if r.dist_km else None,
            "score_mercado_atual": float(r.score_mercado_atual) if r.score_mercado_atual else None,
            "tier_mercado":        r.tier_mercado,
            "growth_score":        float(r.growth_score) if r.growth_score else None,
            "growth_tier":         r.growth_tier,
            "growth_label":        r.growth_label,
            "farmacias_delta_pct": float(r.farmacias_delta_pct) if r.farmacias_delta_pct else None,
            "farmacias_2024":      r.farmacias_2024,
            "oportunidade_score":  float(r.oportunidade_score) if r.oportunidade_score else None,
            "is_white_space":      r.is_white_space,
        })

    return {
        "centro": {"latitude": req.latitude, "longitude": req.longitude},
        "raio_km": req.raio_km,
        "total":   len(result),
        "top_n":   req.top_n,
        "municipios": result,
    }

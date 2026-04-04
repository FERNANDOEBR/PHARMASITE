"""
GET /score/{codigo_ibge}  — score detail + tier + rankings + component breakdown
GET /ranking              — top-N ranked municipalities with optional filters
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from cache import TTL_RANKING, TTL_SCORE, cache_get, cache_set
from database import get_db
from schemas import RankingItem, RankingResponse, ScoreBreakdown, ScoreResponse

router = APIRouter()


@router.get("/score/{codigo_ibge}", response_model=ScoreResponse)
def get_score(codigo_ibge: str, db: Connection = Depends(get_db)):
    cache_key = f"score:{codigo_ibge}"
    cached = cache_get(cache_key)
    if cached:
        return ScoreResponse(**cached)

    row = db.execute(text("""
        SELECT
            m.codigo_ibge, m.nome, m.uf,
            s.score_total, s.tier, s.ranking_nacional, s.ranking_estadual,
            s.score_demografico, s.score_infraestrutura_saude, s.score_economico,
            s.score_logistico, s.score_competitividade, s.distance_campinas_km,
            s.pca_component_1, s.pca_component_2, s.pca_component_3
        FROM municipios m
        JOIN scores s ON m.codigo_ibge = s.codigo_ibge
        WHERE m.codigo_ibge = :cod
        ORDER BY s.id DESC
    """), {"cod": codigo_ibge}).mappings().first()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Score não encontrado para {codigo_ibge}. "
                   "Verifique se o pipeline foi executado.",
        )

    d = dict(row)
    result = ScoreResponse(
        codigo_ibge=d["codigo_ibge"],
        nome=d["nome"],
        uf=d["uf"],
        score_total=d.get("score_total"),
        tier=d.get("tier"),
        ranking_nacional=d.get("ranking_nacional"),
        ranking_estadual=d.get("ranking_estadual"),
        breakdown=ScoreBreakdown(
            score_demografico=d.get("score_demografico"),
            score_infraestrutura_saude=d.get("score_infraestrutura_saude"),
            score_economico=d.get("score_economico"),
            score_logistico=d.get("score_logistico"),
            score_competitividade=d.get("score_competitividade"),
            distance_campinas_km=d.get("distance_campinas_km"),
            pca_component_1=d.get("pca_component_1"),
            pca_component_2=d.get("pca_component_2"),
            pca_component_3=d.get("pca_component_3"),
        ),
    )
    cache_set(cache_key, result.model_dump(), TTL_SCORE)
    return result


@router.get("/ranking", response_model=RankingResponse)
def get_ranking(
    uf:              Optional[str] = Query(None, min_length=2, max_length=2),
    tier:            Optional[str] = Query(None, pattern="^[ABCD]$"),
    regiao:          Optional[str] = Query(None),
    max_distance_km: Optional[int] = Query(None, ge=1, description="Filtrar municípios dentro do raio (km) do CD em Campinas-SP. Ex: 200"),
    limit:           int           = Query(50, ge=1, le=500),
    db: Connection = Depends(get_db),
):
    cache_key = f"ranking:{uf}:{tier}:{regiao}:{max_distance_km}:{limit}"
    cached = cache_get(cache_key)
    if cached:
        return RankingResponse(**cached)

    filters = []
    params: Dict[str, Any] = {"limit": limit}

    if uf:
        filters.append("m.uf = :uf")
        params["uf"] = uf.upper()
    if tier:
        filters.append("s.tier = :tier")
        params["tier"] = tier.upper()
    if regiao:
        filters.append("m.regiao = :regiao")
        params["regiao"] = regiao
    if max_distance_km is not None:
        filters.append("s.distance_campinas_km <= :max_dist")
        params["max_dist"] = max_distance_km

    where = "WHERE " + " AND ".join(filters) if filters else ""

    rows = db.execute(text(f"""
        WITH latest_scores AS (
            SELECT DISTINCT ON (codigo_ibge) * FROM scores ORDER BY codigo_ibge, id DESC
        ),
        latest_demo AS (
            SELECT DISTINCT ON (codigo_ibge) * FROM demograficos ORDER BY codigo_ibge, id DESC
        ),
        latest_ec AS (
            SELECT DISTINCT ON (codigo_ibge) * FROM indicadores_economicos ORDER BY codigo_ibge, id DESC
        ),
        latest_est AS (
            SELECT DISTINCT ON (codigo_ibge) * FROM estabelecimentos_saude ORDER BY codigo_ibge, id DESC
        )
        SELECT
            s.ranking_nacional, m.codigo_ibge, m.nome, m.uf, m.regiao,
            s.score_total, s.score_logistico, s.tier, s.distance_campinas_km,
            d.populacao_total, ec.pib_per_capita,
            e.farmacias, ec.idh
        FROM municipios m
        JOIN latest_scores s ON m.codigo_ibge = s.codigo_ibge
        LEFT JOIN latest_demo d ON m.codigo_ibge = d.codigo_ibge
        LEFT JOIN latest_ec ec ON m.codigo_ibge = ec.codigo_ibge
        LEFT JOIN latest_est e ON m.codigo_ibge = e.codigo_ibge
        {where}
        ORDER BY s.score_total DESC NULLS LAST, m.codigo_ibge
        LIMIT :limit
    """), params).mappings().all()

    total = db.execute(text(f"""
        WITH latest_scores AS (
            SELECT DISTINCT ON (codigo_ibge) * FROM scores ORDER BY codigo_ibge, id DESC
        )
        SELECT COUNT(*)
        FROM municipios m
        JOIN latest_scores s ON m.codigo_ibge = s.codigo_ibge
        {where}
    """), params).scalar_one()

    filters_applied = {
        k: v for k, v in {
            "uf": uf, "tier": tier, "regiao": regiao, "max_distance_km": max_distance_km
        }.items() if v is not None
    }
    result = RankingResponse(
        total=total,
        filters_applied=filters_applied,
        results=[RankingItem(**dict(r)) for r in rows],
    )
    cache_set(cache_key, result.model_dump(), TTL_RANKING)
    return result

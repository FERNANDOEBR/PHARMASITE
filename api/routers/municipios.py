"""
GET /municipios  — paginated list with optional filters
GET /municipios/{codigo_ibge}  — full detail (all joined tables)
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from cache import TTL_MUNICIPIO_DETAIL, cache_get, cache_set
from database import get_db
from schemas import (
    DemograficosOut,
    EconomicosOut,
    EstabelecimentosOut,
    MunicipioDetail,
    MunicipioListItem,
    MunicipioListResponse,
    ScoreBreakdown,
)

router = APIRouter()


@router.get("/municipios", response_model=MunicipioListResponse)
def list_municipios(
    uf:     Optional[str] = Query(None, min_length=2, max_length=2, description="Sigla UF ex: SP"),
    regiao: Optional[str] = Query(None, description="Norte, Nordeste, Sudeste, Sul, Centro-Oeste"),
    tier:   Optional[str] = Query(None, pattern="^[ABCD]$"),
    q:      Optional[str] = Query(None, min_length=2, description="Busca por nome"),
    page:   int           = Query(1, ge=1),
    limit:  int           = Query(20, ge=1, le=10000),
    db: Connection = Depends(get_db),
):
    filters = []
    params: dict = {"offset": (page - 1) * limit, "limit": limit}

    if uf:
        filters.append("m.uf = :uf")
        params["uf"] = uf.upper()
    if regiao:
        filters.append("m.regiao = :regiao")
        params["regiao"] = regiao
    if tier:
        filters.append("s.tier = :tier")
        params["tier"] = tier.upper()
    if q:
        # Use ILIKE — compatible without unaccent extension
        filters.append("m.nome ILIKE :q")
        params["q"] = f"%{q}%"

    where = "WHERE " + " AND ".join(filters) if filters else ""

    total = db.execute(text(f"""
        SELECT COUNT(*)
        FROM municipios m
        LEFT JOIN LATERAL (
            SELECT tier FROM scores
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) s ON true
        {where}
    """), params).scalar_one()

    rows = db.execute(text(f"""
        SELECT
            m.codigo_ibge, m.nome, m.uf, m.regiao,
            m.mesorregiao, m.microrregiao, m.latitude, m.longitude,
            s.score_total, s.tier, s.ranking_nacional
        FROM municipios m
        LEFT JOIN LATERAL (
            SELECT score_total, tier, ranking_nacional 
            FROM scores 
            WHERE codigo_ibge = m.codigo_ibge 
            ORDER BY id DESC LIMIT 1
        ) s ON true
        {where}
        ORDER BY s.score_total DESC NULLS LAST, m.codigo_ibge
        LIMIT :limit OFFSET :offset
    """), params).mappings().all()

    return MunicipioListResponse(
        total=total,
        page=page,
        limit=limit,
        results=[MunicipioListItem(**dict(r)) for r in rows],
    )


@router.get("/municipios/{codigo_ibge}", response_model=MunicipioDetail)
def get_municipio(codigo_ibge: str, db: Connection = Depends(get_db)):
    # Bumping cache key to v2 to bust Production Redis cache for new elderly_pct field
    cache_key = f"municipio:detail:v2:{codigo_ibge}"
    cached = cache_get(cache_key)
    if cached:
        return MunicipioDetail(**cached)

    row = db.execute(text("""
        SELECT codigo_ibge, nome, uf, regiao, mesorregiao, microrregiao,
               latitude, longitude, area_km2
        FROM municipios WHERE codigo_ibge = :cod
    """), {"cod": codigo_ibge}).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"Município {codigo_ibge} não encontrado")

    demo = db.execute(text("""
        SELECT populacao_total, populacao_urbana, populacao_rural, taxa_urbanizacao,
               populacao_alvo, pct_populacao_alvo, renda_per_capita,
               CASE WHEN indice_envelhecimento IS NOT NULL AND indice_envelhecimento > 0
                    THEN ROUND(CAST(indice_envelhecimento AS numeric) /
                               (100 + CAST(indice_envelhecimento AS numeric)) * 100, 1)
                    ELSE NULL END AS elderly_pct,
               pop_0_4, pop_5_14, pop_15_29, pop_30_44, pop_45_64, pop_65_plus, ano_referencia
        FROM demograficos WHERE codigo_ibge = :cod ORDER BY id DESC
    """), {"cod": codigo_ibge}).mappings().first()

    estab = db.execute(text("""
        SELECT farmacias, farmacias_magistrais, consultorios_medicos, consultorios_odonto,
               laboratorios, clinicas, hospitais, ubs_upa, total_estabelecimentos,
               farmacias_por_10k, estabelecimentos_saude_por_10k, leitos_total, leitos_sus,
               ano_referencia
        FROM estabelecimentos_saude WHERE codigo_ibge = :cod ORDER BY id DESC
    """), {"cod": codigo_ibge}).mappings().first()

    econ = db.execute(text("""
        SELECT pib_per_capita, pib_total, cnpjs_farmacias, cnpjs_saude,
               cnpjs_instrumentos_medicos, cnpjs_distribuidores, beneficiarios_planos,
               cobertura_planos_pct, idh, empregos_saude, ano_referencia
        FROM indicadores_economicos WHERE codigo_ibge = :cod ORDER BY id DESC
    """), {"cod": codigo_ibge}).mappings().first()

    score = db.execute(text("""
        SELECT score_total, score_demografico, score_infraestrutura_saude, score_economico,
               score_logistico, score_competitividade, tier, ranking_nacional, ranking_estadual,
               pca_component_1, pca_component_2, pca_component_3
        FROM scores WHERE codigo_ibge = :cod ORDER BY id DESC
    """), {"cod": codigo_ibge}).mappings().first()

    score_dict = dict(score) if score else {}
    breakdown_fields = ScoreBreakdown.model_fields.keys()

    result = MunicipioDetail(
        **dict(row),
        demograficos=DemograficosOut(**dict(demo)) if demo else None,
        estabelecimentos=EstabelecimentosOut(**dict(estab)) if estab else None,
        economicos=EconomicosOut(**dict(econ)) if econ else None,
        score=ScoreBreakdown(**{k: score_dict.get(k) for k in breakdown_fields}) if score else None,
        score_total=score_dict.get("score_total"),
        tier=score_dict.get("tier"),
        ranking_nacional=score_dict.get("ranking_nacional"),
        ranking_estadual=score_dict.get("ranking_estadual"),
    )

    cache_set(cache_key, result.model_dump(), TTL_MUNICIPIO_DETAIL)
    return result

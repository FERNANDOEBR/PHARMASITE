"""
GET /setores/{codigo_ibge}
Retorna microeconomia por bairro (setores censitários) de um município,
com contagem de PDVs OSM por setor.

Este é o dado que o cliente disse nunca ter encontrado facilmente:
"Microeconomia regional por bairros" — os dois mundos dentro de Campinas.
"""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Connection

from cache import TTL_LONG, cache_get, cache_set
from database import get_db
from schemas import SetorResponse, SetoresListResponse

router = APIRouter()


def _row_to_setor(row) -> dict:
    return {
        "codigo_setor":          row.codigo_setor,
        "codigo_ibge":           row.codigo_ibge,
        "uf":                    row.uf,
        "situacao":              row.situacao,
        "populacao_total":       row.populacao_total,
        "domicilios_total":      row.domicilios_total,
        "renda_media_domiciliar": row.renda_media_domiciliar,
        "area_km2":              float(row.area_km2) if row.area_km2 else None,
        # PDVs OSM
        "total_pdvs":            int(row.total_pdvs) if row.total_pdvs else 0,
        "farmacias":             int(row.farmacias) if row.farmacias else 0,
        "clinicas":              int(row.clinicas) if row.clinicas else 0,
        "dentistas":             int(row.dentistas) if row.dentistas else 0,
        "hospitais":             int(row.hospitais) if row.hospitais else 0,
        "laboratorios":          int(row.laboratorios) if row.laboratorios else 0,
        "pdvs_por_km2":          float(row.pdvs_por_km2) if row.pdvs_por_km2 else None,
        "farmacias_por_10k":     float(row.farmacias_por_10k) if row.farmacias_por_10k else None,
        # Geometria GeoJSON (opcional — pode ser grande)
        "geom_geojson":          None,  # retornado apenas se ?geom=true
    }


@router.get("/setores/{codigo_ibge}", tags=["Microeconomia por Bairro"])
def get_setores_municipio(
    codigo_ibge: str,
    geom: bool = Query(False, description="Incluir geometria GeoJSON dos polígonos"),
    situacao: Optional[str] = Query(None, description="Filtrar por URBANO ou RURAL"),
    db: Connection = Depends(get_db),
):
    """
    Retorna todos os setores censitários de um município com contagem de PDVs OSM.

    Este é o nível de microeconomia regional que ferramentas como IQVA e Close Up
    não oferecem: os 'dois mundos dentro de Campinas' na mesma consulta.
    """
    cache_key = f"setores:{codigo_ibge}:geom={geom}:sit={situacao}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    # Valida município
    mun = db.execute(
        text("SELECT nome, uf FROM municipios WHERE codigo_ibge = :c"),
        {"c": codigo_ibge}
    ).fetchone()

    if not mun:
        raise HTTPException(status_code=404, detail=f"Município {codigo_ibge} não encontrado")

    # Verifica se temos setores para este município
    count_setores = db.execute(
        text("SELECT COUNT(*) FROM setores_censitarios WHERE codigo_ibge = :c"),
        {"c": codigo_ibge}
    ).scalar()

    if count_setores == 0:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Nenhum setor censitário disponível para {mun.nome}-{mun.uf}. "
                "Dados disponíveis apenas para SP (Entrega 2). "
                "Outros estados em breve."
            )
        )

    # Monta query: usa a view pdvs_por_setor + geometria opcional
    geom_col = (
        "ST_AsGeoJSON(ps.geom)::json AS geom_geojson"
        if geom else
        "NULL AS geom_geojson"
    )

    sit_filter = ""
    params = {"c": codigo_ibge}
    if situacao:
        sit_filter = "AND ps.situacao ILIKE :sit"
        params["sit"] = f"%{situacao}%"

    query = text(f"""
        SELECT
            ps.codigo_setor,
            ps.codigo_ibge,
            ps.uf,
            ps.situacao,
            ps.populacao_total,
            ps.domicilios_total,
            ps.renda_media_domiciliar,
            ps.area_km2,
            ps.total_pdvs,
            ps.farmacias,
            ps.clinicas,
            ps.dentistas,
            ps.hospitais,
            ps.laboratorios,
            ps.pdvs_por_km2,
            ps.farmacias_por_10k,
            {geom_col}
        FROM pdvs_por_setor ps
        WHERE ps.codigo_ibge = :c
        {sit_filter}
        ORDER BY ps.farmacias DESC, ps.total_pdvs DESC
    """)

    rows = db.execute(query, params).fetchall()

    setores = []
    for row in rows:
        d = _row_to_setor(row)
        if geom and row.geom_geojson:
            d["geom_geojson"] = row.geom_geojson
        setores.append(d)

    # Estatísticas resumidas
    pop_total   = sum(s["populacao_total"] or 0 for s in setores)
    total_farma = sum(s["farmacias"] for s in setores)
    setores_urb = sum(1 for s in setores if s.get("situacao") and "URBA" in s["situacao"].upper())

    result = {
        "codigo_ibge":       codigo_ibge,
        "nome":              mun.nome,
        "uf":                mun.uf,
        "total_setores":     len(setores),
        "setores_urbanos":   setores_urb,
        "setores_rurais":    len(setores) - setores_urb,
        "populacao_total":   pop_total,
        "total_farmacias_osm": total_farma,
        "nota": (
            "PDVs via OpenStreetMap. Complementa dados CNES com geolocalização "
            "precisa por setor censitário."
        ),
        "setores": setores,
    }

    cache_set(cache_key, result, ttl=TTL_LONG)
    return result


@router.get("/setores/{codigo_ibge}/hotspots", tags=["Microeconomia por Bairro"])
def get_hotspots_municipio(
    codigo_ibge: str,
    top_n: int = Query(10, ge=1, le=50, description="Número de setores a retornar"),
    db: Connection = Depends(get_db),
):
    """
    Retorna os N setores com maior densidade de PDVs — os 'hotspots' logísticos.

    Útil para o vendedor priorizar quais bairros visitar primeiro numa rota nova.
    """
    mun = db.execute(
        text("SELECT nome, uf FROM municipios WHERE codigo_ibge = :c"),
        {"c": codigo_ibge}
    ).fetchone()

    if not mun:
        raise HTTPException(status_code=404, detail=f"Município {codigo_ibge} não encontrado")

    rows = db.execute(text("""
        SELECT
            ps.codigo_setor,
            ps.situacao,
            ps.populacao_total,
            ps.area_km2,
            ps.total_pdvs,
            ps.farmacias,
            ps.pdvs_por_km2,
            ps.farmacias_por_10k,
            ST_AsGeoJSON(ST_Centroid(ps.geom))::json AS centroid_geojson
        FROM pdvs_por_setor ps
        WHERE ps.codigo_ibge = :c
          AND ps.farmacias > 0
        ORDER BY ps.farmacias DESC, ps.pdvs_por_km2 DESC NULLS LAST
        LIMIT :n
    """), {"c": codigo_ibge, "n": top_n}).fetchall()

    hotspots = []
    for row in rows:
        hotspots.append({
            "codigo_setor":      row.codigo_setor,
            "situacao":          row.situacao,
            "populacao_total":   row.populacao_total,
            "area_km2":          float(row.area_km2) if row.area_km2 else None,
            "farmacias":         int(row.farmacias) if row.farmacias else 0,
            "total_pdvs":        int(row.total_pdvs) if row.total_pdvs else 0,
            "pdvs_por_km2":      float(row.pdvs_por_km2) if row.pdvs_por_km2 else None,
            "farmacias_por_10k": float(row.farmacias_por_10k) if row.farmacias_por_10k else None,
            "centroid":          row.centroid_geojson,
        })

    return {
        "codigo_ibge": codigo_ibge,
        "nome":        mun.nome,
        "uf":          mun.uf,
        "top_n":       top_n,
        "hotspots":    hotspots,
        "descricao": (
            f"Top {top_n} setores por concentração de farmácias em {mun.nome}. "
            "Prioridade de rota: setor com mais PDVs próximos = rota mais rentável."
        ),
    }

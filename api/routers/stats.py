"""
GET /stats — pipeline summary, table counts, data quality metrics.
"""
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.engine import Connection

from cache import TTL_STATS, cache_get, cache_set
from database import get_db
from schemas import PipelineLogEntry, StatsResponse, TableCount

router = APIRouter()


@router.get("/stats", response_model=StatsResponse)
def get_stats(db: Connection = Depends(get_db)):
    cache_key = "stats:global"
    cached = cache_get(cache_key)
    if cached:
        return StatsResponse(**cached)

    total_muns = db.execute(text("SELECT COUNT(*) FROM municipios")).scalar_one()

    data_counts = [
        TableCount(
            tabela="municipios",
            total=total_muns,
            com_dados=total_muns,
        ),
        TableCount(
            tabela="demograficos",
            total=db.execute(text("SELECT COUNT(*) FROM demograficos")).scalar_one(),
            com_dados=db.execute(
                text("SELECT COUNT(*) FROM demograficos WHERE populacao_total IS NOT NULL")
            ).scalar_one(),
        ),
        TableCount(
            tabela="estabelecimentos_saude",
            total=db.execute(text("SELECT COUNT(*) FROM estabelecimentos_saude")).scalar_one(),
            com_dados=db.execute(
                text("SELECT COUNT(*) FROM estabelecimentos_saude WHERE total_estabelecimentos > 0")
            ).scalar_one(),
        ),
        TableCount(
            tabela="indicadores_economicos",
            total=db.execute(text("SELECT COUNT(*) FROM indicadores_economicos")).scalar_one(),
            com_dados=db.execute(
                text("SELECT COUNT(*) FROM indicadores_economicos WHERE pib_per_capita IS NOT NULL")
            ).scalar_one(),
        ),
        TableCount(
            tabela="scores",
            total=db.execute(text("SELECT COUNT(*) FROM scores")).scalar_one(),
            com_dados=db.execute(
                text("SELECT COUNT(*) FROM scores WHERE score_total IS NOT NULL")
            ).scalar_one(),
        ),
    ]

    total_scores = next(t.total for t in data_counts if t.tabela == "scores")
    scored_pct = round(total_scores / max(total_muns, 1) * 100, 1)

    null_score = db.execute(
        text("SELECT COUNT(*) FROM scores WHERE score_total IS NULL")
    ).scalar_one()
    null_pop = db.execute(
        text("SELECT COUNT(*) FROM demograficos WHERE populacao_total IS NULL")
    ).scalar_one()

    data_quality = {
        "municipios_total": total_muns,
        "pct_com_score": scored_pct,
        "scores_sem_valor": null_score,
        "demograficos_sem_populacao": null_pop,
    }

    log_rows = db.execute(text("""
        SELECT etapa, status, municipios_processados, mensagem,
               started_at::TEXT, finished_at::TEXT
        FROM pipeline_log
        ORDER BY id DESC
        LIMIT 10
    """)).mappings().all()

    pipeline_log = [PipelineLogEntry(**dict(r)) for r in log_rows]

    last_run = db.execute(text("""
        SELECT MAX(finished_at)::TEXT FROM pipeline_log WHERE status = 'done'
    """)).scalar_one()

    result = StatsResponse(
        data_counts=data_counts,
        pipeline_log=pipeline_log,
        last_pipeline_run=last_run,
        data_quality=data_quality,
    )
    cache_set(cache_key, result.model_dump(), TTL_STATS)
    return result

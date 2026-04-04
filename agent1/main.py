"""
PHARMASITE INTELLIGENCE - AGENTE 1: DATA PIPELINE
Orquestrador principal do ETL de dados públicos brasileiros.
Usa Gemini 2.5 Pro para contexto longo e tomada de decisão no pipeline.
"""

import os
import sys
import time
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

from pipeline.municipios import collect_municipios
from pipeline.demograficos import collect_demograficos
from pipeline.cnes import collect_cnes
from pipeline.economicos import collect_economicos
from pipeline.scores import calculate_scores
from pipeline.setores_sp import collect_setores_sp
from pipeline.cnpj_growth import collect_cnpj_growth
from db.connection import get_engine, log_pipeline

load_dotenv()

# ─── Logger setup ───────────────────────────────────────────────────────────
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
logger.add("data/pipeline.log", rotation="10 MB")

# ─── Pipeline steps ──────────────────────────────────────────────────────────
STEPS = [
    ("municipios",    collect_municipios,    "📍 Municípios IBGE (5.570)"),
    ("demograficos",  collect_demograficos,  "👥 Demográficos Censo 2022"),
    ("cnes",          collect_cnes,          "🏥 Estabelecimentos CNES/DataSUS"),
    ("economicos",    collect_economicos,    "💰 Indicadores Econômicos"),
    ("scores",        calculate_scores,      "🎯 Calculando Scores (Modelo Aditivo v3)"),
    ("setores_sp",    collect_setores_sp,    "🗺️  Microeconomia por Bairro (Setores SP + PDVs OSM)"),
    ("cnpj_growth",   collect_cnpj_growth,   "📈 Agent Scout: Sinais de Crescimento (Receita Federal SP)"),
]


def run_pipeline():
    logger.info("=" * 60)
    logger.info("🚀 PHARMASITE INTELLIGENCE - AGENTE 1 INICIANDO")
    logger.info(f"   Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    engine = get_engine()
    os.makedirs("data", exist_ok=True)

    total_start = time.time()

    for step_name, step_fn, step_label in STEPS:
        logger.info(f"\n{'─'*50}")
        logger.info(f"ETAPA: {step_label}")
        logger.info(f"{'─'*50}")

        start = time.time()
        log_pipeline(engine, step_name, "running")

        try:
            result = step_fn(engine)
            elapsed = time.time() - start
            log_pipeline(engine, step_name, "done",
                        municipios=result.get("count", 0),
                        msg=result.get("message", "OK"))
            logger.success(f"✅ {step_label} — {elapsed:.1f}s | {result.get('count', 0)} registros")

        except Exception as e:
            elapsed = time.time() - start
            log_pipeline(engine, step_name, "error", msg=str(e))
            logger.error(f"❌ ERRO em {step_name}: {e}")
            logger.exception(e)
            # Continue pipeline — don't die on partial failures
            continue

    total_elapsed = time.time() - total_start
    logger.info(f"\n{'='*60}")
    logger.success(f"🏁 PIPELINE CONCLUÍDO em {total_elapsed/60:.1f} minutos")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()

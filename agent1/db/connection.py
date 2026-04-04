import os
from datetime import datetime
from sqlalchemy import create_engine, text
from loguru import logger


def get_engine():
    url = os.getenv("DATABASE_URL", "postgresql://pharma:pharma123@db:5432/pharmasite")
    engine = create_engine(url, pool_pre_ping=True, pool_size=5)
    logger.debug(f"DB conectado: {url.split('@')[1]}")
    return engine


def log_pipeline(engine, etapa: str, status: str, municipios: int = 0, msg: str = ""):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_log (etapa, status, municipios_processados, mensagem, finished_at)
            VALUES (:etapa, :status, :municipios, :msg, CASE WHEN :status != 'running' THEN NOW() ELSE NULL END)
            ON CONFLICT DO NOTHING
        """), {"etapa": etapa, "status": status, "municipios": municipios, "msg": msg})

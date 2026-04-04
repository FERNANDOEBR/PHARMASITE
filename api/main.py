"""
PHARMASITE INTELLIGENCE — FastAPI Phase 2 (Agente 2)

Endpoints:
  GET  /municipios                  — paginated list with filters
  GET  /municipios/{codigo_ibge}    — full detail
  GET  /score/{codigo_ibge}         — score + tier + breakdown
  GET  /ranking                     — top-N ranked municipalities
  POST /optimize                    — filter + rank by criteria
  GET  /stats                       — pipeline summary + data quality
  POST /insights/{codigo_ibge}      — Claude PT-BR market narrative
  GET  /setores/{codigo_ibge}       — microeconomia por bairro (setores censitários SP)
  GET  /setores/{codigo_ibge}/hotspots — top N bairros por densidade de PDVs
  GET  /health                      — liveness
"""
import sys
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from cache import get_redis
from database import get_engine
from routers import insights, municipios, optimize, scores, scout, setores, stats, tradearea, tradearea_insights

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup checks ────────────────────────────────────────────────────────
    logger.info("PHARMASITE API iniciando...")

    try:
        from sqlalchemy import text
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.success("Conexão DB verificada")
    except Exception as e:
        logger.error(f"Falha na conexão DB: {e}")

    try:
        get_redis().ping()
        logger.success("Conexão Redis verificada")
    except Exception as e:
        logger.warning(f"Redis indisponível (não-fatal): {e}")

    yield
    logger.info("PHARMASITE API encerrando")


app = FastAPI(
    title="PHARMASITE Intelligence API",
    description=(
        "Plataforma de inteligência de mercado farmacêutico brasileiro. "
        "5.570 municípios com scores, rankings e análises por IA."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(municipios.router, tags=["Municípios"])
app.include_router(scores.router,     tags=["Scores"])
app.include_router(optimize.router,   tags=["Otimização"])
app.include_router(tradearea.router,  tags=["Trade Area (Gravity Model)"])
app.include_router(stats.router,      tags=["Pipeline & Stats"])
app.include_router(insights.router,            tags=["IA Insights"])
app.include_router(tradearea_insights.router,  tags=["IA Insights"])
app.include_router(setores.router,             tags=["Microeconomia por Bairro"])
app.include_router(scout.router,               tags=["Agent Scout"])


@app.get("/health", tags=["Sistema"])
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}

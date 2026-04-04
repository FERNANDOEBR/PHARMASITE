"""
Database connection — synchronous SQLAlchemy 2.0.
Mirrors pattern from agent1/db/connection.py.
"""
import os

from sqlalchemy import create_engine, text
from loguru import logger

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = os.getenv("DATABASE_URL", "postgresql://pharma:pharma123@db:5432/pharmasite")
        _engine = create_engine(url, pool_pre_ping=True, pool_size=10, max_overflow=20)
        logger.info(f"DB engine created: {url.split('@')[1]}")
    return _engine


def get_db():
    """
    FastAPI dependency. Yields a SQLAlchemy connection per request.

    Usage:
        @router.get("/foo")
        def handler(db=Depends(get_db)):
            rows = db.execute(text("SELECT ...")).mappings().all()
    """
    with get_engine().connect() as conn:
        yield conn

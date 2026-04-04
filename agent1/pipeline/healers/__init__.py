"""
Phase 3 — Self-Healing Data Pipeline
=====================================
Intercepts suspicious zero-values produced by upstream API failures (CNES 503,
IBGE SIDRA timeouts) and dispatches AI agents to estimate the real figures
before they are written to the database.

Components
----------
validator   — SuspiciousZeroValidator   (Data Validation Gate)
researcher  — DataResearcher            (Agent 3: web-search estimator)
verifier    — DataVerifier              (Agent 4: cross-reference confidence)
orchestrator — HealingOrchestrator     (coordinates the full flow)
"""
from .orchestrator import HealingOrchestrator
from .validator import SuspiciousZeroValidator, AnomalyReport

__all__ = ["HealingOrchestrator", "SuspiciousZeroValidator", "AnomalyReport"]

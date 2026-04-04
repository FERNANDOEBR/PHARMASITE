"""
HealingOrchestrator
===================
Coordinates the full self-healing flow for the PHARMASITE data pipeline.

Flow (mirrors the architecture diagram)
-----------------------------------------
  CNES raw counts
       │
       ▼
  SuspiciousZeroValidator ── normal? ──► pass counts through unchanged
       │ suspicious
       ▼
  Anomaly Queue (in-process list)
       │
       ▼
  DataResearcher (Agent 3) — DuckDuckGo web search → estimated count
       │
       ▼
  DataVerifier   (Agent 4) — cross-reference search → confidence score
       │
    ┌──┴───────────────────┐
    │ confidence ≥ 80%      │ confidence < 80%
    ▼                       ▼
 Accept healed value     Flag for manual review (keep original 0)
    │
    ▼
  Updated counts dict  ──► cnes.py DB insert

Cost controls
-------------
- Agents are lazy-initialised (no LLM call unless healing is needed).
- HEALER_MAX_PER_RUN (env var, default 20) caps municipalities healed per
  pipeline run to limit LLM API spend.
- Set HEALER_ENABLED=false to disable the healer entirely without code change.
- Only fields listed in HEALABLE_FIELDS are researched (skip low-value ones).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from loguru import logger

from .validator import SuspiciousZeroValidator, AnomalyReport
from .researcher import DataResearcher
from .verifier import DataVerifier

# Fields the healer will attempt to estimate (highest-value first)
HEALABLE_FIELDS = [
    "farmacias",
    "clinicas",
    "hospitais",
    "laboratorios",
    "consultorios_medicos",
    "consultorios_odonto",
    "ubs_upa",
]

# Cost-control defaults (override via environment variables)
_MAX_PER_RUN: int = int(os.getenv("HEALER_MAX_PER_RUN", "20"))
_ENABLED: bool = os.getenv("HEALER_ENABLED", "true").lower() != "false"


@dataclass
class HealingResult:
    codigo_ibge: str
    nome: str
    uf: str
    original: dict
    healed: dict
    healed_fields: list[str] = field(default_factory=list)
    confidence_scores: dict[str, float] = field(default_factory=dict)
    flagged_for_review: list[str] = field(default_factory=list)
    was_healed: bool = False

    def summary(self) -> str:
        if self.was_healed:
            changes = {f: self.healed[f] for f in self.healed_fields}
            return f"{self.nome}-{self.uf}: healed {changes}"
        if self.flagged_for_review:
            return f"{self.nome}-{self.uf}: flagged {self.flagged_for_review} for review"
        return f"{self.nome}-{self.uf}: no anomaly"


class HealingOrchestrator:
    """
    Entry point for Phase 3 self-healing.

    Typical usage inside cnes.py
    -----------------------------
    >>> orchestrator = HealingOrchestrator()
    >>> healed_counts = orchestrator.heal_batch(
    ...     municipalities=[
    ...         {"codigo_ibge": "3509502", "nome": "Campinas", "uf": "SP",
    ...          "counts": {...}, "population": 1200000},
    ...     ],
    ...     api_failed_ufs={"SP"},
    ... )
    >>> # healed_counts is dict[codigo_ibge -> counts dict]
    """

    def __init__(self):
        self._validator = SuspiciousZeroValidator()
        self._researcher: Optional[DataResearcher] = None
        self._verifier: Optional[DataVerifier] = None
        self._agents_ready = False

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _init_agents(self) -> bool:
        """Lazy-initialise LLM agents.  Returns False if init fails."""
        if self._agents_ready:
            return True
        if not os.getenv("ANTHROPIC_API_KEY"):
            logger.warning("HEALER: ANTHROPIC_API_KEY not set — healing disabled")
            return False
        try:
            self._researcher = DataResearcher()
            self._verifier = DataVerifier()
            self._agents_ready = True
            logger.info("🤖 Healing agents initialised (Researcher + Verifier)")
            return True
        except Exception as exc:
            logger.error(f"HEALER: agent init failed — {exc}")
            return False

    # ── Public API ───────────────────────────────────────────────────────────

    def heal_municipality(
        self,
        codigo_ibge: str,
        nome: str,
        uf: str,
        counts: dict,
        population: Optional[int] = None,
        api_failed: bool = False,
    ) -> HealingResult:
        """
        Attempt to heal suspicious zeros for a single municipality.

        Parameters
        ----------
        counts      : establishment count dict (keys = DB column names)
        api_failed  : True if the upstream API batch returned a non-200 status
        """
        result = HealingResult(
            codigo_ibge=codigo_ibge,
            nome=nome,
            uf=uf,
            original=counts.copy(),
            healed=counts.copy(),
        )

        # Step 1 — Validation Gate
        anomaly: Optional[AnomalyReport] = self._validator.is_suspicious(
            codigo_ibge, nome, uf, counts,
            api_failed=api_failed,
            population=population,
        )
        if not anomaly:
            return result   # Record is healthy — pass through unchanged

        logger.info(f"🩹 Anomaly detected: {anomaly}")

        # Step 2 — Init agents (lazy)
        if not self._init_agents():
            return result   # Agents unavailable — keep zeros

        # Step 3 — Research + Verify each suspicious field
        for field_name in HEALABLE_FIELDS:
            if counts.get(field_name, 0) != 0:
                continue    # Field already has data — skip

            # Agent 3: Researcher
            estimated, sources = self._researcher.research_field(
                city_name=nome,
                uf=uf,
                field=field_name,
                population=population,
            )

            # Agent 4: Verifier
            final_value, confidence, disposition = self._verifier.verify(
                city_name=nome,
                uf=uf,
                field=field_name,
                researcher_estimate=estimated,
                researcher_sources=sources,
                population=population,
            )

            result.confidence_scores[field_name] = confidence

            if disposition == "accept" and final_value > 0:
                result.healed[field_name] = final_value
                result.healed_fields.append(field_name)
                result.was_healed = True
            else:
                result.flagged_for_review.append(field_name)

        # Recalculate total if any field was healed
        if result.was_healed:
            total_fields = [
                "farmacias", "clinicas", "hospitais", "laboratorios",
                "consultorios_medicos", "consultorios_odonto", "ubs_upa",
            ]
            result.healed["total_estabelecimentos"] = sum(
                result.healed.get(f, 0) for f in total_fields
            )
            logger.success(f"✅ {result.summary()}")
        elif result.flagged_for_review:
            logger.warning(f"⚠️  {result.summary()}")

        return result

    def heal_batch(
        self,
        municipalities: list[dict],
        api_failed_ufs: Optional[set] = None,
    ) -> dict[str, dict]:
        """
        Heal an entire batch of municipalities.

        Parameters
        ----------
        municipalities : list of dicts with keys:
            codigo_ibge, nome, uf, counts, population (optional)
        api_failed_ufs : set of UF codes where the batch API call failed

        Returns
        -------
        dict[codigo_ibge -> healed_counts_dict]
        """
        if not _ENABLED:
            logger.info("HEALER: disabled via HEALER_ENABLED=false")
            return {m["codigo_ibge"]: m["counts"] for m in municipalities}

        api_failed_ufs = api_failed_ufs or set()
        healed_counts: dict[str, dict] = {}
        heal_count = 0
        flag_count = 0

        # Sort by population descending — heal high-priority municipalities first
        sorted_muns = sorted(
            municipalities,
            key=lambda m: m.get("population") or 0,
            reverse=True,
        )

        # ── Heal up to MAX_PER_RUN municipalities ────────────────────────────
        for mun in sorted_muns[:_MAX_PER_RUN]:
            cod = mun["codigo_ibge"]
            api_failed = mun.get("uf", "") in api_failed_ufs

            healing = self.heal_municipality(
                codigo_ibge=cod,
                nome=mun["nome"],
                uf=mun["uf"],
                counts=mun["counts"],
                population=mun.get("population"),
                api_failed=api_failed,
            )
            healed_counts[cod] = healing.healed
            if healing.was_healed:
                heal_count += 1
            if healing.flagged_for_review:
                flag_count += 1

        # ── Pass remaining municipalities through unchanged ───────────────────
        for mun in sorted_muns[_MAX_PER_RUN:]:
            healed_counts[mun["codigo_ibge"]] = mun["counts"]

        logger.info(
            f"🩹 Healing batch complete — "
            f"healed: {heal_count}, "
            f"flagged for review: {flag_count}, "
            f"unchanged: {len(municipalities) - min(len(municipalities), _MAX_PER_RUN)}"
        )
        return healed_counts

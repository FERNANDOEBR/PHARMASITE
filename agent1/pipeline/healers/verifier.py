"""
Agent 4: The Verifier
=====================
Cross-references the Researcher's estimate with a second independent search
and computes a confidence score.  Values below the threshold (default 80 %)
are rejected and flagged for manual human review instead of being written to
the database.

Confidence scoring heuristics (additive)
-----------------------------------------
0.50  — base (researcher returned a positive estimate)
+0.15 — researcher cited at least one source snippet
+0.15 — verifier's independent search returned any content
+0.10 — the city name appears in the verifier's search result
+0.10 — verifier result contains a numeric count that is close to
         the researcher estimate (within 50 %)

Maximum possible: 1.00, capped at 0.95.
"""
from __future__ import annotations

import re
from typing import Optional

from loguru import logger
from langchain_community.tools import DuckDuckGoSearchRun

CONFIDENCE_THRESHOLD: float = 0.80

# Verifier search template — deliberately different query from researcher
_VERIFY_QUERY = (
    "{field_type} {city_name} {uf} site:doctoralia.com.br OR "
    "site:telessaude.org.br OR site:saude.gov.br"
)


class DataVerifier:
    """
    Agent 4: The Verifier.

    Runs a lightweight second search and applies the confidence threshold
    defined by the architecture (80 %).

    Parameters
    ----------
    confidence_threshold : float
        Minimum confidence to accept the healed value (default 0.80).
    """

    def __init__(self, confidence_threshold: float = CONFIDENCE_THRESHOLD):
        self.threshold = confidence_threshold
        self._search = DuckDuckGoSearchRun()

    def verify(
        self,
        city_name: str,
        uf: str,
        field: str,
        researcher_estimate: int,
        researcher_sources: list[str],
        population: Optional[int] = None,
    ) -> tuple[int, float, str]:
        """
        Verify the researcher's estimate.

        Returns
        -------
        (final_value, confidence, disposition)
            final_value  : accepted count (0 if rejected)
            confidence   : float 0–1
            disposition  : "accept" | "flag_for_review"
        """
        if researcher_estimate <= 0:
            logger.warning(f"    Verifier: researcher estimate is 0 — flagging for review")
            return 0, 0.0, "flag_for_review"

        # ── Run independent verification search ──────────────────────────────
        field_type = field.replace("_", " ")
        query = _VERIFY_QUERY.format(
            field_type=field_type, city_name=city_name, uf=uf
        )
        try:
            snippet = self._search.run(query)
        except Exception as exc:
            logger.warning(f"    Verifier search failed: {exc}")
            snippet = ""

        # ── Compute confidence score ─────────────────────────────────────────
        confidence = 0.50

        if researcher_estimate > 0 and researcher_sources:
            confidence += 0.15

        if snippet and len(snippet) > 50:
            confidence += 0.15

        if city_name.lower() in snippet.lower():
            confidence += 0.10

        # Check if verifier independently finds a similar count
        nums_in_snippet = [int(n) for n in re.findall(r"\b\d+\b", snippet)]
        if nums_in_snippet:
            closest = min(nums_in_snippet, key=lambda n: abs(n - researcher_estimate))
            if researcher_estimate > 0:
                ratio = closest / researcher_estimate
                if 0.5 <= ratio <= 2.0:   # within 50 % either direction
                    confidence += 0.10

        confidence = min(confidence, 0.95)

        # ── Decision ─────────────────────────────────────────────────────────
        if confidence >= self.threshold:
            logger.success(
                f"    ✅ Verifier ACCEPT: {researcher_estimate} "
                f"{field} in {city_name}-{uf} (conf: {confidence:.0%})"
            )
            return researcher_estimate, confidence, "accept"
        else:
            logger.warning(
                f"    ⚠️  Verifier REJECT: confidence {confidence:.0%} < "
                f"{self.threshold:.0%} — flagged for manual review"
            )
            return 0, confidence, "flag_for_review"

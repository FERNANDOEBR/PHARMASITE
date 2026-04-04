"""
Data Validation Gate — detects suspicious zero-values in health establishment
data before they are written to the database.

Rules
-----
Any municipality returning total_estabelecimentos == 0 is suspicious:
Brazil mandates at minimum one UBS (basic health unit) per municipality, and
virtually every settlement has at least one registered pharmacy.  A complete
zero can only mean the upstream API silently failed.

If the entire UF batch returned a non-200 status (api_failed=True) that
context is recorded in the AnomalyReport for the healer's prompt.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# Fields checked for zero-values (in reporting order)
ESTABLISHMENT_FIELDS = [
    "farmacias",
    "clinicas",
    "hospitais",
    "laboratorios",
    "consultorios_medicos",
    "consultorios_odonto",
    "ubs_upa",
]


@dataclass
class AnomalyReport:
    codigo_ibge: str
    nome: str
    uf: str
    fields_suspicious: list[str]
    reason: str
    population: Optional[int] = None
    api_failed: bool = False

    def __str__(self) -> str:
        pop = f", pop≈{self.population:,}" if self.population else ""
        return (
            f"[{self.codigo_ibge}] {self.nome}-{self.uf}{pop} | "
            f"suspicious={self.fields_suspicious} | {self.reason}"
        )


class SuspiciousZeroValidator:
    """
    Validates a dict of establishment counts for a single municipality.

    Parameters
    ----------
    min_nonzero_fields : int
        How many establishment fields must be non-zero for the record to be
        considered healthy.  Defaults to 1 (at least one type of facility).
    """

    def __init__(self, min_nonzero_fields: int = 1):
        self.min_nonzero_fields = min_nonzero_fields

    def is_suspicious(
        self,
        codigo_ibge: str,
        nome: str,
        uf: str,
        counts: dict,
        api_failed: bool = False,
        population: Optional[int] = None,
    ) -> Optional[AnomalyReport]:
        """
        Returns an AnomalyReport if the counts look suspicious, else None.
        """
        nonzero = [f for f in ESTABLISHMENT_FIELDS if counts.get(f, 0) > 0]

        if len(nonzero) >= self.min_nonzero_fields:
            return None  # Looks fine

        suspicious = [f for f in ESTABLISHMENT_FIELDS if counts.get(f, 0) == 0]

        if api_failed:
            reason = "All-zero counts from confirmed API batch failure (e.g. 503)"
        else:
            reason = "All-zero counts with no known API error — possible silent failure"

        return AnomalyReport(
            codigo_ibge=codigo_ibge,
            nome=nome,
            uf=uf,
            fields_suspicious=suspicious,
            reason=reason,
            population=population,
            api_failed=api_failed,
        )

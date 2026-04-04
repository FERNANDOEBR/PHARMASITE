"""
Coleta estabelecimentos de saúde do CNES/DataSUS.
Download do CSV mensal: ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_*.ZIP
Fallback: API REST CNES por município.
"""

import io
import os
import zipfile
import requests
import pandas as pd
from sqlalchemy import text
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import time

from pipeline.healers import HealingOrchestrator

# CNES tipos de estabelecimento relevantes para correlatos
TIPOS_RELEVANTES = {
    "01": "ubs_upa",        # UBS
    "02": "ubs_upa",        # Hospital Geral
    "04": "hospitais",      # Hospital Especializado
    "05": "hospitais",      # Pronto Socorro
    "07": "ubs_upa",        # UPA
    "20": "clinicas",       # Pronto Atendimento
    "21": "consultorios_medicos",  # Consultório Isolado
    "22": "consultorios_odonto",   # Consultório Odontológico
    "23": "consultorios_medicos",  # Consultório Médico
    "36": "clinicas",       # Clínica/Centro Especialidade
    "39": "laboratorios",   # Laboratório Clínico
    "43": "farmacias",      # Farmácia
    "60": "laboratorios",   # Laboratório de Prótese
    "70": "consultorios_odonto",   # Centro Atenção Odonto
    "71": "consultorios_odonto",   # CEO
    "79": "farmacias",      # Farmácia Magistral
}

CNES_CSV_URL = "https://cnes.datasus.gov.br/services/estabelecimentos-exportar?estado=&municipio=&natureza=&esfera=&tipo=&tipoPrestador=&status=1"
CNES_API_URL = "https://cnes.datasus.gov.br/services/estabelecimentos"


def collect_cnes(engine) -> dict:
    logger.info("Coletando estabelecimentos CNES/DataSUS...")

    # Get municipalities from DB (include nome for healer prompts)
    with engine.connect() as conn:
        muns = pd.read_sql("SELECT codigo_ibge, nome, uf FROM municipios", conn)

    logger.info(f"Processando {len(muns)} municípios")

    # Strategy: collect by UF to avoid overloading API
    all_counts: dict = {}
    api_failed_ufs: set[str] = set()   # Track UFs where batch API call failed

    for uf in sorted(muns["uf"].unique()):
        uf_muns = muns[muns["uf"] == uf]["codigo_ibge"].tolist()
        logger.info(f"  UF: {uf} ({len(uf_muns)} municípios)")

        try:
            uf_counts, uf_failed = collect_uf_cnes(uf, uf_muns)
            all_counts.update(uf_counts)
            if uf_failed:
                api_failed_ufs.add(uf)
            time.sleep(2)  # Be gentle with DataSUS
        except Exception as e:
            logger.warning(f"  Erro UF {uf}: {e}")
            api_failed_ufs.add(uf)
            continue

    # ── Phase 3: Self-Healing Gate ───────────────────────────────────────────
    # Always load population + name maps (needed for DB write whether healer runs or not)
    try:
        with engine.connect() as conn_pop:
            pop_df = pd.read_sql("SELECT codigo_ibge, populacao_total FROM demograficos", conn_pop)
        pop_map = dict(zip(pop_df["codigo_ibge"], pop_df["populacao_total"]))
    except Exception:
        pop_map = {}

    nome_map = dict(zip(muns["codigo_ibge"], muns["nome"]))
    uf_map   = dict(zip(muns["codigo_ibge"], muns["uf"]))

    total_ufs = len(sorted(muns["uf"].unique()))
    failed_pct = len(api_failed_ufs) / max(total_ufs, 1)

    if failed_pct >= 0.9:
        # CNES API is systemically down — skip per-municipality web searches
        # (would take days at ~30s each for 5,571 municipalities).
        logger.warning(
            f"⚠️  CNES: {len(api_failed_ufs)}/{total_ufs} estados com erro "
            f"({failed_pct*100:.0f}%) — API possivelmente off-line. "
            "Pulando self-healing. Zeros serão inseridos para todos os municípios. "
            "Re-execute o pipeline quando a API DataSUS estiver disponível."
        )
    else:
        municipalities_for_healing = [
            {
                "codigo_ibge": cod,
                "nome": nome_map.get(cod, cod),
                "uf": uf_map.get(cod, ""),
                "counts": counts,
                "population": pop_map.get(cod, None),
            }
            for cod, counts in all_counts.items()
        ]

        healer = HealingOrchestrator()
        all_counts = healer.heal_batch(
            municipalities=municipalities_for_healing,
            api_failed_ufs=api_failed_ufs,
        )
    # ─────────────────────────────────────────────────────────────────────────

    # Write aggregated counts to DB
    logger.info(f"💾 Inserindo dados CNES ({len(all_counts)} municípios)...")
    inserted = 0

    with engine.begin() as conn:
        for codigo_ibge, counts in all_counts.items():
            pop = pop_map.get(codigo_ibge, 0) or 1
            total = sum(v for k, v in counts.items() if k != "codigo_ibge")

            farmacias_por_10k = round(counts.get("farmacias", 0) / pop * 10000, 2)
            estab_por_10k = round(total / pop * 10000, 2)

            conn.execute(text("""
                INSERT INTO estabelecimentos_saude (
                    codigo_ibge, farmacias, consultorios_medicos, consultorios_odonto,
                    laboratorios, clinicas, hospitais, ubs_upa, total_estabelecimentos,
                    farmacias_por_10k, estabelecimentos_saude_por_10k, ano_referencia
                ) VALUES (
                    :codigo_ibge, :farmacias, :consultorios_medicos, :consultorios_odonto,
                    :laboratorios, :clinicas, :hospitais, :ubs_upa, :total,
                    :farmacias_por_10k, :estab_por_10k, 2024
                )
                ON CONFLICT (codigo_ibge) DO NOTHING
            """), {
                "codigo_ibge": codigo_ibge,
                "farmacias": counts.get("farmacias", 0),
                "consultorios_medicos": counts.get("consultorios_medicos", 0),
                "consultorios_odonto": counts.get("consultorios_odonto", 0),
                "laboratorios": counts.get("laboratorios", 0),
                "clinicas": counts.get("clinicas", 0),
                "hospitais": counts.get("hospitais", 0),
                "ubs_upa": counts.get("ubs_upa", 0),
                "total": total,
                "farmacias_por_10k": farmacias_por_10k,
                "estab_por_10k": estab_por_10k,
            })
            inserted += 1

    logger.success(f"CNES: {inserted} municípios com dados de estabelecimentos")
    return {"count": inserted, "message": "CNES coletado"}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=3, min=5, max=60))
def collect_uf_cnes(uf: str, municipios: list) -> tuple[dict, bool]:
    """
    Coleta estabelecimentos CNES para uma UF via API REST.

    Returns
    -------
    (counts, api_failed)
        counts     : dict {codigo_ibge: {tipo: count}}
        api_failed : True if the API returned a non-200 response
    """
    counts = {m: {t: 0 for t in set(TIPOS_RELEVANTES.values())} for m in municipios}
    api_failed = False

    # CNES API: busca por estado
    url = f"{CNES_API_URL}?estado={uf}&status=1&limit=10000"

    try:
        resp = requests.get(url, timeout=60, headers={"Accept": "application/json"})

        if resp.status_code == 200:
            data = resp.json()
            items = data if isinstance(data, list) else data.get("itens", data.get("data", []))

            for item in items:
                # Different possible field names in CNES API
                cod_mun = str(item.get("codigoMunicipio", item.get("co_municipio", ""))).zfill(7)
                tipo = str(item.get("tipoEstabelecimento", item.get("tp_pfpj", ""))).zfill(2)

                if cod_mun in counts and tipo in TIPOS_RELEVANTES:
                    cat = TIPOS_RELEVANTES[tipo]
                    counts[cod_mun][cat] = counts[cod_mun].get(cat, 0) + 1

                    # Special: farmácias magistrais (tipo 79)
                    if tipo == "79":
                        counts[cod_mun]["farmacias_magistrais"] = counts[cod_mun].get("farmacias_magistrais", 0) + 1

        else:
            logger.warning(f"CNES API {uf}: status {resp.status_code}")
            api_failed = True   # Signals healer that zeros are API-induced

    except requests.exceptions.RequestException as e:
        logger.warning(f"CNES request error {uf}: {e}")
        api_failed = True

    return counts, api_failed

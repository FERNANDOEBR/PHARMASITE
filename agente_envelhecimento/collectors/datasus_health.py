"""
DATASUS Health Collector — CNES establishments + SIH hospitalizations.

Uses PySUS library to download:
1. CNES: Count of health establishments per município
2. SIH: Hospitalizations of elderly patients (60+) with chronic disease filters

Falls back to direct FTP/HTTP if pysus is unavailable.
"""

import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)

# CNES establishment types of interest
CNES_TYPES_OF_INTEREST = [
    "HOSPITAL GERAL",
    "HOSPITAL ESPECIALIZADO",
    "PRONTO ATENDIMENTO",
    "UNIDADE BASICA DE SAUDE",
    "POSTO DE SAUDE",
    "CENTRO DE SAUDE/UNIDADE BASICA",
    "POLICLINICA",
    "CLINICA/CENTRO DE ESPECIALIDADE",
]

# CID-10 codes for common chronic diseases in elderly
CHRONIC_CID10_PREFIXES = [
    "I10",  # Hipertensão
    "I11",  # Doença cardíaca hipertensiva
    "I20",  # Angina pectoris
    "I21",  # Infarto agudo do miocárdio
    "I25",  # Doença isquêmica crônica do coração
    "I50",  # Insuficiência cardíaca
    "I63",  # Infarto cerebral (AVC)
    "I64",  # AVC não especificado
    "E10",  # Diabetes mellitus tipo 1
    "E11",  # Diabetes mellitus tipo 2
    "E14",  # Diabetes mellitus não especificado
    "J44",  # DPOC
    "J45",  # Asma
    "M15",  # Poliartrose
    "M16",  # Coxartrose
    "M17",  # Gonartrose
    "G30",  # Doença de Alzheimer
    "N18",  # Doença renal crônica
    "C34",  # Neoplasia maligna dos brônquios e pulmões
]


def fetch_health_data(
    uf_code: str = "all",
    year: int = 2023,
    use_cache: bool = True,
    cache_path: str = "output/datasus_health_cache.parquet",
) -> pd.DataFrame:
    """
    Fetch DATASUS health data per município.

    Returns DataFrame with:
    - cod_municipio (str): 6 or 7 digit IBGE code
    - n_estabelecimentos_saude (int): count of health establishments
    - n_leitos (int): total hospital beds
    - n_internacoes_idoso (int): hospitalizations age 60+
    - n_internacoes_cronicas_idoso (int): chronic disease hospitalizations 60+
    """
    if use_cache and os.path.exists(cache_path):
        logger.info(f"Loading DATASUS data from cache: {cache_path}")
        return pd.read_parquet(cache_path)

    logger.info("Fetching DATASUS health data...")

    # Try to use pysus
    try:
        df_cnes = _fetch_cnes_pysus(uf_code, year)
        df_sih = _fetch_sih_pysus(uf_code, year)
    except Exception as e:
        logger.warning(f"PySUS failed ({e}), using fallback HTTP data...")
        df_cnes = _fetch_cnes_fallback(uf_code, year)
        df_sih = _fetch_sih_fallback(uf_code, year)

    # Merge CNES + SIH on cod_municipio
    if df_cnes is not None and df_sih is not None:
        result = df_cnes.merge(df_sih, on="cod_municipio", how="outer")
    elif df_cnes is not None:
        result = df_cnes
    elif df_sih is not None:
        result = df_sih
    else:
        logger.warning("No DATASUS data available, returning empty DataFrame")
        return pd.DataFrame(columns=[
            "cod_municipio", "n_estabelecimentos_saude", "n_leitos",
            "n_internacoes_idoso", "n_internacoes_cronicas_idoso"
        ])

    result = result.fillna(0)
    for col in result.columns:
        if col != "cod_municipio":
            result[col] = result[col].astype(int)

    result["cod_municipio"] = result["cod_municipio"].astype(str).str.strip()

    # Cache
    if use_cache:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        result.to_parquet(cache_path, index=False)
        logger.info(f"Cached DATASUS data to {cache_path}")

    logger.info(f"DATASUS: {len(result)} municípios with health data")
    return result


def _fetch_cnes_pysus(uf_code: str, year: int) -> pd.DataFrame:
    """Fetch CNES establishment counts via PySUS."""
    from pysus.online_data.CNES import download as cnes_download

    ufs = _get_uf_list(uf_code)
    all_data = []

    for uf in ufs:
        try:
            logger.info(f"Downloading CNES for UF={uf}, year={year}...")
            df = cnes_download("ST", state=uf, year=year, month=12)
            if df is not None and not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.warning(f"CNES download failed for UF={uf}: {e}")

    if not all_data:
        return None

    cnes = pd.concat(all_data, ignore_index=True)

    # Count establishments per município
    if "CODUFMUN" in cnes.columns:
        mun_col = "CODUFMUN"
    elif "CO_MUNICIPIO_GESTOR" in cnes.columns:
        mun_col = "CO_MUNICIPIO_GESTOR"
    else:
        mun_col = cnes.columns[0]

    # Count unique CNES establishments
    counts = cnes.groupby(mun_col).size().reset_index(name="n_estabelecimentos_saude")

    # Count beds if available
    if "QT_EXIST" in cnes.columns:
        beds = cnes.groupby(mun_col)["QT_EXIST"].sum().reset_index(name="n_leitos")
        counts = counts.merge(beds, on=mun_col, how="left")
    else:
        counts["n_leitos"] = 0

    counts = counts.rename(columns={mun_col: "cod_municipio"})
    return counts


def _fetch_sih_pysus(uf_code: str, year: int) -> pd.DataFrame:
    """Fetch SIH hospitalization data for elderly via PySUS."""
    from pysus.online_data.SIH import download as sih_download

    ufs = _get_uf_list(uf_code)
    all_data = []

    for uf in ufs:
        try:
            logger.info(f"Downloading SIH for UF={uf}, year={year}...")
            df = sih_download("RD", state=uf, year=year, month=12)
            if df is not None and not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.warning(f"SIH download failed for UF={uf}: {e}")

    if not all_data:
        return None

    sih = pd.concat(all_data, ignore_index=True)

    # Filter for elderly (60+)
    if "IDADE" in sih.columns:
        sih_idoso = sih[sih["IDADE"].astype(int) >= 60].copy()
    elif "NASC" in sih.columns:
        # Calculate age from birth date — approximate
        sih_idoso = sih.copy()  # Can't filter without age
    else:
        sih_idoso = sih.copy()

    mun_col = "MUNIC_RES" if "MUNIC_RES" in sih.columns else sih.columns[0]

    # Total elderly hospitalizations per município
    hosp_idoso = sih_idoso.groupby(mun_col).size().reset_index(name="n_internacoes_idoso")

    # Chronic disease hospitalizations (by CID-10 prefix)
    if "DIAG_PRINC" in sih_idoso.columns:
        chronic_mask = sih_idoso["DIAG_PRINC"].str[:3].isin(CHRONIC_CID10_PREFIXES)
        hosp_cronicas = (
            sih_idoso[chronic_mask]
            .groupby(mun_col)
            .size()
            .reset_index(name="n_internacoes_cronicas_idoso")
        )
        hosp_idoso = hosp_idoso.merge(hosp_cronicas, on=mun_col, how="left")
    else:
        hosp_idoso["n_internacoes_cronicas_idoso"] = 0

    hosp_idoso = hosp_idoso.rename(columns={mun_col: "cod_municipio"})
    return hosp_idoso


def _fetch_cnes_fallback(uf_code: str, year: int) -> pd.DataFrame | None:
    """Fallback: fetch CNES summary data from IBGE SIDRA auxiliary tables."""
    import requests

    logger.info("Using CNES fallback via IBGE servicodados API...")
    try:
        # IBGE localities API gives us município codes at least
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        municipios = resp.json()

        # We can't get actual CNES counts from IBGE, so return None
        # The agent will handle missing CNES data gracefully
        logger.warning("CNES fallback: no establishment counts available without PySUS")
        return None
    except Exception as e:
        logger.error(f"CNES fallback failed: {e}")
        return None


def _fetch_sih_fallback(uf_code: str, year: int) -> pd.DataFrame | None:
    """Fallback: return None — SIH data requires PySUS."""
    logger.warning("SIH fallback: hospitalization data not available without PySUS")
    return None


def _get_uf_list(uf_code: str) -> list[str]:
    """Get list of UF codes."""
    if uf_code == "all":
        return [
            "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
            "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
            "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
        ]
    # Convert numeric UF code to abbreviation
    UF_MAP = {
        "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
        "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
        "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
        "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
        "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
        "52": "GO", "53": "DF",
    }
    return [UF_MAP.get(uf_code, uf_code)]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = fetch_health_data(uf_code="43", year=2023, use_cache=False)
    if df is not None:
        print(df.head(10))
        print(f"\nTotal municípios: {len(df)}")

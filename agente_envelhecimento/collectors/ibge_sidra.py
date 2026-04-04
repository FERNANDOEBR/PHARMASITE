"""
IBGE SIDRA Collector — Censo 2022 population by age group per município.

Uses the SIDRA API (via sidrapy) to fetch Table 9514:
  - Variable 9324: População residente
  - Classification C58: Grupo de idade
  - Level N6: Município
  - Period: 2022

Computes age buckets: pop_0_14, pop_15_59, pop_60_plus for aging index.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

# SIDRA Table 9514 — Censo 2022 population by age and sex
TABLE_CODE = "9514"
VARIABLE = "9324"        # População residente
PERIOD = "2022"
TERRITORIAL_LEVEL = "6"  # Município

# Classification 58 = Grupo de idade
# Key age-group category codes from SIDRA for Table 9514
# These are single-year ages; we'll request all and aggregate.
# Using "all" fetches every age category including totals.
AGE_CLASSIFICATION = "58"

# Age group category codes for aggregation
# 0-14 years: categories for ages 0,1,2,...,14
# 60+ years: categories for ages 60,61,...,100+
# We'll parse the age label text instead of hardcoding category IDs.


def _parse_age_from_label(label: str) -> int | None:
    """Extract numeric age from SIDRA age label like '5 anos', '100 anos ou mais'."""
    if not label or label == "Total":
        return None
    label = label.strip().lower()
    if "menos de 1" in label or "menor de 1" in label:
        return 0
    if "100" in label and ("mais" in label or "ou mais" in label):
        return 100
    # Try to extract leading number
    parts = label.split()
    if parts:
        try:
            return int(parts[0])
        except ValueError:
            pass
    return None


def _classify_age_bucket(age: int | None) -> str | None:
    """Classify age into one of the 3 buckets."""
    if age is None:
        return None
    if 0 <= age <= 14:
        return "pop_0_14"
    elif 15 <= age <= 59:
        return "pop_15_59"
    elif age >= 60:
        return "pop_60_plus"
    return None


def fetch_population_by_age(
    uf_code: str = "all",
    use_cache: bool = True,
    cache_path: str = "output/ibge_sidra_cache.parquet",
) -> pd.DataFrame:
    """
    Fetch Censo 2022 population data by age group per município.

    Args:
        uf_code: IBGE UF code to filter (e.g. "35" for SP), or "all" for nationwide.
        use_cache: If True, try to load from local cache first.
        cache_path: Path to cache file.

    Returns:
        DataFrame with columns:
        - cod_municipio (str): 7-digit IBGE code
        - nome_municipio (str)
        - pop_0_14 (int)
        - pop_15_59 (int)
        - pop_60_plus (int)
        - pop_total (int)
    """
    import os

    if use_cache and os.path.exists(cache_path):
        logger.info(f"Loading IBGE SIDRA data from cache: {cache_path}")
        return pd.read_parquet(cache_path)

    logger.info("Fetching population data from IBGE SIDRA API...")
    logger.info(f"Table={TABLE_CODE}, Variable={VARIABLE}, Period={PERIOD}")

    try:
        import sidrapy
    except ImportError:
        raise ImportError("Install sidrapy: pip install sidrapy")

    # Determine territorial code
    if uf_code == "all":
        ibge_territorial_code = "all"
    else:
        ibge_territorial_code = f"in n3 {uf_code}"

    # Fetch all age categories for all municípios
    # This is a large query — SIDRA may take 1-3 minutes
    logger.info("Querying SIDRA (this may take 2-5 minutes for all municípios)...")

    try:
        raw = sidrapy.get_table(
            table_code=TABLE_CODE,
            territorial_level=TERRITORIAL_LEVEL,
            ibge_territorial_code=ibge_territorial_code,
            variable=VARIABLE,
            period=PERIOD,
            classifications={AGE_CLASSIFICATION: "allxt"},  # all except total
        )
    except Exception as e:
        logger.error(f"SIDRA API error: {e}")
        logger.info("Falling back to direct REST API call...")
        raw = _fetch_via_rest(uf_code)

    if raw is None or raw.empty:
        raise RuntimeError("No data returned from IBGE SIDRA")

    logger.info(f"Raw data received: {len(raw)} rows")

    # Process the raw data
    df = _process_raw_sidra(raw)

    # Cache for next time
    if use_cache:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_parquet(cache_path, index=False)
        logger.info(f"Cached to {cache_path}")

    return df


def _fetch_via_rest(uf_code: str = "all") -> pd.DataFrame:
    """Fallback: direct REST API call to SIDRA."""
    import requests
    import io

    territorial = f"n6/all" if uf_code == "all" else f"n6/in n3 {uf_code}"

    url = (
        f"https://apisidra.ibge.gov.br/values"
        f"/t/{TABLE_CODE}"
        f"/p/{PERIOD}"
        f"/v/{VARIABLE}"
        f"/{territorial}"
        f"/c{AGE_CLASSIFICATION}/allxt"
        f"/f/a/h/y"
    )

    logger.info(f"REST URL: {url}")
    response = requests.get(url, timeout=300)
    response.raise_for_status()

    data = response.json()
    if not data:
        return pd.DataFrame()

    # First row is header
    header = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows)
    return df


def _process_raw_sidra(raw: pd.DataFrame) -> pd.DataFrame:
    """Process raw SIDRA response into clean age-bucketed DataFrame."""

    # Column names vary by how we called SIDRA.
    # Common columns: 'Valor', município code/name, age category name
    # Let's inspect and adapt

    # Standardize column names
    col_map = {}
    for col in raw.columns:
        cl = col.lower().strip()
        if "valor" in cl or col == "V":
            col_map[col] = "valor"
        elif "município" in cl.lower() if hasattr(cl, 'lower') else False:
            if "cód" in cl or "cod" in cl or "(código)" in cl:
                col_map[col] = "cod_municipio"
            else:
                col_map[col] = "nome_municipio"
        elif "idade" in cl or "grupo" in cl:
            col_map[col] = "age_label"

    # If standard column detection failed, try positional approach
    if "valor" not in col_map.values():
        # Likely has D1C, D1N, D2C, D2N, V pattern
        for col in raw.columns:
            if col == "V":
                col_map[col] = "valor"
            elif col == "D1C":
                col_map[col] = "cod_municipio"
            elif col == "D1N":
                col_map[col] = "nome_municipio"
            elif col == "D3N" or col == "D2N":
                if col not in col_map:
                    col_map[col] = "age_label"

    raw = raw.rename(columns=col_map)

    # Ensure we have the required columns
    required = ["valor", "cod_municipio"]
    for r in required:
        if r not in raw.columns:
            logger.error(f"Missing column '{r}'. Available: {list(raw.columns)}")
            # Try to find the age label column
            if "age_label" not in raw.columns:
                # pick the column that has age-like values
                for col in raw.columns:
                    sample = raw[col].dropna().head(20).tolist()
                    if any("anos" in str(v).lower() for v in sample):
                        raw = raw.rename(columns={col: "age_label"})
                        break

    # Parse valor to numeric
    raw["valor"] = pd.to_numeric(raw["valor"], errors="coerce")

    # Parse age from label
    if "age_label" in raw.columns:
        raw["age"] = raw["age_label"].apply(_parse_age_from_label)
    else:
        logger.warning("No age_label column found; attempting numeric classification column")
        raw["age"] = None

    raw["bucket"] = raw["age"].apply(_classify_age_bucket)

    # Drop rows with no valid bucket (totals, etc.)
    valid = raw.dropna(subset=["bucket", "valor"])

    # Aggregate by município and bucket
    agg = (
        valid.groupby(["cod_municipio", "bucket"])["valor"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
    )

    # Ensure all 3 bucket columns exist
    for col in ["pop_0_14", "pop_15_59", "pop_60_plus"]:
        if col not in agg.columns:
            agg[col] = 0

    agg["pop_total"] = agg["pop_0_14"] + agg["pop_15_59"] + agg["pop_60_plus"]

    # Get municipality names
    if "nome_municipio" in raw.columns:
        names = (
            raw[["cod_municipio", "nome_municipio"]]
            .drop_duplicates(subset="cod_municipio")
            .set_index("cod_municipio")
        )
        agg = agg.merge(names, left_on="cod_municipio", right_index=True, how="left")

    # Clean types
    agg["cod_municipio"] = agg["cod_municipio"].astype(str).str.strip()

    result = agg[
        ["cod_municipio", "nome_municipio", "pop_0_14", "pop_15_59", "pop_60_plus", "pop_total"]
    ].copy() if "nome_municipio" in agg.columns else agg[
        ["cod_municipio", "pop_0_14", "pop_15_59", "pop_60_plus", "pop_total"]
    ].copy()

    for col in ["pop_0_14", "pop_15_59", "pop_60_plus", "pop_total"]:
        result[col] = result[col].astype(int)

    logger.info(f"Processed {len(result)} municípios from IBGE SIDRA")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = fetch_population_by_age(uf_code="43")  # Test with RS
    print(df.head(20))
    print(f"\nTotal municípios: {len(df)}")
    print(f"Columns: {list(df.columns)}")

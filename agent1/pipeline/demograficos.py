"""
Coleta dados demográficos dos municípios brasileiros via IBGE Agregados v3.

DOMÍNIO: servicodados.ibge.gov.br (mesmo domínio do endpoint /localidades que funciona)
         ≠ apisidra.ibge.gov.br  (SIDRA — domínio separado, frequentemente bloqueado em Docker)

Fontes:
  - Pop. total/UF: Estimativas da Pop. Residente 2022  (Pesquisa 6579, Var. 9324)
  - Censo 2022 detalhes: Agregado 4709 (pop. por situação) e 9514 (pop. por idade)
  - Renda domiciliar: Agregado 6691 (Censo 2022 — rendimento domiciliar per capita)
"""

import requests
import pandas as pd
from sqlalchemy import text
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import time

IBGE_V3 = "https://servicodados.ibge.gov.br/api/v3/agregados"

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=20))
def ibge_get(url: str) -> list:
    """GET an IBGE v3/agregados URL; raises on non-200."""
    resp = requests.get(url, timeout=90)
    resp.raise_for_status()
    return resp.json()


def parse_agregados(data: list) -> pd.DataFrame:
    """
    Parse IBGE v3/agregados response format:
      [{ "id": VAR_ID, "resultados": [{ "series": [{ "localidade": {...}, "serie": {YEAR: VALUE} }] }] }]
    Returns DataFrame(codigo_ibge, value).
    """
    rows = []
    try:
        for var_block in data:
            for resultado in var_block.get("resultados", []):
                for series_item in resultado.get("series", []):
                    loc_id = str(series_item["localidade"]["id"]).zfill(7)
                    serie = series_item.get("serie", {})
                    # Take the first (and usually only) year value
                    raw = next(iter(serie.values()), None) if serie else None
                    if raw and raw not in ("-", "...", "X"):
                        try:
                            rows.append({"codigo_ibge": loc_id, "value": float(raw)})
                        except (ValueError, TypeError):
                            pass
    except (KeyError, TypeError, StopIteration):
        pass
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["codigo_ibge", "value"])


def fetch_var(agregado_id: int, var_id: int, periodo: str = "2022") -> pd.DataFrame:
    """Fetch one variable from IBGE agregados v3 for all municipalities (N6)."""
    url = f"{IBGE_V3}/{agregado_id}/periodos/{periodo}/variaveis/{var_id}?localidades=N6[all]"
    try:
        data = ibge_get(url)
        return parse_agregados(data)
    except Exception as e:
        logger.warning(f"  Agregado {agregado_id}/var {var_id}: {type(e).__name__}: {e}")
        return pd.DataFrame(columns=["codigo_ibge", "value"])


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def collect_demograficos(engine) -> dict:
    logger.info("Coletando dados demográficos IBGE Agregados v3...")

    with engine.connect() as conn:
        muns = pd.read_sql("SELECT codigo_ibge FROM municipios", conn)
    logger.info(f"{len(muns)} municípios para enriquecer")

    df = muns.copy()

    # ── 1. População total — Estimativas 2022 (Pesquisa 6579, Var. 9324) ─────
    logger.info("  📊 Pop. total (Estimativas 2022)...")
    df_pop = fetch_var(6579, 9324, "2022")
    if df_pop.empty:
        # Fallback: Censo 2022 Universo — Agregado 9514 (pop. residente)
        logger.info("  📊 Pop. total — fallback Censo 2022 (Ag. 9514)...")
        df_pop = fetch_var(9514, 93, "2022")
    if not df_pop.empty:
        df = df.merge(df_pop.rename(columns={"value": "populacao_total"}), on="codigo_ibge", how="left")
        ok = df["populacao_total"].notna().sum()
        logger.success(f"  Pop. total: {ok}/{len(df)} municípios")
    else:
        df["populacao_total"] = None
        logger.warning("  Pop. total: sem dados")
    time.sleep(1)

    # ── 2. Pop. urbana vs rural — Censo 2022 (Ag. 4709, Var. 93) ─────────────
    # Classificação: c1=1 (Urbana) / c1=2 (Rural)  [situação do domicílio]
    logger.info("  🏙️  Pop. urbana (Censo 2022 Ag. 4709)...")

    def fetch_urban_rural():
        url_urb  = f"{IBGE_V3}/4709/periodos/2022/variaveis/93?localidades=N6[all]&classificacao=1[1]"
        url_rur  = f"{IBGE_V3}/4709/periodos/2022/variaveis/93?localidades=N6[all]&classificacao=1[2]"
        try:
            df_u = parse_agregados(ibge_get(url_urb)).rename(columns={"value": "populacao_urbana"})
            df_r = parse_agregados(ibge_get(url_rur)).rename(columns={"value": "populacao_rural"})
            return df_u, df_r
        except Exception as e:
            logger.warning(f"  Pop. urbana/rural: {type(e).__name__}: {e}")
            return pd.DataFrame(columns=["codigo_ibge", "populacao_urbana"]), \
                   pd.DataFrame(columns=["codigo_ibge", "populacao_rural"])

    df_urb, df_rur = fetch_urban_rural()
    df = df.merge(df_urb, on="codigo_ibge", how="left")
    df = df.merge(df_rur, on="codigo_ibge", how="left")

    # Derive urbanization rate — coerce to numeric first: left-merging an empty
    # DataFrame gives object dtype with None values, which breaks .round(2).
    if "populacao_total" in df.columns and "populacao_urbana" in df.columns:
        urb_num = pd.to_numeric(df["populacao_urbana"], errors="coerce")
        denom   = pd.to_numeric(df["populacao_total"],  errors="coerce").replace(0, float("nan"))
        if urb_num.notna().any():
            df["taxa_urbanizacao"] = (urb_num / denom * 100).round(2)
        else:
            df["taxa_urbanizacao"] = None
            logger.warning("  taxa_urbanizacao: sem dados (pop_urbana indisponível)")
    else:
        df["taxa_urbanizacao"] = None
    time.sleep(1)

    # ── 3. Renda domiciliar per capita — Censo 2022 (Ag. 6691, Var. 10605) ──
    # Ag. 6691 = "Rendimento domiciliar per capita da pop. residente" Censo 2022
    logger.info("  💵 Renda domiciliar per capita (Censo 2022 Ag. 6691)...")
    df_renda = fetch_var(6691, 10605, "2022")
    if df_renda.empty:
        # Try older Ag. 9605 (Censo 2022 first-release tables)
        logger.info("  💵 Renda fallback Ag. 9605...")
        df_renda = fetch_var(9605, 10605, "2022")
    if not df_renda.empty:
        df = df.merge(df_renda.rename(columns={"value": "renda_per_capita"}), on="codigo_ibge", how="left")
        ok = df["renda_per_capita"].notna().sum()
        logger.success(f"  Renda: {ok}/{len(df)} municípios")
    else:
        df["renda_per_capita"] = None
        logger.warning("  Renda: sem dados")
    time.sleep(1)

    # ── 4. Faixas etárias — Censo 2022 (Ag. 9923) ────────────────────────────
    # Classificação c287: 100362=0-4, 100363=5-14, 2793=15-29, 2794=30-44, 2795=45-64, 2796=65+
    # IBGE v3 clasificacao format: "287[CATID]" (NOT "287=CATID")
    logger.info("  👶 Faixas etárias (Censo 2022 Ag. 9923)...")
    age_groups = {
        "pop_0_4":    ("9923", "93", "287[100362]"),
        "pop_5_14":   ("9923", "93", "287[100363]"),
        "pop_15_29":  ("9923", "93", "287[2793]"),
        "pop_30_44":  ("9923", "93", "287[2794]"),
        "pop_45_64":  ("9923", "93", "287[2795]"),
        "pop_65_plus":("9923", "93", "287[2796]"),
    }

    for col, (ag, var, classif) in age_groups.items():
        url = f"{IBGE_V3}/{ag}/periodos/2022/variaveis/{var}?localidades=N6[all]&classificacao={classif}"
        try:
            data = ibge_get(url)
            df_age = parse_agregados(data).rename(columns={"value": col})
            if not df_age.empty:
                df = df.merge(df_age, on="codigo_ibge", how="left")
                ok = df[col].notna().sum()
                logger.info(f"    {col}: {ok} municípios")
            else:
                df[col] = None
                logger.warning(f"    {col}: sem dados")
        except Exception as e:
            logger.warning(f"    {col}: {type(e).__name__}: {e}")
            df[col] = None
        time.sleep(0.5)

    # ── 5. Derived columns ─────────────────────────────────────────────────────
    age_cols_alvo = ["pop_30_44", "pop_45_64"]
    existing_alvo = [c for c in age_cols_alvo if c in df.columns and df[c].notna().any()]
    if existing_alvo:
        df["populacao_alvo"] = df[existing_alvo].sum(axis=1, min_count=1)
        denom = df["populacao_total"].replace(0, float("nan"))
        df["pct_populacao_alvo"] = (df["populacao_alvo"] / denom * 100).round(2)
    else:
        df["populacao_alvo"] = None
        df["pct_populacao_alvo"] = None

    if all(c in df.columns for c in ["pop_65_plus", "pop_0_4", "pop_5_14"]):
        pop_jovem = df["pop_0_4"].fillna(0) + df["pop_5_14"].fillna(0)
        df["indice_envelhecimento"] = (df["pop_65_plus"].fillna(0) / pop_jovem.replace(0, 1) * 100).round(2)
    else:
        df["indice_envelhecimento"] = None

    # ── 6. Write to DB ─────────────────────────────────────────────────────────
    logger.info(f"  💾 Inserindo {len(df)} registros demográficos...")

    cols_map = [
        "populacao_total", "populacao_urbana", "populacao_rural",
        "taxa_urbanizacao", "renda_per_capita", "pop_0_4", "pop_5_14",
        "pop_15_29", "pop_30_44", "pop_45_64", "pop_65_plus",
        "indice_envelhecimento", "populacao_alvo", "pct_populacao_alvo",
    ]

    inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            params = {"codigo_ibge": row["codigo_ibge"]}
            for col in cols_map:
                val = row.get(col)
                params[col] = float(val) if col in df.columns and pd.notna(val) else None

            conn.execute(text(f"""
                INSERT INTO demograficos (
                    codigo_ibge, populacao_total, populacao_urbana, populacao_rural,
                    taxa_urbanizacao, renda_per_capita, pop_0_4, pop_5_14, pop_15_29,
                    pop_30_44, pop_45_64, pop_65_plus, indice_envelhecimento,
                    populacao_alvo, pct_populacao_alvo, ano_referencia
                ) VALUES (
                    :codigo_ibge, :populacao_total, :populacao_urbana, :populacao_rural,
                    :taxa_urbanizacao, :renda_per_capita, :pop_0_4, :pop_5_14, :pop_15_29,
                    :pop_30_44, :pop_45_64, :pop_65_plus, :indice_envelhecimento,
                    :populacao_alvo, :pct_populacao_alvo, 2022
                )
                ON CONFLICT (codigo_ibge) DO NOTHING
            """), params)
            inserted += 1

    pop_ok = df["populacao_total"].notna().sum() if "populacao_total" in df.columns else 0
    logger.success(f"Demográficos: {inserted} inseridos | pop_total preenchida: {pop_ok}")
    return {"count": inserted, "message": f"Demográficos coletados ({pop_ok} com pop_total)"}

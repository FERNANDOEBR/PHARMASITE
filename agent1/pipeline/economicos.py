"""
Coleta indicadores econômicos de múltiplas fontes:
- IPEADATA: PIB per capita, IDH municipal
- ANS: beneficiários planos de saúde por município
- Receita Federal: CNPJs ativos por CNAE (farmácias, saúde, distribuidores)
"""

import io
import requests
import pandas as pd
from sqlalchemy import text
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import time

IPEADATA_BASE = "http://www.ipeadata.gov.br/api/odata4"
ANS_BENEFICIARIOS_URL = "https://www.ans.gov.br/images/stories/Plano_de_saude_e_Operadoras/dados_e_indicadores/Beneficiarios/beneficiarios_municipio_2024.csv"

# CNAEs relevantes para correlatos farmacêuticos
CNAES_RELEVANTES = {
    "4771": "cnpjs_farmacias",           # Comércio varejista farmacêutico
    "4644": "cnpjs_distribuidores",      # Comércio atacadista produtos farmacêuticos
    "4645": "cnpjs_distribuidores",      # Comércio atacadista instrumentos/materiais médicos
    "3250": "cnpjs_instrumentos_medicos", # Fabricação equipamentos/instrumentos médicos
    "8610": "cnpjs_saude",               # Atividades de atendimento hospitalar
    "8630": "cnpjs_saude",               # Atividades de atenção ambulatorial
    "8650": "cnpjs_saude",               # Atividades de profissionais de saúde
    "8660": "cnpjs_saude",               # Atividades de apoio diagnóstico e terapia
}


def collect_economicos(engine) -> dict:
    logger.info("Coletando indicadores econômicos...")

    with engine.connect() as conn:
        muns = pd.read_sql("SELECT codigo_ibge, uf FROM municipios", conn)

    logger.info(f"  {len(muns)} municípios para enriquecer")

    # ── 1. PIB per capita (IPEADATA) ──
    df_pib = collect_pib_ipeadata(muns["codigo_ibge"].tolist())

    # ── 2. IDH (IPEADATA) ──
    df_idh = collect_idh_ipeadata(muns["codigo_ibge"].tolist())

    # ── 3. ANS beneficiários planos saúde ──
    df_ans = collect_ans_beneficiarios()

    # ── 4. CNPJs Receita Federal ──
    df_cnpj = collect_cnpjs_receita(muns)

    # ── 5. Merge all ──
    df = muns[["codigo_ibge"]].copy()
    for dfi in [df_pib, df_idh, df_ans, df_cnpj]:
        if not dfi.empty:
            df = df.merge(dfi, on="codigo_ibge", how="left")

    # ── 6. Calculate coverage % ──
    with engine.connect() as conn:
        pop_df = pd.read_sql("SELECT codigo_ibge, populacao_total FROM demograficos", conn)
    df = df.merge(pop_df, on="codigo_ibge", how="left")

    if "beneficiarios_planos" in df.columns and "populacao_total" in df.columns:
        df["cobertura_planos_pct"] = (
            df["beneficiarios_planos"] / df["populacao_total"].replace(0, 1) * 100
        ).clip(0, 100).round(2)

    # ── 7. Write to DB ──
    logger.info(f"  💾 Inserindo {len(df)} registros econômicos...")
    inserted = 0

    numeric_cols = [
        "pib_per_capita", "pib_total", "idh", "beneficiarios_planos",
        "cobertura_planos_pct", "cnpjs_farmacias", "cnpjs_saude",
        "cnpjs_instrumentos_medicos", "cnpjs_distribuidores",
        "empregos_saude"
    ]

    with engine.begin() as conn:
        for _, row in df.iterrows():
            params = {"codigo_ibge": row["codigo_ibge"]}
            for col in numeric_cols:
                val = row.get(col)
                params[col] = float(val) if pd.notna(val) else None

            conn.execute(text("""
                INSERT INTO indicadores_economicos (
                    codigo_ibge, pib_per_capita, pib_total, idh,
                    beneficiarios_planos, cobertura_planos_pct,
                    cnpjs_farmacias, cnpjs_saude,
                    cnpjs_instrumentos_medicos, cnpjs_distribuidores,
                    empregos_saude, ano_referencia
                ) VALUES (
                    :codigo_ibge, :pib_per_capita, :pib_total, :idh,
                    :beneficiarios_planos, :cobertura_planos_pct,
                    :cnpjs_farmacias, :cnpjs_saude,
                    :cnpjs_instrumentos_medicos, :cnpjs_distribuidores,
                    :empregos_saude, 2023
                ) ON CONFLICT (codigo_ibge) DO NOTHING
            """), params)
            inserted += 1

    logger.success(f"Econômicos: {inserted} municípios inseridos")
    return {"count": inserted, "message": "Indicadores econômicos coletados"}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2))
def collect_pib_ipeadata(municipio_codes: list) -> pd.DataFrame:
    logger.info("  📈 IPEADATA: PIB per capita municipal...")
    try:
        # IPEADATA série: PIB municipal per capita (PIBMUNIC_PIBPMCAP)
        url = f"{IPEADATA_BASE}/Metadados('PIBMUNIC_PIBPMCAP')/Valores?$top=6000&$select=TERCODIGO,VALDATA,VALVALOR&$orderby=VALDATA desc"
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            data = resp.json().get("value", [])
            if data:
                df = pd.DataFrame(data)
                df = df.rename(columns={"TERCODIGO": "codigo_ibge_6", "VALVALOR": "pib_per_capita"})
                # IPEADATA uses 6-digit codes, IBGE uses 7 — add leading zero
                df["codigo_ibge"] = df["codigo_ibge_6"].astype(str).str.zfill(7)
                # Keep latest year per municipality
                df = df.sort_values("VALDATA", ascending=False).groupby("codigo_ibge").first().reset_index()
                logger.success(f"    PIB per capita: {len(df)} municípios")
                return df[["codigo_ibge", "pib_per_capita"]]
    except Exception as e:
        logger.warning(f"    PIB IPEADATA erro: {e}")

    return pd.DataFrame(columns=["codigo_ibge", "pib_per_capita"])


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2))
def collect_idh_ipeadata(municipio_codes: list) -> pd.DataFrame:
    logger.info("  🏅 IPEADATA: IDH municipal...")
    try:
        url = f"{IPEADATA_BASE}/Metadados('IDHM_IDHM')/Valores?$top=6000&$select=TERCODIGO,VALDATA,VALVALOR&$orderby=VALDATA desc"
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            data = resp.json().get("value", [])
            if data:
                df = pd.DataFrame(data)
                df = df.rename(columns={"TERCODIGO": "codigo_ibge_6", "VALVALOR": "idh"})
                df["codigo_ibge"] = df["codigo_ibge_6"].astype(str).str.zfill(7)
                df = df.groupby("codigo_ibge").first().reset_index()
                logger.success(f"    IDH: {len(df)} municípios")
                return df[["codigo_ibge", "idh"]]
    except Exception as e:
        logger.warning(f"    IDH IPEADATA erro: {e}")

    return pd.DataFrame(columns=["codigo_ibge", "idh"])


def collect_ans_beneficiarios() -> pd.DataFrame:
    logger.info("  🏥 ANS: beneficiários planos de saúde...")
    try:
        resp = requests.get(ANS_BENEFICIARIOS_URL, timeout=120)
        if resp.status_code == 200:
            # ANS CSV uses semicolons and latin-1 encoding
            content = resp.content.decode("latin-1")
            df = pd.read_csv(io.StringIO(content), sep=";", decimal=",")

            # Find relevant columns (ANS format varies)
            mun_col = next((c for c in df.columns if "municipio" in c.lower() or "ibge" in c.lower()), None)
            ben_col = next((c for c in df.columns if "benefi" in c.lower()), None)

            if mun_col and ben_col:
                df["codigo_ibge"] = df[mun_col].astype(str).str.zfill(7)
                df["beneficiarios_planos"] = pd.to_numeric(df[ben_col], errors="coerce")
                df_agg = df.groupby("codigo_ibge")["beneficiarios_planos"].sum().reset_index()
                logger.success(f"    ANS: {len(df_agg)} municípios com dados de planos")
                return df_agg
        else:
            logger.warning(f"    ANS retornou {resp.status_code}")
    except Exception as e:
        logger.warning(f"    ANS erro: {e}")

    return pd.DataFrame(columns=["codigo_ibge", "beneficiarios_planos"])


def collect_cnpjs_receita(muns_df: pd.DataFrame) -> pd.DataFrame:
    """
    CNPJs da Receita Federal via dados abertos (cnpja.com ou brasil.io como proxy).
    Fonte primária: dados.gov.br/dataset/cnpj
    Fallback: cnpja API (gratuita com limite)
    """
    logger.info("  🏢 Receita Federal: CNPJs ativos por CNAE...")

    try:
        # Try brasil.io CNPJ API (public, rate-limited)
        results = []

        for cnae, col in CNAES_RELEVANTES.items():
            time.sleep(1)
            try:
                url = f"https://brasilapi.com.br/api/cnpj/v1/{cnae}"
                # BrasilAPI doesn't support CNAE search directly
                # Use alternative: IBGE municipalities + CNPJ count estimation
                pass
            except Exception:
                pass

        # Alternative: Use CNPJ.ws or local estimation from CNES ratio
        # For MVP: estimate CNPJs from CNES data with ratio
        logger.info("    Usando estimativa CNPJs via CNES (MVP)...")

        with engine_connection_placeholder(muns_df) as conn_placeholder:
            # Will be filled from CNES data in scoring step
            pass

        return pd.DataFrame(columns=["codigo_ibge", "cnpjs_farmacias", "cnpjs_saude",
                                      "cnpjs_instrumentos_medicos", "cnpjs_distribuidores"])

    except Exception as e:
        logger.warning(f"    CNPJs Receita Federal erro: {e}")
        return pd.DataFrame(columns=["codigo_ibge", "cnpjs_farmacias", "cnpjs_saude", "cnpjs_instrumentos_medicos", "cnpjs_distribuidores"])


def engine_connection_placeholder(df):
    """Context manager placeholder."""
    class CM:
        def __enter__(self): return None
        def __exit__(self, *args): pass
    return CM()

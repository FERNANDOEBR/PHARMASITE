"""
ENTREGA 3 — Agent Scout: Sinais de Crescimento por CEP/Município
=================================================================
Fonte: Receita Federal — dados abertos de CNPJ
URL:  https://dadosabertos.rfb.gov.br/CNPJ/

Lógica central:
  "Onde estão abrindo mais farmácias, construtoras e galpões logísticos
   nos últimos 2 anos?" → proxy de crescimento econômico local.

  Quem tem essa informação antecipadamente chega antes da concorrência
  e pega imóveis mais baratos. (Frase literal do questionário do cliente.)

CNAEs monitorados:
  4771   Farmácias e drogarias        → mercado direto
  4110   Incorporação imobiliária     → novos condomínios
  4120   Construção de edifícios      → obras em andamento
  5211   Armazéns gerais              → polo logístico/industrial
  5212   Depósitos de mercadorias     → idem
  6810   Compra e venda de imóveis    → valorização do bairro
  6822   Gestão imobiliária           → idem

Estratégia de download:
  Os arquivos da Receita Federal são grandes (> 1 GB cada).
  Fazemos download em chunks, filtramos por UF=SP e CNAEs relevantes,
  e descartamos o resto. Memória pico: ~200 MB por arquivo.

Output:
  Tabelas cnpj_abertura_anual + growth_signals no PostgreSQL.
"""

import io
import os
import re
import zipfile
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

# ── Constantes ────────────────────────────────────────────────────────────────

UF_FILTER = "SP"

# CNAEs de interesse e seus grupos (4 dígitos = subclasse agrupada)
CNAE_GROUPS = {
    "4771": ("farmacias",   "Farmácias e drogarias"),
    "4110": ("construcao",  "Incorporação imobiliária"),
    "4120": ("construcao",  "Construção de edifícios residenciais"),
    "5211": ("logistica",   "Armazéns gerais"),
    "5212": ("logistica",   "Depósitos de mercadorias"),
    "6810": ("imob",        "Compra e venda de imóveis"),
    "6822": ("imob",        "Gestão e administração imobiliária"),
}
CNAE_KEYS = set(CNAE_GROUPS.keys())

# Pesos para o growth_score final (soma = 1.0)
GROWTH_WEIGHTS = {
    "farmacias":  0.40,
    "construcao": 0.25,
    "logistica":  0.20,
    "imob":       0.15,
}

ANOS = [2022, 2023, 2024]

# Receita Federal — arquivos de Estabelecimentos (particionado em ~10 zips)
# Cada arquivo tem ~200-400 MB comprimido
RF_BASE = "https://dadosabertos.rfb.gov.br/CNPJ"
RF_ESTAB_URLS = [
    f"{RF_BASE}/Estabelecimentos{i}.zip" for i in range(10)
]

# Colunas do arquivo de Estabelecimentos (layout Receita Federal 2024)
# Ref: https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/
#      cadastros/cnpj/dados-publicos-cnpj
ESTAB_COLS = [
    "cnpj_basico",        # 0
    "cnpj_ordem",         # 1
    "cnpj_dv",            # 2
    "identificador",      # 3: 1=matriz, 2=filial
    "nome_fantasia",      # 4
    "situacao_cadastral", # 5: 02=ativa
    "data_sit_cadastral", # 6
    "motivo_sit",         # 7
    "nome_cidade_ext",    # 8
    "pais",               # 9
    "data_inicio_ativ",   # 10  ← ano de abertura
    "cnae_principal",     # 11  ← CNAE filtrado
    "cnae_secundaria",    # 12
    "tipo_logradouro",    # 13
    "logradouro",         # 14
    "numero",             # 15
    "complemento",        # 16
    "bairro",             # 17
    "cep",                # 18  ← CEP
    "uf",                 # 19  ← filtro SP
    "municipio",          # 20  ← código IBGE (7 dígitos no cadastro RF)
    "ddd1",               # 21
    "telefone1",          # 22
    "ddd2",               # 23
    "telefone2",          # 24
    "ddd_fax",            # 25
    "fax",                # 26
    "email",              # 27
    "situacao_especial",  # 28
    "data_sit_especial",  # 29
]

CHUNK_SIZE = 50_000   # linhas por chunk (controle de memória)
REQUEST_TIMEOUT = 300


# ── Download e filtragem ──────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def _stream_rf_file(url: str) -> bytes | None:
    """Faz download de um arquivo ZIP da Receita Federal."""
    try:
        logger.info(f"  Baixando {url.split('/')[-1]}...")
        r = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        if r.status_code == 404:
            logger.warning(f"  404 — arquivo não existe: {url}")
            return None
        r.raise_for_status()
        content = b""
        total = 0
        for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
            content += chunk
            total += len(chunk)
            if total % (50 * 1024 * 1024) == 0:
                logger.info(f"    {total/1024/1024:.0f} MB baixados...")
        logger.success(f"  ✅ {url.split('/')[-1]}: {total/1024/1024:.1f} MB")
        return content
    except requests.exceptions.Timeout:
        logger.error(f"  Timeout ao baixar {url}")
        raise
    except Exception as e:
        logger.error(f"  Erro: {e}")
        raise


def _filter_estab_zip(zip_bytes: bytes) -> pd.DataFrame:
    """
    Lê o CSV dentro do ZIP em chunks, filtra UF=SP + CNAEs relevantes
    + situação ativa + anos de interesse. Retorna DataFrame compacto.
    """
    frames = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_files = [n for n in zf.namelist() if n.endswith('.csv') or n.endswith('.ESTABELE')]
        if not csv_files:
            logger.warning("  Nenhum CSV encontrado no ZIP")
            return pd.DataFrame()

        csv_name = csv_files[0]
        logger.info(f"  Lendo {csv_name} em chunks de {CHUNK_SIZE:,} linhas...")

        with zf.open(csv_name) as f:
            for i, chunk in enumerate(pd.read_csv(
                f,
                sep=";",
                encoding="latin-1",
                header=None,
                names=ESTAB_COLS,
                dtype=str,
                chunksize=CHUNK_SIZE,
                low_memory=False,
            )):
                # 1. Filtro por UF
                chunk = chunk[chunk["uf"].str.strip() == UF_FILTER]
                if chunk.empty:
                    continue

                # 2. Filtro por situação ativa (02)
                chunk = chunk[chunk["situacao_cadastral"].str.strip() == "02"]
                if chunk.empty:
                    continue

                # 3. Filtro por CNAE principal (4 primeiros dígitos)
                chunk["cnae_4d"] = chunk["cnae_principal"].str.strip().str[:4]
                chunk = chunk[chunk["cnae_4d"].isin(CNAE_KEYS)]
                if chunk.empty:
                    continue

                # 4. Extrai ano de abertura
                chunk["ano_abertura"] = pd.to_numeric(
                    chunk["data_inicio_ativ"].str.strip().str[:4],
                    errors="coerce"
                )
                chunk = chunk[chunk["ano_abertura"].isin(ANOS)]
                if chunk.empty:
                    continue

                # 5. Limpa CEP e código IBGE
                chunk["cep_clean"]   = chunk["cep"].str.strip().str.zfill(8)
                chunk["ibge_clean"]  = chunk["municipio"].str.strip().str.zfill(7)

                frames.append(chunk[[
                    "ibge_clean", "cep_clean", "cnae_4d", "ano_abertura"
                ]].copy())

                if (i + 1) % 20 == 0:
                    logger.info(f"    Chunk {i+1}: {sum(len(f) for f in frames)} registros filtrados até agora")

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    logger.info(f"  Filtrado: {len(df):,} registros SP + CNAEs relevantes")
    return df


# ── Agregação e score ──────────────────────────────────────────────────────────

def _aggregate_to_municipio(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega contagem de aberturas por (município, CNAE_grupo, ano)."""
    if df.empty:
        return pd.DataFrame()

    # Mapeia CNAE 4d → categoria
    cnae_to_cat = {k: v[0] for k, v in CNAE_GROUPS.items()}
    df["categoria"] = df["cnae_4d"].map(cnae_to_cat)

    agg = (
        df.groupby(["ibge_clean", "categoria", "ano_abertura"])
        .size()
        .reset_index(name="qtd")
        .rename(columns={
            "ibge_clean":   "codigo_ibge",
            "ano_abertura": "ano",
        })
    )
    return agg


def _compute_growth_score(agg: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma dados long → wide e calcula delta percentual + growth_score.
    """
    if agg.empty:
        return pd.DataFrame()

    # Pivot: uma linha por (municipio, categoria), colunas = anos
    pivot = agg.pivot_table(
        index=["codigo_ibge", "categoria"],
        columns="ano",
        values="qtd",
        aggfunc="sum",
        fill_value=0
    ).reset_index()
    pivot.columns.name = None
    pivot.columns = [str(c) for c in pivot.columns]

    # Garante que todos os anos existem
    for ano in ANOS:
        col = str(ano)
        if col not in pivot.columns:
            pivot[col] = 0

    # Delta % = (2024 - 2022) / max(2022, 1)
    pivot["delta_pct"] = ((pivot["2024"] - pivot["2022"]) / pivot["2022"].clip(lower=1) * 100).round(2)

    # Pivot por categoria → uma linha por município
    cats = ["farmacias", "construcao", "logistica", "imob"]
    municipios = pivot["codigo_ibge"].unique()

    rows = []
    for mun in municipios:
        row = {"codigo_ibge": mun}
        sub = pivot[pivot["codigo_ibge"] == mun]
        for cat in cats:
            cat_row = sub[sub["categoria"] == cat]
            if not cat_row.empty:
                row[f"{cat}_2022"]      = int(cat_row["2022"].values[0])
                row[f"{cat}_2023"]      = int(cat_row["2023"].values[0]) if "2023" in cat_row.columns else 0
                row[f"{cat}_2024"]      = int(cat_row["2024"].values[0])
                row[f"{cat}_delta_pct"] = float(cat_row["delta_pct"].values[0])
            else:
                row[f"{cat}_2022"]      = 0
                row[f"{cat}_2023"]      = 0
                row[f"{cat}_2024"]      = 0
                row[f"{cat}_delta_pct"] = 0.0
        rows.append(row)

    df_wide = pd.DataFrame(rows)

    # Growth score: normaliza deltas para 0-100, pondera por categoria
    for cat in cats:
        col = f"{cat}_delta_pct"
        mn, mx = df_wide[col].min(), df_wide[col].max()
        if mx > mn:
            df_wide[f"{cat}_norm"] = ((df_wide[col] - mn) / (mx - mn) * 100).clip(0, 100)
        else:
            df_wide[f"{cat}_norm"] = 50.0

    df_wide["growth_score"] = sum(
        df_wide[f"{cat}_norm"] * w
        for cat, w in GROWTH_WEIGHTS.items()
    ).round(2)

    # Growth tier (quartis)
    p75 = df_wide["growth_score"].quantile(0.75)
    p50 = df_wide["growth_score"].quantile(0.50)
    p25 = df_wide["growth_score"].quantile(0.25)

    def _tier(s):
        if s >= p75: return "A"
        if s >= p50: return "B"
        if s >= p25: return "C"
        return "D"

    df_wide["growth_tier"] = df_wide["growth_score"].apply(_tier)

    # Label qualitativo
    def _label(row):
        if row["growth_tier"] == "A":  return "ACELERADO"
        if row["growth_tier"] == "B":  return "CRESCENDO"
        if row["growth_tier"] == "C":  return "ESTAVEL"
        return "DECLINIO"

    df_wide["growth_label"] = df_wide.apply(_label, axis=1)
    df_wide["anos_analisados"] = f"{ANOS[0]}-{ANOS[-1]}"
    df_wide["uf"] = UF_FILTER

    # Limpa colunas auxiliares
    df_wide = df_wide.drop(columns=[f"{c}_norm" for c in cats], errors="ignore")

    logger.info(
        f"  Growth tiers → A: {(df_wide['growth_tier']=='A').sum()} | "
        f"B: {(df_wide['growth_tier']=='B').sum()} | "
        f"C: {(df_wide['growth_tier']=='C').sum()} | "
        f"D: {(df_wide['growth_tier']=='D').sum()}"
    )
    top5_growth = df_wide.nlargest(5, "growth_score")[["codigo_ibge", "growth_score", "farmacias_delta_pct"]]
    logger.info(f"  Top 5 crescimento:\n{top5_growth.to_string(index=False)}")

    return df_wide


# ── Persistência ──────────────────────────────────────────────────────────────

def _insert_raw_agg(engine, agg: pd.DataFrame) -> int:
    """Insere dados brutos de abertura anual por município/CNAE."""
    if agg.empty:
        return 0

    cat_to_descr = {v[0]: v[1] for v in CNAE_GROUPS.values()}

    inserted = 0
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM cnpj_abertura_anual WHERE uf = 'SP'"))
        for _, row in agg.iterrows():
            cat = row["categoria"]
            conn.execute(text("""
                INSERT INTO cnpj_abertura_anual
                    (codigo_ibge, cnae_grupo, cnae_descr, ano, qtd_abertos, uf)
                VALUES (:ibge, :cnae, :descr, :ano, :qtd, :uf)
                ON CONFLICT (codigo_ibge, cep, cnae_grupo, ano) DO UPDATE SET
                    qtd_abertos = EXCLUDED.qtd_abertos,
                    ingested_at = NOW()
            """), {
                "ibge":  row["codigo_ibge"],
                "cnae":  cat,
                "descr": cat_to_descr.get(cat, ""),
                "ano":   int(row["ano"]),
                "qtd":   int(row["qtd"]),
                "uf":    UF_FILTER,
            })
            inserted += 1
    return inserted


def _insert_growth_signals(engine, df_wide: pd.DataFrame) -> int:
    """Insere/atualiza growth_signals por município."""
    if df_wide.empty:
        return 0

    inserted = 0
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM growth_signals WHERE uf = 'SP'"))
        for _, row in df_wide.iterrows():
            conn.execute(text("""
                INSERT INTO growth_signals (
                    codigo_ibge, uf,
                    farmacias_2022, farmacias_2023, farmacias_2024, farmacias_delta_pct,
                    construcao_2022, construcao_2023, construcao_2024, construcao_delta_pct,
                    logistica_2022, logistica_2023, logistica_2024, logistica_delta_pct,
                    imob_2022, imob_2023, imob_2024, imob_delta_pct,
                    growth_score, growth_tier, growth_label, anos_analisados
                ) VALUES (
                    :ibge, :uf,
                    :f22, :f23, :f24, :f_d,
                    :c22, :c23, :c24, :c_d,
                    :l22, :l23, :l24, :l_d,
                    :i22, :i23, :i24, :i_d,
                    :gs, :gt, :gl, :anos
                )
                ON CONFLICT (codigo_ibge) DO UPDATE SET
                    farmacias_2022      = EXCLUDED.farmacias_2022,
                    farmacias_2023      = EXCLUDED.farmacias_2023,
                    farmacias_2024      = EXCLUDED.farmacias_2024,
                    farmacias_delta_pct = EXCLUDED.farmacias_delta_pct,
                    construcao_2022     = EXCLUDED.construcao_2022,
                    construcao_2023     = EXCLUDED.construcao_2023,
                    construcao_2024     = EXCLUDED.construcao_2024,
                    construcao_delta_pct= EXCLUDED.construcao_delta_pct,
                    logistica_2022      = EXCLUDED.logistica_2022,
                    logistica_2023      = EXCLUDED.logistica_2023,
                    logistica_2024      = EXCLUDED.logistica_2024,
                    logistica_delta_pct = EXCLUDED.logistica_delta_pct,
                    imob_2022           = EXCLUDED.imob_2022,
                    imob_2023           = EXCLUDED.imob_2023,
                    imob_2024           = EXCLUDED.imob_2024,
                    imob_delta_pct      = EXCLUDED.imob_delta_pct,
                    growth_score        = EXCLUDED.growth_score,
                    growth_tier         = EXCLUDED.growth_tier,
                    growth_label        = EXCLUDED.growth_label,
                    anos_analisados     = EXCLUDED.anos_analisados,
                    calculated_at       = NOW()
            """), {
                "ibge": row["codigo_ibge"], "uf": UF_FILTER,
                "f22": row["farmacias_2022"],  "f23": row["farmacias_2023"],  "f24": row["farmacias_2024"],  "f_d": row["farmacias_delta_pct"],
                "c22": row["construcao_2022"], "c23": row["construcao_2023"], "c24": row["construcao_2024"], "c_d": row["construcao_delta_pct"],
                "l22": row["logistica_2022"],  "l23": row["logistica_2023"],  "l24": row["logistica_2024"],  "l_d": row["logistica_delta_pct"],
                "i22": row["imob_2022"],        "i23": row["imob_2023"],        "i24": row["imob_2024"],        "i_d": row["imob_delta_pct"],
                "gs": row["growth_score"],     "gt": row["growth_tier"],      "gl": row["growth_label"],
                "anos": row["anos_analisados"],
            })
            inserted += 1

    return inserted


# ── Entry point ───────────────────────────────────────────────────────────────

def collect_cnpj_growth(engine) -> dict:
    """
    Entry point chamado pelo pipeline principal.
    Baixa dados Receita Federal, filtra SP + CNAEs relevantes,
    calcula delta de crescimento 2022→2024 e persiste no banco.
    """
    from pathlib import Path as _Path
    from sqlalchemy import text as _text

    logger.info("=" * 60)
    logger.info("📈 ENTREGA 3 — AGENT SCOUT: SINAIS DE CRESCIMENTO (SP)")
    logger.info("=" * 60)

    # Migração SQL
    sql_migration = _Path(__file__).parent.parent.parent / "shared" / "init_growth.sql"
    if sql_migration.exists():
        logger.info("  Executando migração SQL (growth_signals + scout_view)...")
        with engine.begin() as conn:
            conn.execute(_text(sql_migration.read_text()))
        logger.success("  Migração aplicada")

    # Coleta todos os arquivos de Estabelecimentos da RF
    all_frames = []
    success_count = 0

    for url in RF_ESTAB_URLS:
        try:
            zip_bytes = _stream_rf_file(url)
            if zip_bytes is None:
                continue
            df_chunk = _filter_estab_zip(zip_bytes)
            if not df_chunk.empty:
                all_frames.append(df_chunk)
                success_count += 1
            del zip_bytes  # libera memória imediatamente
        except Exception as e:
            logger.error(f"  Falha em {url}: {e} — continuando")
            continue

    if not all_frames:
        logger.error("❌ Nenhum dado da Receita Federal coletado")
        return {"count": 0, "message": "Falha na coleta Receita Federal"}

    logger.info(f"  {success_count} arquivos RF processados com sucesso")

    # Consolida e agrega
    df_all = pd.concat(all_frames, ignore_index=True)
    del all_frames

    logger.info(f"  Total de registros filtrados: {len(df_all):,}")
    agg = _aggregate_to_municipio(df_all)
    del df_all

    # Persiste dados brutos anuais
    n_raw = _insert_raw_agg(engine, agg)
    logger.info(f"  Registros brutos inseridos: {n_raw:,}")

    # Calcula growth score
    df_wide = _compute_growth_score(agg)
    n_signals = _insert_growth_signals(engine, df_wide)

    # White spaces: crescimento alto + mercado médio
    with engine.connect() as conn:
        ws = conn.execute(_text(
            "SELECT COUNT(*) FROM scout_view WHERE is_white_space = TRUE"
        )).scalar() or 0
        logger.success(f"  White spaces identificados: {ws} municípios")

    logger.success(f"✅ Agent Scout concluído: {n_signals} municípios com growth signals")

    return {
        "count":          n_signals,
        "white_spaces":   ws,
        "anos":           f"{ANOS[0]}-{ANOS[-1]}",
        "message":        f"Growth signals: {n_signals} municípios SP | {ws} white spaces",
    }

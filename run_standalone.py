"""
PHARMASITE INTELLIGENCE — Pipeline Standalone (sem Docker / sem Postgres)
========================================================================
Executa localmente na sua máquina com acesso à internet.

Output: municipios_sp_scored.csv  (645 municípios SP com score e tier)

Uso:
    pip install requests pandas numpy scikit-learn loguru tenacity
    python run_standalone.py

Fontes de dados:
    IBGE Localidades  → lista municípios + coordenadas
    IBGE Agregados v3 → população total, faixas etárias, renda domiciliar
    CNES DataSUS API  → farmácias, consultórios, laboratórios, clínicas
    IPEADATA API      → PIB per capita, IDH municipal
    ANS               → beneficiários planos de saúde por município

Modelo de scoring: 4 pilares (Demo/Logística/Economia/Saúde) — mesma lógica
do scoring engine v3 em agent1/pipeline/scores.py, adaptado para CSV standalone.
"""

import io
import json
import math
import os
import sys
import time
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

# ─── Config ──────────────────────────────────────────────────────────────────
CACHE_DIR   = Path("cache_standalone")
OUTPUT_CSV  = Path("municipios_sp_scored.csv")
SP_UF_CODE  = 35          # código IBGE do estado SP
SP_UF_SIG   = "SP"

# Distância logística: distribuidor em Campinas (lat/lon IBGE)
CAMPINAS_LAT = -22.9056
CAMPINAS_LON = -47.0608
MAX_VIABLE_KM = 300.0     # raio máximo de atendimento

# Pesos dos pilares (total 450 pts → normalizado)
PILAR_WEIGHTS_RAW = {"demo": 100, "logistica": 100, "economia": 90, "saude": 80, "competitividade": 80}
_TOTAL_W = sum(PILAR_WEIGHTS_RAW.values())
PILAR_WEIGHTS = {k: v / _TOTAL_W for k, v in PILAR_WEIGHTS_RAW.items()}

IBGE_V3  = "https://servicodados.ibge.gov.br/api/v3/agregados"
IBGE_V1  = "https://servicodados.ibge.gov.br/api/v1"
CNES_API = "https://cnes.datasus.gov.br/services/estabelecimentos"
IPEA_API = "http://www.ipeadata.gov.br/api/odata4"
ANS_URL  = "https://www.ans.gov.br/images/stories/Plano_de_saude_e_Operadoras/dados_e_indicadores/Beneficiarios/beneficiarios_municipio_2024.csv"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "pharmasite-intelligence/1.0 (research)"})

# ─── Logging ─────────────────────────────────────────────────────────────────
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}", level="INFO")
logger.add("pipeline_standalone.log", rotation="5 MB", level="DEBUG")

CACHE_DIR.mkdir(exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════
# UTILIDADES
# ═══════════════════════════════════════════════════════════════════════════

def cached_json(name: str, fn, *args, **kwargs):
    """Executa fn() apenas se cache não existir. Retorna dados parseados."""
    path = CACHE_DIR / f"{name}.json"
    if path.exists():
        logger.debug(f"Cache hit: {name}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    logger.debug(f"Cache miss: {name} — coletando...")
    result = fn(*args, **kwargs)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)
    return result


def cached_df(name: str, fn, *args, **kwargs) -> pd.DataFrame:
    path = CACHE_DIR / f"{name}.csv"
    if path.exists():
        logger.debug(f"Cache DF hit: {name}")
        return pd.read_csv(path, dtype=str)
    logger.debug(f"Cache DF miss: {name} — coletando...")
    df = fn(*args, **kwargs)
    df.to_csv(path, index=False)
    return df


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=3, max=30))
def get_json(url: str, timeout: int = 90) -> list | dict:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def centroid_from_geom(geom: dict):
    gtype  = geom.get("type", "")
    coords = geom.get("coordinates", [])
    try:
        if gtype == "Point":
            return coords[1], coords[0]
        elif gtype == "Polygon":
            pts = coords[0]
            return (sum(p[1] for p in pts)/len(pts), sum(p[0] for p in pts)/len(pts))
        elif gtype == "MultiPolygon":
            pts = [p for poly in coords for p in poly[0]]
            return (sum(p[1] for p in pts)/len(pts), sum(p[0] for p in pts)/len(pts))
    except (IndexError, ZeroDivisionError, TypeError):
        pass
    return None, None


def parse_ibge_agregado(data: list) -> pd.DataFrame:
    """Parse formato padrão IBGE v3/agregados → DataFrame(codigo_ibge, value)."""
    rows = []
    for var_block in data:
        for resultado in var_block.get("resultados", []):
            for serie in resultado.get("series", []):
                loc_id = str(serie["localidade"]["id"]).zfill(7)
                vals   = serie.get("serie", {})
                raw    = next(iter(vals.values()), None) if vals else None
                if raw and raw not in ("-", "...", "X"):
                    try:
                        rows.append({"codigo_ibge": loc_id, "value": float(raw)})
                    except (ValueError, TypeError):
                        pass
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["codigo_ibge", "value"])


def fetch_ibge_var(agregado_id: int, var_id: int, periodo: str = "2022",
                    classif: str = "") -> pd.DataFrame:
    suffix = f"&classificacao={classif}" if classif else ""
    url = f"{IBGE_V3}/{agregado_id}/periodos/{periodo}/variaveis/{var_id}?localidades=N6[all]{suffix}"
    try:
        data = get_json(url)
        df = parse_ibge_agregado(data)
        if not df.empty:
            df = df.groupby("codigo_ibge", as_index=False)["value"].sum()
        logger.debug(f"  IBGE Ag.{agregado_id}/Var.{var_id}: {len(df)} municípios")
        return df
    except Exception as e:
        logger.warning(f"  IBGE Ag.{agregado_id}/Var.{var_id} falhou: {e}")
        return pd.DataFrame(columns=["codigo_ibge", "value"])


def norm_minmax(s: pd.Series) -> pd.Series:
    mn, mx = s.min(), s.max()
    return pd.Series(0.5, index=s.index) if mx == mn else (s - mn) / (mx - mn)


def pilar_score(df: pd.DataFrame, sub_weights: dict) -> pd.Series:
    """Calcula score 0-100 de um pilar com sub-indicadores disponíveis."""
    from sklearn.preprocessing import MinMaxScaler
    from sklearn.impute import SimpleImputer

    available = {col: w for col, w in sub_weights.items() if col in df.columns}
    if not available:
        return pd.Series(0.0, index=df.index)

    total_w = sum(available.values())
    cols    = list(available.keys())
    X       = df[cols].copy().astype(float)

    # Prevent SimpleImputer from dropping entirely NaN columns
    X.fillna(0, inplace=True)
    
    X_log  = np.log1p(X)  # Apply LOG so SP (15k) doesn't squash small towns
    X_norm = MinMaxScaler().fit_transform(X_log)
    w      = np.array([available[c] / total_w for c in cols])

    return pd.Series((X_norm * w).sum(axis=1) * 100, index=df.index).round(2)


# ═══════════════════════════════════════════════════════════════════════════
# ETAPA 1 — MUNICÍPIOS SP + COORDENADAS
# ═══════════════════════════════════════════════════════════════════════════

def _fetch_municipios_sp():
    logger.info("IBGE: lista de municípios SP...")
    data = get_json(f"{IBGE_V1}/localidades/estados/{SP_UF_CODE}/municipios")
    return data


def _fetch_coords_sp():
    """GeoJSON SP (malhas API) → dict {codigo_ibge_7: (lat, lon)}."""
    logger.info("IBGE: centróides municípios SP (malhas API)...")
    url = (f"{IBGE_V1.replace('v1','v3')}/malhas/estados/{SP_UF_CODE}"
           f"?intrarregiao=municipio&qualidade=minima&formato=application%2Fvnd.geo%2Bjson")
    try:
        r = SESSION.get(url, timeout=120, headers={"Accept": "application/vnd.geo+json"})
        r.raise_for_status()
        geo = r.json()
        result = {}
        for feat in geo.get("features", []):
            props = feat.get("properties") or {}
            code  = str(props.get("codarea") or props.get("CD_MUN") or "").zfill(7)
            if code and len(code) >= 7:
                lat, lon = centroid_from_geom(feat.get("geometry") or {})
                if lat is not None:
                    result[code] = (lat, lon)
        logger.success(f"  Coordenadas: {len(result)} municípios SP")
        return result
    except Exception as e:
        logger.warning(f"  Coordenadas falhou: {e}")
        return {}


def collect_municipios_sp() -> pd.DataFrame:
    logger.info("═" * 50)
    logger.info("ETAPA 1: Municípios SP")
    logger.info("═" * 50)

    raw  = cached_json("municipios_sp", _fetch_municipios_sp)
    rows = []
    for m in raw:
        try:
            microreg = m.get("microrregiao", {})
            mesoreg  = microreg.get("mesorregiao", {}) if microreg else {}
            rows.append({
                "codigo_ibge": str(m["id"]).zfill(7),
                "nome":        m["nome"],
                "mesorregiao": mesoreg.get("nome", ""),
                "microrregiao": microreg.get("nome", "") if microreg else "",
            })
        except Exception:
            pass

    df = pd.DataFrame(rows)
    logger.success(f"  {len(df)} municípios SP carregados")

    # Coordenadas
    coords = cached_json("coords_sp", _fetch_coords_sp)
    df["latitude"]  = df["codigo_ibge"].map(lambda x: coords.get(x, (None, None))[0])
    df["longitude"] = df["codigo_ibge"].map(lambda x: coords.get(x, (None, None))[1])
    coord_ok = df["latitude"].notna().sum()
    logger.info(f"  Coordenadas: {coord_ok}/{len(df)}")

    return df


# ═══════════════════════════════════════════════════════════════════════════
# ETAPA 2 — DEMOGRÁFICOS (IBGE Agregados v3)
# ═══════════════════════════════════════════════════════════════════════════

def collect_demograficos(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("═" * 50)
    logger.info("ETAPA 2: Demográficos IBGE")
    logger.info("═" * 50)

    SP_CODES = sorted(df["codigo_ibge"].tolist())

    # ── Pop. total (Estimativas 2022: Ag.6579/Var.9324 → fallback Ag.9514/Var.93)
    logger.info("  Pop. total...")
    df_pop = fetch_ibge_var(6579, 9324, "2022")
    if df_pop.empty:
        df_pop = fetch_ibge_var(9514, 93, "2022")
    df = df.merge(df_pop.rename(columns={"value": "populacao_total"}), on="codigo_ibge", how="left")
    logger.info(f"    OK: {df['populacao_total'].notna().sum()}/{len(df)}")
    time.sleep(1)

    # ── Pop. urbana (Ag.1378 Var.93 classif=1[1] Censo 2010)
    logger.info("  Pop. urbana...")
    df_urb = fetch_ibge_var(1378, 93, "2010", "1[1]")
    if not df_urb.empty:
        df = df.merge(df_urb.rename(columns={"value": "populacao_urbana"}), on="codigo_ibge", how="left")
    else:
        df["populacao_urbana"] = None
    time.sleep(1)

    # ── Renda domiciliar per capita (Ag.6691/Var.10605 Censo 2022)
    logger.info("  Renda per capita...")
    df_renda = fetch_ibge_var(6691, 10605, "2022")
    if df_renda.empty:
        df_renda = fetch_ibge_var(9605, 10605, "2022")
    df = df.merge(df_renda.rename(columns={"value": "renda_per_capita"}), on="codigo_ibge", how="left")
    logger.info(f"    OK: {df['renda_per_capita'].notna().sum()}/{len(df)}")
    time.sleep(1)

    # ── Faixas etárias (Ag.1378 Var.93 Censo 2010)
    logger.info("  Faixas etárias...")
    age_groups = {
        "pop_0_4":    "287[93070]",
        "pop_5_14":   "287[93084,93085]",
        "pop_15_29":  "287[107453,111286,93087,93088]",
        "pop_30_44":  "287[93089,93090,93091]",
        "pop_45_64":  "287[93092,93093,93094,93095]",
        "pop_65_plus":"287[93096,496]",
    }
    for col, classif in age_groups.items():
        df_age = fetch_ibge_var(1378, 93, "2010", classif)
        if not df_age.empty:
            df = df.merge(df_age.rename(columns={"value": col}), on="codigo_ibge", how="left")
        else:
            df[col] = None
        time.sleep(0.5)

    # ── Derivados
    for c in df.columns:
        if c.startswith("pop") or c in ["populacao_total","populacao_urbana","renda_per_capita"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["populacao_alvo"] = df[["pop_30_44","pop_45_64"]].sum(axis=1, min_count=1)
    df["taxa_urbanizacao"] = (
        df["populacao_urbana"] / df["populacao_total"].replace(0, np.nan) * 100
    ).clip(0, 100).round(2)

    df["elderly_pct"] = (
        df.get("pop_65_plus", pd.Series(0, index=df.index)).fillna(0) / df["populacao_total"].replace(0, np.nan) * 100
    ).clip(0, 100).round(2)

    jovem = (df.get("pop_0_4", 0).fillna(0) + df.get("pop_5_14", 0).fillna(0)).replace(0, 1)
    df["indice_envelhecimento"] = (df.get("pop_65_plus", pd.Series(0, index=df.index)).fillna(0) / jovem * 100).round(2)

    pop_ok = df["populacao_total"].notna().sum()
    logger.success(f"  Demográficos: pop_total={pop_ok}/{len(df)} | alvo={df['populacao_alvo'].notna().sum()}")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# ETAPA 3 — CNES (estabelecimentos de saúde)
# ═══════════════════════════════════════════════════════════════════════════

TIPOS_CNES = {
    "01": "ubs_upa", "02": "hospitais", "04": "hospitais",
    "05": "hospitais", "07": "ubs_upa", "20": "clinicas",
    "21": "consultorios_medicos", "22": "consultorios_odonto",
    "23": "consultorios_medicos", "36": "clinicas", "39": "laboratorios",
    "43": "farmacias", "60": "laboratorios", "70": "consultorios_odonto",
    "71": "consultorios_odonto", "79": "farmacias",
}


def _fetch_cnes_sp():
    logger.info("CNES: coletando estabelecimentos SP via API REST...")
    cats = set(TIPOS_CNES.values())
    all_counts = {}  # {codigo_ibge: {cat: count}}

    url = f"{CNES_API}?estado={SP_UF_SIG}&status=1&limit=10000"
    try:
        r = SESSION.get(url, timeout=120, headers={"Accept": "application/json"})
        if r.status_code == 200:
            data = r.json()
            items = data if isinstance(data, list) else data.get("itens", data.get("data", []))
            logger.info(f"  CNES API: {len(items)} estabelecimentos retornados")

            for item in items:
                cod = str(item.get("codigoMunicipio",
                          item.get("co_municipio", ""))).zfill(7)
                tipo = str(item.get("tipoEstabelecimento",
                           item.get("tp_pfpj", ""))).zfill(2)
                if cod not in all_counts:
                    all_counts[cod] = {c: 0 for c in cats}
                if tipo in TIPOS_CNES:
                    all_counts[cod][TIPOS_CNES[tipo]] = all_counts[cod].get(TIPOS_CNES[tipo], 0) + 1
        else:
            logger.warning(f"  CNES API status {r.status_code} — tentando fallback por UF paginado")
            all_counts = _cnes_paginated_fallback()
    except Exception as e:
        logger.warning(f"  CNES API erro: {e} — tentando fallback paginado")
        all_counts = _cnes_paginated_fallback()

    return all_counts


def _cnes_paginated_fallback():
    """Fallback: API CNES paginada com offset."""
    cats = set(TIPOS_CNES.values())
    all_counts = {}
    base = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos"
    offset, limit = 0, 50
    erros = 0

    while erros < 5:
        params = {"co_uf": str(SP_UF_CODE), "ds_uf_sigla": SP_UF_SIG,
                  "limit": limit, "offset": offset}
        try:
            r = SESSION.get(base, params=params, timeout=30)
            if r.status_code in (404, 204):
                break
            r.raise_for_status()
            data = r.json()
            batch = data if isinstance(data, list) else data.get("estabelecimentos",
                    data.get("items", data.get("data", [])))
            if not batch:
                break

            for item in batch:
                cod  = str(item.get("co_municipio","")).zfill(7)
                tipo = str(item.get("tp_pfpj","")).zfill(2)
                if cod not in all_counts:
                    all_counts[cod] = {c: 0 for c in cats}
                if tipo in TIPOS_CNES:
                    all_counts[cod][TIPOS_CNES[tipo]] += 1

            offset += limit
            if len(batch) < limit:
                break
            time.sleep(0.3)
        except Exception as e:
            logger.warning(f"  CNES paginado offset={offset}: {e}")
            erros += 1
            time.sleep(2)
            offset += limit

    return all_counts


def collect_cnes(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("═" * 50)
    logger.info("ETAPA 3: CNES DataSUS")
    logger.info("═" * 50)

    cache_path = CACHE_DIR / "cnes_sp.json"
    if cache_path.exists():
        with open(cache_path) as f:
            counts = json.load(f)
        logger.debug("Cache CNES hit")
    else:
        counts = _fetch_cnes_sp()
        with open(cache_path, "w") as f:
            json.dump(counts, f)

    cats = ["farmacias", "consultorios_medicos", "consultorios_odonto",
            "laboratorios", "clinicas", "hospitais", "ubs_upa"]

    rows = []
    for cod, c in counts.items():
        row = {"codigo_ibge": cod.zfill(7)}
        for cat in cats:
            row[cat] = c.get(cat, 0)
        rows.append(row)

    if rows:
        df_cnes = pd.DataFrame(rows)
        # CNES pode usar código 6 dígitos — alinhar com 7
        df["co6"] = df["codigo_ibge"].str[:6]
        df_cnes["co6"] = df_cnes["codigo_ibge"].str[:6]
        df = df.merge(df_cnes.drop(columns=["codigo_ibge"]), on="co6", how="left")
        df = df.drop(columns=["co6"])
    else:
        for cat in cats:
            df[cat] = 0
        logger.warning("  CNES: sem dados retornados — zeros inseridos")

    for c in cats:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0).astype(int)

    # Densidade de farmácias por 10k habitantes
    pop_num = pd.to_numeric(df.get("populacao_total", 0), errors="coerce").replace(0, np.nan)
    df["farmacias_por_10k"] = (df["farmacias"] / pop_num * 10000).round(2)

    n_farm = df["farmacias"].sum()
    logger.success(f"  CNES: {n_farm} farmácias mapeadas | {(df['farmacias']>0).sum()} municípios com ≥1")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# ETAPA 4 — ECONÔMICOS (IPEADATA + ANS)
# ═══════════════════════════════════════════════════════════════════════════

def _fetch_ipeadata(serie_id: str) -> pd.DataFrame:
    url = (f"{IPEA_API}/Metadados('{serie_id}')/Valores"
           f"?$top=6000&$select=TERCODIGO,VALDATA,VALVALOR&$orderby=VALDATA desc")
    try:
        data = get_json(url)
        vals = data.get("value", [])
        if not vals:
            return pd.DataFrame(columns=["codigo_ibge","value"])
        df = pd.DataFrame(vals)
        df["codigo_ibge"] = df["TERCODIGO"].astype(str).str.zfill(7)
        df["value"] = pd.to_numeric(df["VALVALOR"], errors="coerce")
        df = df.dropna(subset=["value"])
        df = df.sort_values("VALDATA", ascending=False).groupby("codigo_ibge").first().reset_index()
        logger.success(f"    IPEADATA {serie_id}: {len(df)} municípios")
        return df[["codigo_ibge", "value"]]
    except Exception as e:
        logger.warning(f"    IPEADATA {serie_id} falhou: {e}")
        return pd.DataFrame(columns=["codigo_ibge","value"])


def _fetch_ans():
    logger.info("  ANS: beneficiários planos de saúde 2024...")
    try:
        r = SESSION.get(ANS_URL, timeout=120)
        if r.status_code == 200:
            content = r.content.decode("latin-1")
            df = pd.read_csv(io.StringIO(content), sep=";", decimal=",", low_memory=False)
            mun_col = next((c for c in df.columns if "municipio" in c.lower() or "ibge" in c.lower()), None)
            ben_col = next((c for c in df.columns if "benefi" in c.lower()), None)
            if mun_col and ben_col:
                df["codigo_ibge"] = df[mun_col].astype(str).str.zfill(7)
                df["beneficiarios_planos"] = pd.to_numeric(df[ben_col], errors="coerce")
                agg = df.groupby("codigo_ibge")["beneficiarios_planos"].sum().reset_index()
                logger.success(f"    ANS: {len(agg)} municípios")
                return agg.to_dict("records")
            else:
                logger.warning(f"    ANS: colunas não identificadas. Disponíveis: {list(df.columns[:8])}")
        else:
            logger.warning(f"    ANS HTTP {r.status_code}")
    except Exception as e:
        logger.warning(f"    ANS erro: {e}")
    return []


def collect_economicos(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("═" * 50)
    logger.info("ETAPA 4: Indicadores Econômicos (IPEADATA + ANS)")
    logger.info("═" * 50)

    # PIB per capita municipal
    logger.info("  PIB per capita (IPEADATA)...")
    df_pib = cached_df("ipeadata_pib", lambda: _fetch_ipeadata("PIBMUNIC_PIBPMCAP"))
    if not df_pib.empty:
        df_pib["value"] = pd.to_numeric(df_pib["value"], errors="coerce")
        df = df.merge(df_pib.rename(columns={"value": "pib_per_capita"}), on="codigo_ibge", how="left")
    else:
        df["pib_per_capita"] = None
    time.sleep(1)

    # IDH municipal
    logger.info("  IDH (IPEADATA)...")
    df_idh = cached_df("ipeadata_idh", lambda: _fetch_ipeadata("IDHM_IDHM"))
    if not df_idh.empty:
        df_idh["value"] = pd.to_numeric(df_idh["value"], errors="coerce")
        df = df.merge(df_idh.rename(columns={"value": "idh"}), on="codigo_ibge", how="left")
    else:
        df["idh"] = None
    time.sleep(1)

    # ANS beneficiários
    logger.info("  ANS beneficiários...")
    ans_records = cached_json("ans_beneficiarios", _fetch_ans)
    if ans_records:
        df_ans = pd.DataFrame(ans_records)
        df_ans["codigo_ibge"] = df_ans["codigo_ibge"].astype(str).str.zfill(7)
        df_ans["beneficiarios_planos"] = pd.to_numeric(df_ans["beneficiarios_planos"], errors="coerce")
        df = df.merge(df_ans, on="codigo_ibge", how="left")
    else:
        df["beneficiarios_planos"] = None

    # Cobertura planos (%)
    pop_num = pd.to_numeric(df.get("populacao_total", 0), errors="coerce").replace(0, np.nan)
    df["cobertura_planos_pct"] = (
        pd.to_numeric(df.get("beneficiarios_planos", 0), errors="coerce")
        / pop_num * 100
    ).clip(0, 100).round(2)

    pib_ok = pd.to_numeric(df.get("pib_per_capita"), errors="coerce").notna().sum()
    idh_ok = pd.to_numeric(df.get("idh"), errors="coerce").notna().sum()
    ans_ok = pd.to_numeric(df.get("beneficiarios_planos"), errors="coerce").notna().sum()
    logger.success(f"  Econômicos: PIB={pib_ok} | IDH={idh_ok} | ANS={ans_ok}/{len(df)}")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# ETAPA 5 — SCORING
# ═══════════════════════════════════════════════════════════════════════════

# Referência empírica (validação do modelo)
EMPIRICAL_TOP = {
    "3509502": "Campinas",   "3536505": "Paulínia",   "3550308": "São Paulo",
    "3525904": "Jundiaí",   "3556206": "Valinhos",   "3556701": "Vinhedo",
}
EMPIRICAL_BOTTOM = {
    "3506706": "Borá",        "3533205": "Nova Castilho",
    "3516200": "Flora Rica",  "3556909": "Uru",
}

# Sub-pesos dos pilares (mesmos de scores.py)
DEMO_SUB = {
    "populacao_total":       0.40,
    "populacao_alvo":        0.35,
    "taxa_urbanizacao":      0.15,
    "indice_envelhecimento": 0.10,
}
LOGISTICA_SUB = {
    "score_logistico":   0.45,
    "farmacias":         0.40,
    "farmacias_por_10k": 0.15,
}
ECONOMIA_SUB = {
    "renda_per_capita":     0.40,
    "pib_per_capita":       0.25,
    "idh":                  0.20,
    "cobertura_planos_pct": 0.15,
}
SAUDE_SUB = {
    "farmacias":           0.55,
    "consultorios_odonto": 0.20,
    "laboratorios":        0.15,
    "clinicas":            0.10,
}


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("═" * 50)
    logger.info("ETAPA 5: Scoring (modelo aditivo v3 — 4 pilares)")
    logger.info("═" * 50)

    # Garantir tipos numéricos
    numeric_cols = [
        "populacao_total","populacao_alvo","taxa_urbanizacao","indice_envelhecimento",
        "renda_per_capita","pib_per_capita","idh","cobertura_planos_pct",
        "farmacias","consultorios_odonto","laboratorios","clinicas",
        "farmacias_por_10k","latitude","longitude",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Distância logística de Campinas
    def dist_campinas(row):
        if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
            return haversine_km(row["latitude"], row["longitude"], CAMPINAS_LAT, CAMPINAS_LON)
        return MAX_VIABLE_KM

    df["distance_campinas_km"] = df.apply(dist_campinas, axis=1).round(2)
    df["score_logistico"]      = ((1 - df["distance_campinas_km"] / MAX_VIABLE_KM).clip(0,1) * 100).round(2)

    logger.info(f"  Distância mediana: {df['distance_campinas_km'].median():.0f} km | "
                f"dentro de {MAX_VIABLE_KM:.0f}km: {(df['distance_campinas_km']<=MAX_VIABLE_KM).sum()}")

    # Scores por pilar
    df["score_demografico"]  = pilar_score(df, DEMO_SUB)
    df["score_logistica"]    = pilar_score(df, LOGISTICA_SUB)
    df["score_economico"]    = pilar_score(df, ECONOMIA_SUB)
    df["score_saude"]        = pilar_score(df, SAUDE_SUB)

    # Competitividade: Para um DISTRIBUIDOR, mais farmácias = mais clientes = MAIOR score
    densidade = df["farmacias_por_10k"].fillna(df["farmacias_por_10k"].median())
    df["score_competitividade"] = (densidade.rank(pct=True, method='average') * 100).round(2)

    # Score total ponderado
    df["score"] = (
        df["score_demografico"]     * PILAR_WEIGHTS["demo"]      +
        df["score_logistica"]       * PILAR_WEIGHTS["logistica"] +
        df["score_economico"]       * PILAR_WEIGHTS["economia"]  +
        df["score_saude"]           * PILAR_WEIGHTS["saude"]     +
        df["score_competitividade"] * PILAR_WEIGHTS["competitividade"]
    ).round(1)

    # Tier por quartil (igual ao modelo v3)
    p75 = df["score"].quantile(0.75)
    p50 = df["score"].quantile(0.50)
    p25 = df["score"].quantile(0.25)

    def tier(s):
        if s >= p75: return "A"
        if s >= p50: return "B"
        if s >= p25: return "C"
        return "D"

    df["tier"] = df["score"].apply(tier)
    df["ranking"] = df["score"].rank(ascending=False, method="min").astype(int)

    logger.info(f"  Tiers → A:{(df['tier']=='A').sum()} B:{(df['tier']=='B').sum()} "
                f"C:{(df['tier']=='C').sum()} D:{(df['tier']=='D').sum()}")
    logger.info(f"  Score: min={df['score'].min():.1f} | mediana={df['score'].median():.1f} | max={df['score'].max():.1f}")
    logger.info(f"  Cortes de tier: A≥{p75:.1f} | B≥{p50:.1f} | C≥{p25:.1f} | D<{p25:.1f}")

    # Validação empírica
    _validate_empirical(df)

    return df


def _validate_empirical(df: pd.DataFrame):
    logger.info("  ── Validação empírica (ranking declarado pelo cliente) ──")
    check = df.set_index("codigo_ibge")[["score","tier","ranking"]].copy()
    total = len(check)

    for code, name in EMPIRICAL_TOP.items():
        if code in check.index:
            row = check.loc[code]
            logger.info(f"  ✓ {name:18s} → score {row['score']:5.1f} | rank {int(row['ranking']):4d}/{total} | Tier {row['tier']}")
        else:
            logger.warning(f"  ✗ {name}: não encontrado no dataset")

    for code, name in EMPIRICAL_BOTTOM.items():
        if code in check.index:
            row = check.loc[code]
            logger.info(f"  ▼ {name:18s} → score {row['score']:5.1f} | rank {int(row['ranking']):4d}/{total} | Tier {row['tier']}")

    # Verificação Campinas (deve estar no top 5%)
    if "3509502" in check.index:
        rank_c = check.loc["3509502", "ranking"]
        pct = rank_c / total * 100
        if pct <= 5:
            logger.success(f"  ✅ Campinas rank {int(rank_c)}/{total} (top {pct:.1f}%) — OK")
        else:
            logger.warning(f"  ⚠️  Campinas rank {int(rank_c)}/{total} ({pct:.1f}%) — esperado top 5%. Revisar pesos.")


# ═══════════════════════════════════════════════════════════════════════════
# ETAPA 6 — OUTPUT CSV
# ═══════════════════════════════════════════════════════════════════════════

COLS_OUTPUT = [
    "codigo_ibge", "nome", "mesorregiao", "microrregiao",
    "score", "tier", "ranking",
    "score_demografico", "score_logistica", "score_economico", "score_saude", "score_competitividade",
    "farmacias", "consultorios_odonto", "laboratorios", "clinicas", "hospitais", "consultorios_medicos", "ubs_upa",
    "farmacias_por_10k",
    "populacao_total", "populacao_alvo", "taxa_urbanizacao", "indice_envelhecimento", "elderly_pct",
    "renda_per_capita", "pib_per_capita", "idh",
    "cobertura_planos_pct",
    "distance_campinas_km",
    "latitude", "longitude",
]


def save_output(df: pd.DataFrame):
    logger.info("═" * 50)
    logger.info("ETAPA 6: Salvando CSV")
    logger.info("═" * 50)

    df_out = df.sort_values("score", ascending=False).reset_index(drop=True)

    # Selecionar apenas colunas que existem
    cols = [c for c in COLS_OUTPUT if c in df_out.columns]
    df_out = df_out[cols]

    # Arredondar floats
    float_cols = df_out.select_dtypes(include=["float64","float32"]).columns
    df_out[float_cols] = df_out[float_cols].round(2)

    df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    logger.success(f"  ✓ Salvo: {OUTPUT_CSV}  ({len(df_out)} municípios, {len(cols)} colunas)")

    # Top 10
    logger.info("\n" + "═" * 70)
    logger.info("TOP 10 MUNICÍPIOS SP — Potencial para Correlatos Farmacêuticos")
    logger.info("═" * 70)
    top10_cols = ["nome","score","tier","farmacias","populacao_total",
                  "renda_per_capita","cobertura_planos_pct","distance_campinas_km"]
    top10_cols = [c for c in top10_cols if c in df_out.columns]
    print(df_out.head(10)[top10_cols].to_string(index=False))

    logger.info("\n📊 Distribuição por Tier:")
    print(df_out["tier"].value_counts().sort_index().to_string())


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("PHARMASITE INTELLIGENCE — Pipeline Standalone")
    logger.info("Foco: 645 municípios do Estado de São Paulo")
    logger.info("Modelo: Scoring Aditivo v3 (4 Pilares / 370 pts)")
    logger.info("=" * 60)

    # Verificar dependências
    try:
        import sklearn
    except ImportError:
        logger.error("scikit-learn não encontrado. Execute: pip install scikit-learn")
        sys.exit(1)

    # Pipeline
    df = collect_municipios_sp()       # IBGE lista + coordenadas
    df = collect_demograficos(df)      # IBGE Agregados (pop, renda, faixas etárias)
    df = collect_cnes(df)              # CNES estabelecimentos saúde
    df = collect_economicos(df)        # IPEADATA (PIB/IDH) + ANS (planos)
    df = calculate_scores(df)          # Modelo 4 pilares
    save_output(df)                    # CSV final

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.success(f"Pipeline concluído em {elapsed/60:.1f} minutos")
    logger.info(f"Output: {OUTPUT_CSV.resolve()}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

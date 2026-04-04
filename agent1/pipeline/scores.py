"""
PHARMASITE INTELLIGENCE — Scoring Engine v3.0
Modelo: Score Aditivo por Pilares (transparente e auditável)

Pilares conforme questionário do cliente (Projeto Index Calculation):
  Demo      100 pts  → tamanho e perfil da população
  Logística 100 pts  → distância + densidade de PDVs (eficiência de rota)
  Economia   90 pts  → renda, PIB, IDH, planos de saúde
  Saúde      80 pts  → farmácias (mercado), odonto, laboratórios, clínicas

Total: 370 pts → normalizado para 0-100.

KPI decisivo (declarado pelo cliente): volume absoluto de farmácias (PDVs).
Motivo: proxy confiável para todos os demais indicadores + driver de eficiência logística.

Validação empírica (ranking declarado pelo cliente):
  Top: Campinas, Paulínia, São Paulo, Jundiaí, Valinhos/Vinhedo
  Red flags: Borá, Nova Castilho, Flora Rica, Uru, Gavião Peixoto
"""

import math
import numpy as np
import pandas as pd
from sqlalchemy import text
from loguru import logger
from sklearn.preprocessing import MinMaxScaler
from sklearn.impute import SimpleImputer


# ── Constantes geográficas e logísticas ──────────────────────────────────────
CAMPINAS_LAT  = -22.9056   # IBGE 3509502
CAMPINAS_LON  = -47.0608
MAX_VIABLE_KM = 200.0      # raio atual; cresce para 300 com expansão estrutural

# ── Pilares: pesos absolutos → normalizados em runtime ───────────────────────
# Fonte: questionário do cliente, seção "Distribuição de pesos (total: 370 pontos)"
PILAR_WEIGHTS_RAW = {
    "demo":      100,
    "logistica": 100,
    "economia":   90,
    "saude":      80,
}
_total = sum(PILAR_WEIGHTS_RAW.values())  # 370
PILAR_WEIGHTS = {k: v / _total for k, v in PILAR_WEIGHTS_RAW.items()}

# ── Sub-indicadores por pilar (pesos internos somam 1.0) ─────────────────────

# DEMO: quem mora aqui e compra farmácia
DEMO_SUB = {
    "populacao_total":       0.40,   # tamanho absoluto do mercado
    "populacao_alvo":        0.35,   # 30-64 anos — core buyer
    "taxa_urbanizacao":      0.15,   # urbano = PDVs acessíveis
    "indice_envelhecimento": 0.10,   # mais idosos = mais correlatos crônicos
}

# LOGÍSTICA: o cliente consegue servir de forma lucrativa?
# Lógica: rota rentável = muitos PDVs próximos entre si + base próxima
LOGISTICA_SUB = {
    "score_logistico":   0.45,   # distância haversine de Campinas (0-100)
    "farmacias":         0.40,   # volume absoluto de PDVs = KPI #1 do cliente
    "farmacias_por_10k": 0.15,   # densidade relativa (captura Paulínia: poucos PDVs, alto ticket)
}

# ECONOMIA: capacidade de compra dos PDVs e da população local
ECONOMIA_SUB = {
    "renda_per_capita":     0.40,   # poder de compra real
    "pib_per_capita":       0.25,   # atividade econômica geral
    "idh":                  0.20,   # desenvolvimento humano (corr. com consumo de saúde)
    "cobertura_planos_pct": 0.15,   # acesso a planos → maior giro em correlatos
}

# SAÚDE: ecossistema de PDVs e complementares
SAUDE_SUB = {
    "farmacias":           0.55,   # mercado existente — tamanho absoluto
    "consultorios_odonto": 0.20,   # key metric para correlatos odontológicos
    "laboratorios":        0.15,   # diagnóstico → receitas
    "clinicas":            0.10,   # fluxo de pacientes
}

# ── Municípios de validação empírica (IBGE codes) ────────────────────────────
EMPIRICAL_TOP = {
    "3509502": "Campinas",
    "3536505": "Paulínia",
    "3550308": "São Paulo",
    "3525904": "Jundiaí",
    "3556206": "Valinhos",
    "3556701": "Vinhedo",
}
EMPIRICAL_BOTTOM = {
    "3506706": "Borá",
    "3533205": "Nova Castilho",
    "3516200": "Flora Rica",
    "3556909": "Uru",
    "3516952": "Gavião Peixoto",
}


# ─────────────────────────────────────────────────────────────────────────────
# Utilitários
# ─────────────────────────────────────────────────────────────────────────────

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _pilar_score(df: pd.DataFrame, sub_weights: dict, scaler_cache: dict) -> pd.Series:
    """
    Calcula o score de um pilar (0-100) como combinação linear dos sub-indicadores.
    Colunas ausentes são ignoradas com peso redistribuído.
    scaler_cache: dict passado por referência para reutilizar scalers entre chamadas.
    """
    available = {col: w for col, w in sub_weights.items() if col in df.columns}
    if not available:
        return pd.Series([0.0] * len(df), index=df.index)

    total_w = sum(available.values())
    cols = list(available.keys())

    X = df[cols].copy()

    # Imputação com mediana (colunas com NaN)
    imputer = SimpleImputer(strategy="median")
    X_imp = imputer.fit_transform(X)

    # Normalização 0-1 global (MinMax)
    scaler = MinMaxScaler()
    X_norm = scaler.fit_transform(X_imp)

    # Pesos redistribuídos
    weights = np.array([available[c] / total_w for c in cols])

    return pd.Series((X_norm * weights).sum(axis=1) * 100, index=df.index).round(2)


def _validate_empirical_ranking(df: pd.DataFrame) -> None:
    """
    Verifica se o ranking do modelo é consistente com o ranking empírico declarado
    pelo cliente. Gera WARNING se houver inconsistências graves.
    """
    df_check = df.set_index("codigo_ibge")[["score_total", "ranking_nacional", "tier"]].copy()
    total    = len(df_check)

    top_results = []
    for code, name in EMPIRICAL_TOP.items():
        if code in df_check.index:
            row = df_check.loc[code]
            top_pct = (1 - row["ranking_nacional"] / total) * 100
            top_results.append(
                f"  {name:20s} → score {row['score_total']:5.1f} | "
                f"rank {row['ranking_nacional']:4d}/{total} | Tier {row['tier']} | "
                f"top {top_pct:.0f}%"
            )
        else:
            top_results.append(f"  {name:20s} → NÃO ENCONTRADO no dataset")

    bot_results = []
    for code, name in EMPIRICAL_BOTTOM.items():
        if code in df_check.index:
            row = df_check.loc[code]
            bot_pct = (row["ranking_nacional"] / total) * 100
            bot_results.append(
                f"  {name:20s} → score {row['score_total']:5.1f} | "
                f"rank {row['ranking_nacional']:4d}/{total} | Tier {row['tier']} | "
                f"bottom {100-bot_pct:.0f}%"
            )
        else:
            bot_results.append(f"  {name:20s} → NÃO ENCONTRADO no dataset")

    logger.info("━━━ VALIDAÇÃO EMPÍRICA (ranking declarado pelo cliente) ━━━")
    logger.info("  TOP (esperado: Tier A ou próximo):")
    for r in top_results:
        logger.info(r)
    logger.info("  BOTTOM (esperado: Tier D ou próximo):")
    for r in bot_results:
        logger.info(r)

    # Campinas deve estar no top 5% nacional
    if "3509502" in df_check.index:
        campinas_rank = df_check.loc["3509502", "ranking_nacional"]
        if campinas_rank > total * 0.05:
            logger.warning(
                f"⚠️  VALIDAÇÃO: Campinas está no rank {campinas_rank} "
                f"(esperado top 5%). Revisar pesos do modelo."
            )
        else:
            logger.success(f"✅  VALIDAÇÃO: Campinas no rank {campinas_rank} — consistente com ranking empírico.")

    # Borá deve estar no bottom 10%
    if "3506706" in df_check.index:
        bora_rank = df_check.loc["3506706", "ranking_nacional"]
        if bora_rank < total * 0.90:
            logger.warning(
                f"⚠️  VALIDAÇÃO: Borá está no rank {bora_rank} "
                f"(esperado bottom 10%). Revisar pesos do modelo."
            )
        else:
            logger.success(f"✅  VALIDAÇÃO: Borá no rank {bora_rank} — consistente com ranking empírico.")

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


# ─────────────────────────────────────────────────────────────────────────────
# Função principal
# ─────────────────────────────────────────────────────────────────────────────

def calculate_scores(engine) -> dict:
    logger.info("🎯 Calculando scores ADITIVOS por pilares (modelo v3.0 — transparente)...")
    logger.info(
        f"  Pesos pilares: Demo={PILAR_WEIGHTS_RAW['demo']} "
        f"Logística={PILAR_WEIGHTS_RAW['logistica']} "
        f"Economia={PILAR_WEIGHTS_RAW['economia']} "
        f"Saúde={PILAR_WEIGHTS_RAW['saude']} / 370 total"
    )

    # ── 1. Carregar dados ─────────────────────────────────────────────────────
    query = """
        SELECT
            m.codigo_ibge, m.uf, m.latitude, m.longitude,
            d.populacao_total, d.populacao_alvo, d.pct_populacao_alvo,
            d.indice_envelhecimento, d.taxa_urbanizacao, d.renda_per_capita,
            e.farmacias, e.consultorios_odonto, e.laboratorios, e.clinicas,
            e.farmacias_por_10k,
            ec.pib_per_capita, ec.cobertura_planos_pct, ec.idh
        FROM municipios m
        LEFT JOIN LATERAL (
            SELECT * FROM demograficos
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) d ON true
        LEFT JOIN LATERAL (
            SELECT * FROM estabelecimentos_saude
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) e ON true
        LEFT JOIN LATERAL (
            SELECT * FROM indicadores_economicos
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) ec ON true
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    logger.info(f"  Carregados {len(df)} municípios")

    if df.empty:
        logger.error("DataFrame vazio — pipeline anterior pode ter falhado")
        return {"count": 0, "message": "Sem dados para scoring"}

    # ── 2. Score logístico (distância haversine de Campinas) ──────────────────
    def _dist(r):
        if pd.notna(r.get("latitude")) and pd.notna(r.get("longitude")):
            return _haversine_km(r["latitude"], r["longitude"], CAMPINAS_LAT, CAMPINAS_LON)
        return MAX_VIABLE_KM

    df["distance_campinas_km"] = df.apply(_dist, axis=1).round(2)

    # Decaimento linear: 0 km → 100, MAX_VIABLE_KM → 0, além → 0
    # Municípios fora do raio recebem score logístico = 0 (filtro eliminatório)
    df["score_logistico"] = (
        (1 - df["distance_campinas_km"] / MAX_VIABLE_KM).clip(0, 1) * 100
    ).round(2)

    inside_radius = (df["distance_campinas_km"] <= MAX_VIABLE_KM).sum()
    logger.info(
        f"  Distância → mediana: {df['distance_campinas_km'].median():.0f} km | "
        f"dentro de {MAX_VIABLE_KM:.0f} km: {inside_radius} municípios "
        f"({inside_radius/len(df)*100:.1f}%)"
    )

    # ── 3. Score de cada pilar (0-100 cada) ───────────────────────────────────
    scaler_cache: dict = {}

    df["score_demografico"]          = _pilar_score(df, DEMO_SUB,      scaler_cache)
    df["score_infraestrutura_saude"] = _pilar_score(df, SAUDE_SUB,     scaler_cache)
    df["score_economico"]            = _pilar_score(df, ECONOMIA_SUB,  scaler_cache)
    # Logística: usa df com score_logistico já calculado
    df["score_logistica_pilar"]      = _pilar_score(df, LOGISTICA_SUB, scaler_cache)

    # ── 4. Score total: combinação linear ponderada dos 4 pilares ─────────────
    df["score_total"] = (
        df["score_demografico"]          * PILAR_WEIGHTS["demo"]      +
        df["score_logistica_pilar"]      * PILAR_WEIGHTS["logistica"] +
        df["score_economico"]            * PILAR_WEIGHTS["economia"]  +
        df["score_infraestrutura_saude"] * PILAR_WEIGHTS["saude"]
    ).round(2)

    logger.info("  Pilares calculados com pesos:")
    for pilar, w in PILAR_WEIGHTS.items():
        raw = PILAR_WEIGHTS_RAW[pilar]
        logger.info(f"    {pilar:12s}: {raw:3d}/370 = {w*100:.1f}%")

    # ── 5. Tier (quartis nacionais) ───────────────────────────────────────────
    p75 = df["score_total"].quantile(0.75)
    p50 = df["score_total"].quantile(0.50)
    p25 = df["score_total"].quantile(0.25)

    def _tier(s):
        if s >= p75: return "A"
        if s >= p50: return "B"
        if s >= p25: return "C"
        return "D"

    df["tier"] = df["score_total"].apply(_tier)

    logger.info(
        f"  Tiers → A: {(df['tier']=='A').sum()} | B: {(df['tier']=='B').sum()} | "
        f"C: {(df['tier']=='C').sum()} | D: {(df['tier']=='D').sum()}"
    )

    # ── 6. Rankings ───────────────────────────────────────────────────────────
    df["ranking_nacional"] = df["score_total"].rank(ascending=False, method="min").astype(int)
    df["ranking_estadual"] = (
        df.groupby("uf")["score_total"].rank(ascending=False, method="min").astype(int)
    )

    top10 = df.nlargest(10, "score_total")[["codigo_ibge", "score_total", "tier", "distance_campinas_km"]]
    logger.info(f"  Top 10 Nacional:\n{top10.to_string(index=False)}")

    # ── 7. Validação empírica ─────────────────────────────────────────────────
    _validate_empirical_ranking(df)

    # ── 8. Persistência ───────────────────────────────────────────────────────
    logger.info(f"  💾 Inserindo {len(df)} scores no banco...")

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO scores (
                    codigo_ibge, score_demografico, score_infraestrutura_saude,
                    score_economico, score_logistico, score_competitividade,
                    score_total, tier, ranking_nacional, ranking_estadual,
                    pca_component_1, pca_component_2, pca_component_3,
                    distance_campinas_km
                ) VALUES (
                    :codigo_ibge, :score_dem, :score_saude, :score_econ,
                    :score_log, :score_comp, :score_total, :tier,
                    :rank_nac, :rank_est, :pca1, :pca2, :pca3, :dist_campinas
                )
                ON CONFLICT (codigo_ibge) DO UPDATE SET
                    score_demografico           = EXCLUDED.score_demografico,
                    score_infraestrutura_saude  = EXCLUDED.score_infraestrutura_saude,
                    score_economico             = EXCLUDED.score_economico,
                    score_logistico             = EXCLUDED.score_logistico,
                    score_competitividade       = EXCLUDED.score_competitividade,
                    score_total                 = EXCLUDED.score_total,
                    tier                        = EXCLUDED.tier,
                    ranking_nacional            = EXCLUDED.ranking_nacional,
                    ranking_estadual            = EXCLUDED.ranking_estadual,
                    pca_component_1             = EXCLUDED.pca_component_1,
                    pca_component_2             = EXCLUDED.pca_component_2,
                    pca_component_3             = EXCLUDED.pca_component_3,
                    distance_campinas_km        = EXCLUDED.distance_campinas_km,
                    calculated_at               = NOW()
            """), {
                "codigo_ibge":  row["codigo_ibge"],
                "score_dem":    float(row.get("score_demografico", 0)),
                "score_saude":  float(row.get("score_infraestrutura_saude", 0)),
                "score_econ":   float(row.get("score_economico", 0)),
                # score_logistico (DB column) = pilar score (não a distância raw)
                "score_log":    float(row.get("score_logistica_pilar", 0)),
                "score_comp":   50.0,          # reservado para positivação futura
                "score_total":  float(row["score_total"]),
                "tier":         row["tier"],
                "rank_nac":     int(row["ranking_nacional"]),
                "rank_est":     int(row["ranking_estadual"]),
                "pca1":         0.0,           # mantido por compatibilidade de schema
                "pca2":         0.0,
                "pca3":         0.0,
                "dist_campinas": float(row.get("distance_campinas_km", MAX_VIABLE_KM)),
            })

    top_mun = df.nlargest(1, "score_total")["codigo_ibge"].values[0]
    logger.success(f"✅ Scores v3.0 calculados: {len(df)} municípios | Top: {top_mun}")

    return {
        "count":   len(df),
        "model":   "additive_pilar_v3",
        "weights": PILAR_WEIGHTS_RAW,
        "message": f"Scores aditivos v3.0. Top município: {top_mun}",
    }

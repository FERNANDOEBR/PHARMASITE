-- ============================================================
-- ENTREGA 3: Agent Scout — Sinais de Crescimento por CEP
-- Receita Federal (delta CNPJs) + IBGE Projeções Populacionais
-- ============================================================

-- ── Abertura de CNPJs por CEP + CNAE + Ano ───────────────────────────────────
-- Base bruta: uma linha por (cep, cnae_grupo, ano)
CREATE TABLE IF NOT EXISTS cnpj_abertura_anual (
    id           SERIAL PRIMARY KEY,
    codigo_ibge  VARCHAR(7)  NOT NULL,
    cep          VARCHAR(8),                    -- 8 dígitos sem hífen
    cnae_grupo   VARCHAR(6)  NOT NULL,          -- ex: "4771", "4110", "6810"
    cnae_descr   VARCHAR(200),
    ano          SMALLINT    NOT NULL,          -- ano de abertura
    qtd_abertos  INTEGER     NOT NULL DEFAULT 0,
    uf           CHAR(2)     NOT NULL DEFAULT 'SP',
    ingested_at  TIMESTAMP   DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cnpj_anual_uk
    ON cnpj_abertura_anual(codigo_ibge, cep, cnae_grupo, ano);

CREATE INDEX IF NOT EXISTS idx_cnpj_anual_ibge   ON cnpj_abertura_anual(codigo_ibge);
CREATE INDEX IF NOT EXISTS idx_cnpj_anual_cep    ON cnpj_abertura_anual(cep);
CREATE INDEX IF NOT EXISTS idx_cnpj_anual_cnae   ON cnpj_abertura_anual(cnae_grupo);

-- ── Growth Signals: score de crescimento agregado por município ───────────────
-- Uma linha por município, atualizada a cada pipeline run
CREATE TABLE IF NOT EXISTS growth_signals (
    id                       SERIAL PRIMARY KEY,
    codigo_ibge              VARCHAR(7) UNIQUE NOT NULL,
    uf                       CHAR(2),
    -- Farmácias (CNAE 4771): mercado direto do cliente
    farmacias_2022           INTEGER,
    farmacias_2023           INTEGER,
    farmacias_2024           INTEGER,
    farmacias_delta_pct      FLOAT,            -- (2024 - 2022) / 2022
    -- Construção civil (CNAE 41xx): sinal de novos empreendimentos
    construcao_2022          INTEGER,
    construcao_2023          INTEGER,
    construcao_2024          INTEGER,
    construcao_delta_pct     FLOAT,
    -- Logística/galpões (CNAE 52xx): hubs industriais → massa salarial
    logistica_2022           INTEGER,
    logistica_2023           INTEGER,
    logistica_2024           INTEGER,
    logistica_delta_pct      FLOAT,
    -- Imobiliário (CNAE 68xx): incorporadoras → novos bairros
    imob_2022                INTEGER,
    imob_2023                INTEGER,
    imob_2024                INTEGER,
    imob_delta_pct           FLOAT,
    -- Score composto de crescimento (0-100)
    -- Ponderação: farmácias 40% + construção 25% + logística 20% + imob 15%
    growth_score             FLOAT,
    growth_tier              CHAR(1),           -- A/B/C/D (quartis nacionais)
    -- Classificação qualitativa do sinal
    growth_label             VARCHAR(30),       -- ACELERADO | CRESCENDO | ESTAVEL | DECLINIO
    -- Metadados
    anos_analisados          VARCHAR(20),       -- ex: "2022-2024"
    calculated_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_growth_ibge        ON growth_signals(codigo_ibge);
CREATE INDEX IF NOT EXISTS idx_growth_score       ON growth_signals(growth_score DESC);
CREATE INDEX IF NOT EXISTS idx_growth_tier        ON growth_signals(growth_tier);

-- ── View: municípios com score de crescimento + score atual (scout view) ──────
CREATE OR REPLACE VIEW scout_view AS
SELECT
    m.codigo_ibge,
    m.nome,
    m.uf,
    m.latitude,
    m.longitude,
    -- Score de mercado atual (Entrega 1)
    s.score_total          AS score_mercado_atual,
    s.tier                 AS tier_mercado,
    s.ranking_nacional,
    s.distance_campinas_km,
    -- Score de crescimento (Entrega 3)
    g.growth_score,
    g.growth_tier,
    g.growth_label,
    g.farmacias_delta_pct,
    g.construcao_delta_pct,
    g.logistica_delta_pct,
    g.imob_delta_pct,
    g.farmacias_2022,
    g.farmacias_2024,
    -- Score composto: oportunidade = mercado atual × crescimento
    -- Municípios com score_mercado médio mas crescimento alto = WHITE SPACE
    CASE
        WHEN s.score_total IS NOT NULL AND g.growth_score IS NOT NULL
        THEN ROUND(
            (s.score_total * 0.5 + g.growth_score * 0.5)::NUMERIC, 2
        )
        ELSE s.score_total
    END AS oportunidade_score,
    -- Flag de white space: crescimento alto + mercado ainda médio
    CASE
        WHEN g.growth_tier IN ('A','B') AND s.tier IN ('B','C') THEN TRUE
        ELSE FALSE
    END AS is_white_space
FROM municipios m
LEFT JOIN scores       s ON s.codigo_ibge = m.codigo_ibge
LEFT JOIN growth_signals g ON g.codigo_ibge = m.codigo_ibge;

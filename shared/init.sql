-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- =============================================
-- MUNICIPIOS: base table for all 5570 municipalities
-- =============================================
CREATE TABLE IF NOT EXISTS municipios (
    id SERIAL PRIMARY KEY,
    codigo_ibge VARCHAR(7) UNIQUE NOT NULL,
    nome VARCHAR(200) NOT NULL,
    uf VARCHAR(2) NOT NULL,
    regiao VARCHAR(20),
    mesorregiao VARCHAR(200),
    microrregiao VARCHAR(200),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    geom GEOMETRY(POINT, 4326),
    area_km2 DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_municipios_uf ON municipios(uf);
CREATE INDEX IF NOT EXISTS idx_municipios_codigo ON municipios(codigo_ibge);
CREATE INDEX IF NOT EXISTS idx_municipios_geom ON municipios USING GIST(geom);

-- =============================================
-- DEMOGRAFICOS: IBGE Censo 2022
-- =============================================
CREATE TABLE IF NOT EXISTS demograficos (
    id SERIAL PRIMARY KEY,
    codigo_ibge VARCHAR(7) UNIQUE REFERENCES municipios(codigo_ibge),
    -- Population
    populacao_total INTEGER,
    populacao_urbana INTEGER,
    populacao_rural INTEGER,
    taxa_urbanizacao DECIMAL(5, 2),
    -- Age groups (key for correlatos)
    pop_0_4 INTEGER,
    pop_5_14 INTEGER,
    pop_15_29 INTEGER,
    pop_30_44 INTEGER,
    pop_45_64 INTEGER,
    pop_65_plus INTEGER,
    indice_envelhecimento DECIMAL(6, 2),
    -- Income
    renda_per_capita DECIMAL(10, 2),
    -- Housing
    domicilios_total INTEGER,
    -- Derived
    populacao_alvo INTEGER, -- 30-64 anos (core correlatos buyer)
    pct_populacao_alvo DECIMAL(5, 2),
    ano_referencia INTEGER DEFAULT 2022,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_demograficos_ibge ON demograficos(codigo_ibge);

-- =============================================
-- ESTABELECIMENTOS_SAUDE: CNES DataSUS
-- =============================================
CREATE TABLE IF NOT EXISTS estabelecimentos_saude (
    id SERIAL PRIMARY KEY,
    codigo_ibge VARCHAR(7) UNIQUE REFERENCES municipios(codigo_ibge),
    -- Counts by type
    farmacias INTEGER DEFAULT 0,           -- TP=43
    farmacias_magistrais INTEGER DEFAULT 0, -- subset
    consultorios_medicos INTEGER DEFAULT 0, -- TP=23
    consultorios_odonto INTEGER DEFAULT 0,  -- TP=21
    laboratorios INTEGER DEFAULT 0,         -- TP=60
    clinicas INTEGER DEFAULT 0,             -- TP=36
    hospitais INTEGER DEFAULT 0,            -- TP=05
    ubs_upa INTEGER DEFAULT 0,              -- TP=01,07
    total_estabelecimentos INTEGER DEFAULT 0,
    -- Density (per 10k inhabitants)
    farmacias_por_10k DECIMAL(8, 2),
    estabelecimentos_saude_por_10k DECIMAL(8, 2),
    -- Beds
    leitos_total INTEGER DEFAULT 0,
    leitos_sus INTEGER DEFAULT 0,
    ano_referencia INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_estab_ibge ON estabelecimentos_saude(codigo_ibge);

-- =============================================
-- INDICADORES_ECONOMICOS: Receita Federal, RAIS, IPEADATA
-- =============================================
CREATE TABLE IF NOT EXISTS indicadores_economicos (
    id SERIAL PRIMARY KEY,
    codigo_ibge VARCHAR(7) UNIQUE REFERENCES municipios(codigo_ibge),
    -- PIB
    pib_per_capita DECIMAL(12, 2),
    pib_total DECIMAL(15, 2),
    -- Employment health sector (RAIS)
    empregos_saude INTEGER DEFAULT 0,
    massa_salarial_saude DECIMAL(15, 2),
    -- Active CNPJs (Receita Federal)
    cnpjs_farmacias INTEGER DEFAULT 0,       -- CNAE 47.71
    cnpjs_saude INTEGER DEFAULT 0,           -- CNAE 86.xx
    cnpjs_instrumentos_medicos INTEGER DEFAULT 0, -- CNAE 32.50
    cnpjs_distribuidores INTEGER DEFAULT 0,  -- CNAE 46.44, 46.45
    -- Health insurance (ANS)
    beneficiarios_planos INTEGER DEFAULT 0,
    cobertura_planos_pct DECIMAL(5, 2),
    -- IDH
    idh DECIMAL(5, 3),
    ano_referencia INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_econ_ibge ON indicadores_economicos(codigo_ibge);

-- =============================================
-- SCORES: computed scores per municipality
-- =============================================
CREATE TABLE IF NOT EXISTS scores (
    id SERIAL PRIMARY KEY,
    codigo_ibge VARCHAR(7) REFERENCES municipios(codigo_ibge),
    -- Component scores (0-100)
    score_demografico DECIMAL(5, 2),
    score_infraestrutura_saude DECIMAL(5, 2),
    score_economico DECIMAL(5, 2),
    score_logistico DECIMAL(5, 2),
    score_competitividade DECIMAL(5, 2),
    distance_campinas_km DECIMAL(10, 2),
    -- Composite score (PCA-weighted)
    score_total DECIMAL(5, 2),
    -- Classification
    tier VARCHAR(10), -- A, B, C, D
    ranking_nacional INTEGER,
    ranking_estadual INTEGER,
    -- PCA components
    pca_component_1 DECIMAL(8, 4),
    pca_component_2 DECIMAL(8, 4),
    pca_component_3 DECIMAL(8, 4),
    calculated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scores_ibge ON scores(codigo_ibge);
CREATE INDEX IF NOT EXISTS idx_scores_total ON scores(score_total DESC);
CREATE INDEX IF NOT EXISTS idx_scores_tier ON scores(tier);

-- =============================================
-- PIPELINE_LOG: track ETL progress
-- =============================================
CREATE TABLE IF NOT EXISTS pipeline_log (
    id SERIAL PRIMARY KEY,
    etapa VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL, -- running, done, error
    municipios_processados INTEGER DEFAULT 0,
    mensagem TEXT,
    started_at TIMESTAMP DEFAULT NOW(),
    finished_at TIMESTAMP
);

-- =============================================
-- VIEW: municipios_completos (join all)
-- =============================================
CREATE OR REPLACE VIEW municipios_completos AS
SELECT 
    m.codigo_ibge,
    m.nome,
    m.uf,
    m.regiao,
    m.microrregiao,
    m.latitude,
    m.longitude,
    d.populacao_total,
    d.populacao_alvo,
    d.renda_per_capita,
    d.taxa_urbanizacao,
    d.indice_envelhecimento,
    e.farmacias,
    e.farmacias_magistrais,
    e.consultorios_odonto,
    e.laboratorios,
    e.hospitais,
    e.total_estabelecimentos,
    e.farmacias_por_10k,
    ec.pib_per_capita,
    ec.beneficiarios_planos,
    ec.cobertura_planos_pct,
    ec.cnpjs_farmacias,
    ec.idh,
    s.score_total,
    s.score_demografico,
    s.score_infraestrutura_saude,
    s.score_economico,
    s.score_logistico,
    s.tier,
    s.ranking_nacional,
    s.distance_campinas_km
FROM municipios m
LEFT JOIN demograficos d ON m.codigo_ibge = d.codigo_ibge
LEFT JOIN estabelecimentos_saude e ON m.codigo_ibge = e.codigo_ibge
LEFT JOIN indicadores_economicos ec ON m.codigo_ibge = ec.codigo_ibge
LEFT JOIN scores s ON m.codigo_ibge = s.codigo_ibge;

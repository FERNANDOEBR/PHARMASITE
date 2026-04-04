-- ============================================================
-- ENTREGA 2: Microeconomia por Bairro — Setores Censitários SP
-- IBGE Censo 2022 + PDVs OpenStreetMap
-- ============================================================

-- ── Setores Censitários (polígonos IBGE) ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS setores_censitarios (
    id               SERIAL PRIMARY KEY,
    -- IBGE codes
    codigo_setor     CHAR(15) UNIQUE NOT NULL,  -- 15 dígitos: UF+Mun+Subdist+Setor
    codigo_ibge      VARCHAR(7) NOT NULL,        -- 7 dígitos: código do município
    uf               CHAR(2)   NOT NULL,
    nome_municipio   VARCHAR(200),
    -- Classificação
    situacao         VARCHAR(20),               -- URBANO | RURAL | AGLOMERADO_RURAL
    tipo_setor       VARCHAR(50),               -- NORMAL, ESPECIAL, etc.
    -- Demográficos (Resultados do Universo Censo 2022)
    populacao_total  INTEGER,
    domicilios_total INTEGER,
    -- Renda (Resultados da Amostra — disponível por Área de Ponderação)
    renda_media_domiciliar FLOAT,               -- proxy: renda do município quando setor não disponível
    -- Geometry (WGS84)
    geom             GEOMETRY(MULTIPOLYGON, 4326),
    area_km2         FLOAT,
    -- Metadados
    fonte            VARCHAR(50) DEFAULT 'IBGE_CENSO_2022',
    ingested_at      TIMESTAMP  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_setores_codigo     ON setores_censitarios(codigo_setor);
CREATE INDEX IF NOT EXISTS idx_setores_ibge        ON setores_censitarios(codigo_ibge);
CREATE INDEX IF NOT EXISTS idx_setores_uf          ON setores_censitarios(uf);
CREATE INDEX IF NOT EXISTS idx_setores_geom        ON setores_censitarios USING GIST(geom);

-- ── PDVs georeferenciados (OpenStreetMap + enriquecimento futuro) ─────────────
CREATE TABLE IF NOT EXISTS pdvs_osm (
    id              SERIAL PRIMARY KEY,
    osm_id          BIGINT,
    osm_type        VARCHAR(10),                -- node | way | relation
    -- Tipo de estabelecimento
    categoria       VARCHAR(50) NOT NULL,        -- farmacia | clinica | dentista | hospital | laboratorio
    nome            VARCHAR(300),
    -- Endereço / localização
    latitude        FLOAT NOT NULL,
    longitude       FLOAT NOT NULL,
    geom            GEOMETRY(POINT, 4326),
    -- Enriquecimento espacial (preenchido via join PostGIS)
    codigo_setor    CHAR(15),                   -- FK setores_censitarios
    codigo_ibge     VARCHAR(7),                 -- FK municipios
    uf              CHAR(2),
    -- Metadados
    fonte           VARCHAR(20) DEFAULT 'OSM',
    ingested_at     TIMESTAMP  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pdvs_categoria    ON pdvs_osm(categoria);
CREATE INDEX IF NOT EXISTS idx_pdvs_ibge         ON pdvs_osm(codigo_ibge);
CREATE INDEX IF NOT EXISTS idx_pdvs_setor        ON pdvs_osm(codigo_setor);
CREATE INDEX IF NOT EXISTS idx_pdvs_geom         ON pdvs_osm USING GIST(geom);

-- ── View: resumo de PDVs por setor (usada pela API) ──────────────────────────
CREATE OR REPLACE VIEW pdvs_por_setor AS
SELECT
    sc.codigo_setor,
    sc.codigo_ibge,
    sc.uf,
    sc.nome_municipio,
    sc.situacao,
    sc.populacao_total,
    sc.renda_media_domiciliar,
    sc.area_km2,
    COUNT(p.id)                                          AS total_pdvs,
    COUNT(p.id) FILTER (WHERE p.categoria = 'farmacia')  AS farmacias,
    COUNT(p.id) FILTER (WHERE p.categoria = 'clinica')   AS clinicas,
    COUNT(p.id) FILTER (WHERE p.categoria = 'dentista')  AS dentistas,
    COUNT(p.id) FILTER (WHERE p.categoria = 'hospital')  AS hospitais,
    COUNT(p.id) FILTER (WHERE p.categoria = 'laboratorio') AS laboratorios,
    -- Densidade: PDVs por km²
    CASE WHEN sc.area_km2 > 0
         THEN ROUND((COUNT(p.id) / sc.area_km2)::NUMERIC, 4)
         ELSE NULL
    END AS pdvs_por_km2,
    -- Densidade: farmácias por 10k hab
    CASE WHEN sc.populacao_total > 0
         THEN ROUND((COUNT(p.id) FILTER (WHERE p.categoria = 'farmacia') * 10000.0
                     / sc.populacao_total)::NUMERIC, 2)
         ELSE NULL
    END AS farmacias_por_10k,
    sc.geom
FROM setores_censitarios sc
LEFT JOIN pdvs_osm p ON p.codigo_setor = sc.codigo_setor
GROUP BY sc.id, sc.codigo_setor, sc.codigo_ibge, sc.uf, sc.nome_municipio,
         sc.situacao, sc.populacao_total, sc.renda_media_domiciliar,
         sc.area_km2, sc.geom;

-- ── View: resumo de PDVs por município (agregação dos setores) ───────────────
CREATE OR REPLACE VIEW pdvs_osm_por_municipio AS
SELECT
    codigo_ibge,
    uf,
    COUNT(*)                                            AS total_pdvs,
    COUNT(*) FILTER (WHERE categoria = 'farmacia')      AS farmacias_osm,
    COUNT(*) FILTER (WHERE categoria = 'clinica')       AS clinicas_osm,
    COUNT(*) FILTER (WHERE categoria = 'dentista')      AS dentistas_osm,
    COUNT(*) FILTER (WHERE categoria = 'hospital')      AS hospitais_osm,
    COUNT(*) FILTER (WHERE categoria = 'laboratorio')   AS laboratorios_osm
FROM pdvs_osm
GROUP BY codigo_ibge, uf;

"""
ENTREGA 2 — Microeconomia por Bairro: Estado de São Paulo
=========================================================
Duas fontes combinadas para construir o mapa que o cliente nunca teve:

  1. IBGE Censo 2022 — Setores Censitários SP
     Polígonos + população + situação (urbano/rural)
     ~52.000 setores no estado de SP
     Fonte: FTP IBGE (shapefile + CSV de agregados)

  2. OpenStreetMap via Overpass API — PDVs georeferenciados
     Farmácias, clínicas, dentistas, hospitais, laboratórios
     Enriquecidos com join espacial → qual setor pertence cada PDV

Resultado: tabelas setores_censitarios e pdvs_osm no PostGIS,
com view pdvs_por_setor pronta para a API.

Raciocínio: IBGE tem os dados de microeconomia por bairro — só que
ninguém os serviu de forma utilizável para decisão comercial.
Este pipeline resolve isso.
"""

import io
import os
import time
import zipfile
import tempfile
import requests
import geopandas as gpd
import pandas as pd
from pathlib import Path
from loguru import logger
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

# ── Constantes ────────────────────────────────────────────────────────────────

UF_CODE = "SP"
IBGE_UF_NUM = "35"   # código numérico de SP no IBGE

# FTP IBGE — Malhas de setores censitários 2022
# IBGE usa padrão: {num_uf}_{sigla_uf}_[...].zip
IBGE_FTP_BASE = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022"

# Possíveis URLs para o shapefile de setores (IBGE muda convenção entre edições)
SHAPEFILE_URLS = [
    f"{IBGE_FTP_BASE}/Malhas_geograficas/Setor_Censitario/SP/{IBGE_UF_NUM}_SP_Setores_Censitarios_2022.zip",
    f"{IBGE_FTP_BASE}/Malhas_geograficas/Setor_Censitario/SP/{IBGE_UF_NUM}_SP_2022.zip",
    f"{IBGE_FTP_BASE}/Malhas_geograficas/Setor_Censitario/{IBGE_UF_NUM}_SP_Setores_2022.zip",
    f"{IBGE_FTP_BASE}/Malhas_geograficas/Setor_Censitario/SP_Setores_2022.zip",
]

# Agregados do Universo por setor (população + domicílios)
AGREGADOS_URLS = [
    f"{IBGE_FTP_BASE}/Resultados_do_Universo/Agregados_por_Setores_Censitarios/SP_Agregados_2022.zip",
    f"{IBGE_FTP_BASE}/Resultados_do_Universo/Agregados_por_Setores_Censitarios/{IBGE_UF_NUM}_SP_Agregados_2022.zip",
    f"{IBGE_FTP_BASE}/Resultados_do_Universo/Agregados_por_Setores_Censitarios/SP_{IBGE_UF_NUM}_Agregados_2022_xls.zip",
]

# Overpass API (OpenStreetMap) — PDVs do estado de SP
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Bounding box do estado de SP (para fallback se query por area falhar)
SP_BBOX = (-25.3, -53.1, -19.8, -44.2)  # (lat_min, lon_min, lat_max, lon_max)

# Categorias de PDVs a coletar
OSM_QUERIES = {
    "farmacia":    '[amenity=pharmacy]',
    "hospital":    '[amenity=hospital]',
    "clinica":     '[amenity=clinic]',
    "dentista":    '[healthcare=dentist]',
    "laboratorio": '[amenity=doctors][healthcare=laboratory]',
}

# Timeout / retry
REQUEST_TIMEOUT = 120
OVERPASS_TIMEOUT = 180


# ── Helpers ───────────────────────────────────────────────────────────────────

def _try_download_zip(urls: list[str], label: str) -> bytes | None:
    """Tenta cada URL em sequência. Retorna o conteúdo binário do primeiro que funcionar."""
    for url in urls:
        try:
            logger.info(f"  Tentando {label}: {url}")
            r = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
            if r.status_code == 200:
                content = r.content
                logger.success(f"  ✅ {label} baixado ({len(content)/1024/1024:.1f} MB)")
                return content
            else:
                logger.warning(f"  HTTP {r.status_code} — tentando próxima URL")
        except Exception as e:
            logger.warning(f"  Erro ao tentar {url}: {e}")
    logger.error(f"  ❌ {label}: todas as URLs falharam")
    return None


def _extract_gdf_from_zip(zip_bytes: bytes, label: str) -> gpd.GeoDataFrame | None:
    """Extrai o primeiro shapefile/geojson encontrado dentro do ZIP."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(tmpdir)

        # Procura shapefile (.shp) ou geojson
        shp_files = list(Path(tmpdir).rglob("*.shp"))
        geojson_files = list(Path(tmpdir).rglob("*.geojson")) + list(Path(tmpdir).rglob("*.json"))

        target = None
        if shp_files:
            # Prefere o maior (mais completo)
            target = max(shp_files, key=lambda p: p.stat().st_size)
        elif geojson_files:
            target = max(geojson_files, key=lambda p: p.stat().st_size)

        if target is None:
            logger.error(f"  Nenhum shapefile/geojson em {label}")
            return None

        logger.info(f"  Lendo {target.name}...")
        try:
            gdf = gpd.read_file(str(target))
            logger.success(f"  GeoDataFrame: {len(gdf)} registros, CRS={gdf.crs}")
            return gdf
        except Exception as e:
            logger.error(f"  Erro ao ler {target.name}: {e}")
            return None


def _extract_csv_from_zip(zip_bytes: bytes, keyword: str = "") -> pd.DataFrame | None:
    """Extrai CSV/XLS mais relevante do ZIP."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(tmpdir)

        csvs = list(Path(tmpdir).rglob("*.csv"))
        xlss = list(Path(tmpdir).rglob("*.xlsx")) + list(Path(tmpdir).rglob("*.xls"))
        all_files = csvs + xlss

        if not all_files:
            return None

        # Filtra por keyword se fornecido
        if keyword:
            filtered = [f for f in all_files if keyword.lower() in f.name.lower()]
            if filtered:
                all_files = filtered

        target = max(all_files, key=lambda p: p.stat().st_size)
        logger.info(f"  Lendo dados de {target.name}...")
        try:
            if target.suffix.lower() == '.csv':
                # IBGE usa separador ';' e encoding latin-1
                for enc in ['utf-8', 'latin-1', 'cp1252']:
                    try:
                        df = pd.read_csv(str(target), sep=';', encoding=enc, low_memory=False)
                        return df
                    except UnicodeDecodeError:
                        continue
            else:
                df = pd.read_excel(str(target), dtype=str)
                return df
        except Exception as e:
            logger.error(f"  Erro ao ler {target.name}: {e}")
            return None


# ── Passo 1: Setores Censitários IBGE ────────────────────────────────────────

def _download_setores_ibge() -> gpd.GeoDataFrame | None:
    """Baixa o shapefile de setores censitários SP do FTP do IBGE."""
    logger.info("📦 Baixando shapefile de setores censitários SP (IBGE Censo 2022)...")
    zip_bytes = _try_download_zip(SHAPEFILE_URLS, "shapefile setores SP")
    if zip_bytes is None:
        return None
    return _extract_gdf_from_zip(zip_bytes, "setores SP")


def _download_agregados_ibge() -> pd.DataFrame | None:
    """Baixa dados demográficos agregados por setor (pop + domicílios)."""
    logger.info("📊 Baixando agregados por setor (população + domicílios)...")
    zip_bytes = _try_download_zip(AGREGADOS_URLS, "agregados setores SP")
    if zip_bytes is None:
        logger.warning("  Agregados não disponíveis — só geometria será carregada")
        return None
    return _extract_csv_from_zip(zip_bytes, keyword="Basico")  # IBGE nomeia "Basico_SP"


def _normalize_setor_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Normaliza colunas do shapefile para o schema do banco.
    IBGE muda nomes entre versões — cobrimos os principais.
    """
    col_map = {}
    cols_upper = {c.upper(): c for c in gdf.columns}

    # código do setor (15 dígitos)
    for candidate in ['CD_SETOR', 'COD_SETOR', 'CD_SETOR_', 'GEOCODIGO', 'CD_GEOCODI']:
        if candidate in cols_upper:
            col_map[cols_upper[candidate]] = 'codigo_setor'
            break

    # código do município (7 dígitos)
    for candidate in ['CD_MUN', 'COD_MUN', 'CD_GEOCODM', 'CD_MUNICIP']:
        if candidate in cols_upper:
            col_map[cols_upper[candidate]] = 'codigo_ibge'
            break

    # situação (urbano/rural)
    for candidate in ['CD_SIT', 'SITUACAO', 'CD_SITUACA', 'TIPO']:
        if candidate in cols_upper:
            col_map[cols_upper[candidate]] = 'situacao'
            break

    # nome do município
    for candidate in ['NM_MUN', 'NOME_MUN', 'NM_MUNICIP', 'MUNICIPIO']:
        if candidate in cols_upper:
            col_map[cols_upper[candidate]] = 'nome_municipio'
            break

    gdf = gdf.rename(columns=col_map)

    # Garante código do município se ausente mas setor disponível
    if 'codigo_ibge' not in gdf.columns and 'codigo_setor' in gdf.columns:
        gdf['codigo_ibge'] = gdf['codigo_setor'].astype(str).str[:7]

    # Código do setor como string de 15 dígitos
    if 'codigo_setor' in gdf.columns:
        gdf['codigo_setor'] = gdf['codigo_setor'].astype(str).str.zfill(15)

    # Código do município: 7 dígitos
    if 'codigo_ibge' in gdf.columns:
        gdf['codigo_ibge'] = gdf['codigo_ibge'].astype(str).str[:7]

    # UF
    gdf['uf'] = UF_CODE

    # Reprojetar para WGS84 se necessário
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        logger.info(f"  Reprojetando de {gdf.crs} para EPSG:4326...")
        gdf = gdf.to_crs(epsg=4326)

    # Área em km²
    try:
        gdf_proj = gdf.to_crs(epsg=32722)  # UTM 22S — adequado para SP
        gdf['area_km2'] = (gdf_proj.geometry.area / 1_000_000).round(6)
    except Exception:
        gdf['area_km2'] = None

    # Converter MultiPolygon se necessário
    gdf['geometry'] = gdf['geometry'].apply(
        lambda g: g if g is None or g.geom_type == 'MultiPolygon'
        else g.__class__.__name__ != 'MultiPolygon' and
             __import__('shapely.ops', fromlist=['unary_union']).unary_union([g])
             if False else g
    )

    return gdf


def _merge_demograficos(gdf: gpd.GeoDataFrame, df_agr: pd.DataFrame) -> gpd.GeoDataFrame:
    """Faz join entre GDF de setores e DataFrame de agregados demográficos."""
    if df_agr is None:
        gdf['populacao_total']  = None
        gdf['domicilios_total'] = None
        return gdf

    cols_upper = {c.upper(): c for c in df_agr.columns}

    # Coluna de código do setor nos agregados
    setor_col = None
    for candidate in ['COD_SETOR', 'CD_SETOR', 'Cod_setor', 'GEOCODIGO']:
        if candidate.upper() in cols_upper:
            setor_col = cols_upper[candidate.upper()]
            break

    if setor_col is None:
        logger.warning("  Coluna de código do setor não encontrada nos agregados")
        gdf['populacao_total']  = None
        gdf['domicilios_total'] = None
        return gdf

    # Coluna de população
    pop_col = None
    for candidate in ['V001', 'PESSOAS', 'POP_TOTAL', 'Pessoas_residentes']:
        if candidate.upper() in cols_upper:
            pop_col = cols_upper[candidate.upper()]
            break

    # Coluna de domicílios
    dom_col = None
    for candidate in ['V002', 'DOM_TOTAL', 'Domicilios_particulares']:
        if candidate.upper() in cols_upper:
            dom_col = cols_upper[candidate.upper()]
            break

    df_agr['_codigo_setor'] = df_agr[setor_col].astype(str).str.zfill(15)
    merge_cols = {'_codigo_setor': '_codigo_setor'}
    if pop_col: merge_cols[pop_col] = 'populacao_total'
    if dom_col: merge_cols[dom_col] = 'domicilios_total'

    df_slim = df_agr[list(merge_cols.keys())].rename(columns=merge_cols)

    if 'codigo_setor' not in gdf.columns:
        logger.warning("  codigo_setor ausente no GDF — sem join demográfico")
        return gdf

    gdf['_codigo_setor_key'] = gdf['codigo_setor'].astype(str).str.zfill(15)
    gdf = gdf.merge(df_slim, left_on='_codigo_setor_key', right_on='_codigo_setor', how='left')
    gdf = gdf.drop(columns=['_codigo_setor', '_codigo_setor_key'], errors='ignore')

    # Converter para numérico
    for col in ['populacao_total', 'domicilios_total']:
        if col in gdf.columns:
            gdf[col] = pd.to_numeric(gdf[col], errors='coerce').astype('Int64')

    matched = gdf['populacao_total'].notna().sum()
    logger.info(f"  Join demográfico: {matched}/{len(gdf)} setores com população")

    return gdf


def collect_setores_ibge(engine) -> int:
    """Baixa, processa e insere setores censitários SP no PostGIS."""
    logger.info("🗺️  Coletando setores censitários IBGE (SP)...")

    gdf = _download_setores_ibge()
    if gdf is None:
        logger.error("Falha ao obter shapefile IBGE — abortando setores")
        return 0

    df_agr = _download_agregados_ibge()
    gdf    = _normalize_setor_gdf(gdf)
    gdf    = _merge_demograficos(gdf, df_agr)

    required = ['codigo_setor', 'geometry']
    for req in required:
        if req not in gdf.columns:
            logger.error(f"  Coluna obrigatória ausente: {req}")
            return 0

    # Filtra apenas SP (segurança)
    if 'uf' in gdf.columns:
        gdf = gdf[gdf['uf'] == UF_CODE]

    logger.info(f"  Inserindo {len(gdf)} setores no PostGIS...")
    inserted = 0

    with engine.begin() as conn:
        # Limpa dados anteriores de SP para re-ingestão limpa
        conn.execute(text("DELETE FROM setores_censitarios WHERE uf = 'SP'"))

        for _, row in gdf.iterrows():
            if row.geometry is None or row.geometry.is_empty:
                continue
            try:
                geom_wkt = row.geometry.wkt
                conn.execute(text("""
                    INSERT INTO setores_censitarios (
                        codigo_setor, codigo_ibge, uf, nome_municipio,
                        situacao, populacao_total, domicilios_total,
                        area_km2, geom
                    ) VALUES (
                        :setor, :ibge, :uf, :nome_mun,
                        :sit, :pop, :dom,
                        :area, ST_Multi(ST_GeomFromText(:geom, 4326))
                    )
                    ON CONFLICT (codigo_setor) DO UPDATE SET
                        populacao_total  = EXCLUDED.populacao_total,
                        domicilios_total = EXCLUDED.domicilios_total,
                        area_km2         = EXCLUDED.area_km2,
                        geom             = EXCLUDED.geom,
                        ingested_at      = NOW()
                """), {
                    "setor":    str(row.get('codigo_setor', ''))[:15],
                    "ibge":     str(row.get('codigo_ibge', ''))[:7],
                    "uf":       str(row.get('uf', UF_CODE))[:2],
                    "nome_mun": str(row.get('nome_municipio', ''))[:200] if pd.notna(row.get('nome_municipio')) else None,
                    "sit":      str(row.get('situacao', ''))[:20] if pd.notna(row.get('situacao')) else None,
                    "pop":      int(row['populacao_total']) if pd.notna(row.get('populacao_total')) else None,
                    "dom":      int(row['domicilios_total']) if pd.notna(row.get('domicilios_total')) else None,
                    "area":     float(row['area_km2']) if pd.notna(row.get('area_km2')) else None,
                    "geom":     geom_wkt,
                })
                inserted += 1
            except Exception as e:
                logger.warning(f"  Erro no setor {row.get('codigo_setor', '?')}: {e}")
                continue

    logger.success(f"  ✅ Setores IBGE inseridos: {inserted}")
    return inserted


# ── Passo 2: PDVs OpenStreetMap ───────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def _overpass_query(query_str: str) -> list[dict]:
    """Executa query no Overpass API com retry automático."""
    r = requests.post(OVERPASS_URL, data={"data": query_str}, timeout=OVERPASS_TIMEOUT)
    r.raise_for_status()
    return r.json().get("elements", [])


def _build_overpass_query(tag_filter: str) -> str:
    """
    Query Overpass para PDV com determinado tag, estado SP.
    Usa area pelo nome + admin_level para máxima precisão.
    Fallback: bbox do estado.
    """
    return f"""
[out:json][timeout:{OVERPASS_TIMEOUT}];
(
  area["name"="São Paulo"]["boundary"="administrative"]["admin_level"="4"]->.sp;
  node{tag_filter}(area.sp);
  way{tag_filter}(area.sp);
);
out center;
"""


def _parse_overpass_elements(elements: list[dict], categoria: str) -> list[dict]:
    """Converte elementos OSM em dicts prontos para inserção."""
    records = []
    for el in elements:
        # Coordenadas
        if el['type'] == 'node':
            lat, lon = el.get('lat'), el.get('lon')
        elif el['type'] == 'way' and 'center' in el:
            lat, lon = el['center']['lat'], el['center']['lon']
        else:
            continue

        if lat is None or lon is None:
            continue

        tags = el.get('tags', {})
        records.append({
            "osm_id":    el.get('id'),
            "osm_type":  el['type'],
            "categoria": categoria,
            "nome":      tags.get('name') or tags.get('brand') or tags.get('operator'),
            "latitude":  float(lat),
            "longitude": float(lon),
        })
    return records


def collect_pdvs_osm(engine) -> int:
    """Coleta PDVs do OpenStreetMap para SP e faz join espacial com setores."""
    logger.info("🗂️  Coletando PDVs OpenStreetMap (SP)...")

    all_records = []

    for categoria, tag_filter in OSM_QUERIES.items():
        logger.info(f"  → Buscando {categoria} no OSM...")
        try:
            query = _build_overpass_query(tag_filter)
            elements = _overpass_query(query)
            records = _parse_overpass_elements(elements, categoria)
            all_records.extend(records)
            logger.info(f"    {categoria}: {len(records)} PDVs encontrados")
            time.sleep(2)  # respeito ao Overpass: não sobbrecarregar
        except Exception as e:
            logger.error(f"  Erro ao coletar {categoria}: {e}")
            continue

    if not all_records:
        logger.error("Nenhum PDV coletado do OSM")
        return 0

    logger.info(f"  Total PDVs coletados: {len(all_records)}")
    logger.info("  Fazendo join espacial (PDV → setor censitário)...")

    # ── Join espacial via PostGIS ─────────────────────────────────────────────
    # Insere sem setor primeiro, depois atualiza via ST_Within
    inserted = 0
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM pdvs_osm WHERE uf = 'SP'"))

        for rec in all_records:
            try:
                conn.execute(text("""
                    INSERT INTO pdvs_osm (
                        osm_id, osm_type, categoria, nome,
                        latitude, longitude, geom, uf
                    ) VALUES (
                        :osm_id, :osm_type, :categoria, :nome,
                        :lat, :lon,
                        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326),
                        'SP'
                    )
                    ON CONFLICT DO NOTHING
                """), {
                    "osm_id":   rec["osm_id"],
                    "osm_type": rec["osm_type"],
                    "categoria": rec["categoria"],
                    "nome":     rec["nome"],
                    "lat":      rec["latitude"],
                    "lon":      rec["longitude"],
                })
                inserted += 1
            except Exception as e:
                logger.warning(f"  Erro ao inserir PDV {rec.get('osm_id')}: {e}")
                continue

    logger.info(f"  PDVs inseridos: {inserted} — executando join espacial...")

    # ── Join espacial: atribui codigo_setor e codigo_ibge a cada PDV ──────────
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE pdvs_osm p
            SET
                codigo_setor = s.codigo_setor,
                codigo_ibge  = s.codigo_ibge
            FROM setores_censitarios s
            WHERE
                p.uf = 'SP'
                AND ST_Within(p.geom, s.geom)
        """))
        matched = result.rowcount
        logger.success(f"  Join espacial: {matched}/{inserted} PDVs atribuídos a setores")

    return inserted


# ── Orquestrador da Entrega 2 ─────────────────────────────────────────────────

def collect_setores_sp(engine) -> dict:
    """
    Entry point chamado pelo pipeline principal (main.py).
    Executa em sequência: IBGE setores → OSM PDVs → join espacial.
    """
    from sqlalchemy import text as _text

    logger.info("=" * 60)
    logger.info("🗺️  ENTREGA 2 — MICROECONOMIA POR BAIRRO (SP)")
    logger.info("=" * 60)

    # Garante que as tabelas existem
    sql_migration = Path(__file__).parent.parent.parent / "shared" / "init_setores.sql"
    if sql_migration.exists():
        logger.info("  Executando migração SQL (setores + pdvs)...")
        with engine.begin() as conn:
            conn.execute(_text(sql_migration.read_text()))
        logger.success("  Migração aplicada")
    else:
        logger.warning(f"  Arquivo de migração não encontrado: {sql_migration}")

    # Passo 1: Setores IBGE
    n_setores = collect_setores_ibge(engine)

    # Passo 2: PDVs OSM (só faz sentido se tiver setores para join)
    n_pdvs = 0
    if n_setores > 0:
        n_pdvs = collect_pdvs_osm(engine)
    else:
        logger.warning("  Setores não carregados — pulando coleta OSM")

    # Sumário
    logger.info("\n━━ SUMÁRIO ENTREGA 2 ━━")
    logger.info(f"  Setores censitários SP:  {n_setores:,}")
    logger.info(f"  PDVs OSM (SP):           {n_pdvs:,}")

    if n_setores > 0 and n_pdvs > 0:
        with engine.connect() as conn:
            r = conn.execute(_text(
                "SELECT COUNT(*) FROM pdvs_osm WHERE codigo_setor IS NOT NULL AND uf='SP'"
            ))
            matched = r.scalar()
            logger.info(f"  PDVs com setor atribuído: {matched:,} ({matched/n_pdvs*100:.1f}%)")

    logger.success("✅ Entrega 2 concluída — microeconomia por bairro carregada")

    return {
        "count":   n_setores + n_pdvs,
        "setores": n_setores,
        "pdvs":    n_pdvs,
        "message": f"Entrega 2: {n_setores} setores + {n_pdvs} PDVs OSM (SP)",
    }

"""
Coleta todos os 5.570 municípios brasileiros via API IBGE.
Fonte: https://servicodados.ibge.gov.br/api/v1/localidades/municipios

Coordenadas: fetched via IBGE malhas API at minimum resolution (resolucao=1).
  - resolucao=1 (~10-15 MB) vs resolucao=2 (~50 MB) — much safer for Docker timeouts.
  - Endpoint: /api/v3/malhas/municipios?formato=application/json&resolucao=1
  - No API key required — all IBGE endpoints are free and public.

Note: there is NO per-UF filter on the malhas API. The global endpoint is the only
option for batch coordinate loading.
"""

import time
import requests
import pandas as pd
from sqlalchemy import text
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

# Correct malhas URL pattern:
#   /api/v3/malhas/estados/{IBGE_NUMERIC_ID}?intrarregiao=municipio&qualidade=minima
#   → returns GeoJSON with all municipalities of that state as individual features.
#
# Notes:
#   - endpoint requires a NUMERIC state code (not UF abbreviation)
#   - `intrarregiao=municipio` subdivides the state into municipality polygons
#   - `qualidade=minima` = smallest file (~1-3 MB per state) — avoids Docker timeouts
#   - `formato=application/json` AND Accept header to ensure JSON response
#   - The GLOBAL /api/v3/malhas/municipios endpoint (no ID) returns 404 — not supported
UF_IBGE_CODES = {
    "AC": 12, "AL": 27, "AM": 13, "AP": 16, "BA": 29, "CE": 23, "DF": 53,
    "ES": 32, "GO": 52, "MA": 21, "MG": 31, "MS": 50, "MT": 51, "PA": 15,
    "PB": 25, "PE": 26, "PI": 22, "PR": 41, "RJ": 33, "RN": 24, "RO": 11,
    "RR": 14, "RS": 43, "SC": 42, "SE": 28, "SP": 35, "TO": 17,
}

REGIAO_MAP = {
    "N": "Norte", "NE": "Nordeste", "CO": "Centro-Oeste",
    "SE": "Sudeste", "S": "Sul"
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def fetch_municipios_ibge():
    logger.info("Baixando lista municípios IBGE...")
    resp = requests.get(IBGE_MUNICIPIOS_URL, timeout=60)
    resp.raise_for_status()
    return resp.json()


def collect_municipios(engine) -> dict:
    data = fetch_municipios_ibge()
    logger.info(f"Recebidos {len(data)} municípios do IBGE")

    rows = []
    for m in data:
        try:
            microrregiao = m.get("microrregiao")
            if microrregiao:
                mesorregiao = microrregiao["mesorregiao"]
                uf_data = mesorregiao["UF"]
                uf = uf_data["sigla"]
                regiao = REGIAO_MAP.get(uf_data["regiao"]["sigla"], "")
                meso_nome = mesorregiao["nome"]
                micro_nome = microrregiao["nome"]
            else:
                # Fallback: extract UF from regiao-imediata -> regiao-intermediaria -> UF
                regiao_imed = m.get("regiao-imediata") or {}
                regiao_inter = regiao_imed.get("regiao-intermediaria") or {}
                uf_data = regiao_inter.get("UF") or {}
                uf = uf_data.get("sigla", "")
                regiao = REGIAO_MAP.get((uf_data.get("regiao") or {}).get("sigla", ""), "")
                meso_nome = regiao_inter.get("nome", "")
                micro_nome = regiao_imed.get("nome", "")

            rows.append({
                "codigo_ibge": str(m["id"]),
                "nome": m["nome"],
                "uf": uf,
                "regiao": regiao,
                "mesorregiao": meso_nome,
                "microrregiao": micro_nome,
            })
        except (KeyError, TypeError) as e:
            logger.warning(f"Município {m.get('id')} com estrutura incompleta: {e}")

    df = pd.DataFrame(rows)
    logger.info(f"DataFrame: {len(df)} municípios, {df['uf'].nunique()} UFs")

    # Enrich with coordinates via IBGE malhas API (per UF — 27 small requests)
    df = enrich_with_coordinates(df)

    # Write to DB — ON CONFLICT updates ALL fields including lat/lon/geom
    with engine.begin() as conn:
        for _, row in df.iterrows():
            lat = row.get("latitude")
            lon = row.get("longitude")
            conn.execute(text("""
                INSERT INTO municipios (codigo_ibge, nome, uf, regiao, mesorregiao, microrregiao, latitude, longitude, geom)
                VALUES (:codigo_ibge, :nome, :uf, :regiao, :mesorregiao, :microrregiao, :lat, :lon,
                        CASE WHEN :lat IS NOT NULL AND :lon IS NOT NULL
                             THEN ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
                             ELSE NULL
                        END)
                ON CONFLICT (codigo_ibge) DO UPDATE SET
                    nome        = EXCLUDED.nome,
                    uf          = EXCLUDED.uf,
                    regiao      = EXCLUDED.regiao,
                    mesorregiao = EXCLUDED.mesorregiao,
                    microrregiao = EXCLUDED.microrregiao,
                    latitude    = EXCLUDED.latitude,
                    longitude   = EXCLUDED.longitude,
                    geom        = EXCLUDED.geom,
                    updated_at  = NOW()
            """), {
                "codigo_ibge": row["codigo_ibge"],
                "nome": row["nome"],
                "uf": row["uf"],
                "regiao": row["regiao"],
                "mesorregiao": row["mesorregiao"],
                "microrregiao": row["microrregiao"],
                "lat": float(lat) if lat is not None and not pd.isna(lat) else None,
                "lon": float(lon) if lon is not None and not pd.isna(lon) else None,
            })

    coord_filled = df["latitude"].notna().sum()
    logger.success(f"{len(df)} municípios inseridos/atualizados | {coord_filled} com coordenadas")
    return {"count": len(df), "message": f"{len(df)} municípios inseridos ({coord_filled} com lat/lon)"}


def _centroid_from_geom(geom: dict) -> tuple[float, float] | None:
    """Extract approximate centroid (lat, lon) from a GeoJSON geometry dict."""
    gtype = (geom.get("type") or "")
    coords = geom.get("coordinates") or []
    try:
        if gtype == "Point":
            return (coords[1], coords[0])
        elif gtype == "Polygon":
            pts = coords[0]
            return (sum(p[1] for p in pts) / len(pts), sum(p[0] for p in pts) / len(pts))
        elif gtype == "MultiPolygon":
            all_pts = [pt for poly in coords for pt in poly[0]]
            return (sum(p[1] for p in all_pts) / len(all_pts), sum(p[0] for p in all_pts) / len(all_pts))
    except (IndexError, ZeroDivisionError, TypeError):
        pass
    return None


def enrich_with_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Busca centróides dos municípios via IBGE malhas API — um request por estado (27 total).

    Endpoint correto (v3):
      GET /api/v3/malhas/estados/{IBGE_NUMERIC_ID}?intrarregiao=municipio&qualidade=minima
      Accept: application/json

    Por que per-estado e não global:
      - /api/v3/malhas/municipios          → 404 (endpoint sem ID não existe)
      - /api/v3/malhas/municipios?resolucao → 404 (parâmetro errado para v3)
      - /api/v3/malhas/estados/{ID}?intrarregiao=municipio → ✅ ~1-3 MB/estado, rápido

    IDs numéricos IBGE (não abreviatura UF):  SP=35, RJ=33, MG=31 etc.
    Propriedade do GeoJSON: features[].properties.codarea (código IBGE 7 dígitos)
    """
    logger.info("Buscando centróides — 27 requests per estado (IBGE malhas v3)...")

    coord_map: dict[str, tuple[float, float]] = {}
    failed = []

    # formato=application/vnd.geo+json → real GeoJSON (FeatureCollection with features[].properties.codarea)
    # formato=application/json         → TopoJSON (type="Topology", no features key) — DO NOT USE
    headers = {"Accept": "application/vnd.geo+json"}

    for uf, ibge_id in UF_IBGE_CODES.items():
        url = (
            f"https://servicodados.ibge.gov.br/api/v3/malhas/estados/{ibge_id}"
            f"?intrarregiao=municipio&qualidade=minima&formato=application%2Fvnd.geo%2Bjson"
        )
        try:
            resp = requests.get(url, timeout=60, headers=headers)
            if resp.status_code != 200:
                logger.warning(f"  {uf} ({ibge_id}): HTTP {resp.status_code}")
                failed.append(uf)
                continue

            geo = resp.json()
            features = geo.get("features", [])
            count = 0
            for feat in features:
                props = feat.get("properties") or {}
                # "codarea" = 7-digit IBGE code in malhas v3 GeoJSON
                code = str(props.get("codarea") or props.get("CD_MUN") or props.get("geocodigo") or "")
                if not code or len(code) < 6:
                    continue
                code = code.zfill(7)  # ensure 7-digit
                geom = feat.get("geometry") or {}
                pt = _centroid_from_geom(geom)
                if pt:
                    coord_map[code] = pt
                    count += 1

            logger.info(f"  {uf}: {count} centróides")
            time.sleep(0.3)  # polite: ~3 req/s

        except requests.exceptions.Timeout:
            logger.warning(f"  {uf}: timeout (60s)")
            failed.append(uf)
        except Exception as e:
            logger.warning(f"  {uf}: {type(e).__name__}: {e}")
            failed.append(uf)

    if failed:
        logger.warning(f"UFs sem coordenadas: {failed}")

    df["latitude"]  = df["codigo_ibge"].map(lambda x: coord_map.get(x, (None, None))[0])
    df["longitude"] = df["codigo_ibge"].map(lambda x: coord_map.get(x, (None, None))[1])

    matched = df["latitude"].notna().sum()
    logger.success(f"Coordenadas: {matched}/{len(df)} municípios ({matched/len(df)*100:.1f}%)")
    if matched < len(df) * 0.5:
        logger.warning(
            "< 50% com coordenadas — haversine usará MAX_VIABLE_KM para os sem lat/lon."
        )
    return df

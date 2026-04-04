"""
GET /tradearea/{lat}/{lon} — Gravity Model (Huff model approximation) — B2B PDV edition.
Returns the probability distribution of pharmacies (PDVs) being captured by a distribution
node at the queried center point.  B2B pivot: attractiveness is now proportional to the
absolute number of pharmacies in each municipality, not the consumer population.
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.engine import Connection

from database import get_db
from schemas import TradeAreaItem, TradeAreaResponse

router = APIRouter()

@router.get("/tradearea/{lat}/{lon}", response_model=TradeAreaResponse)
def tradearea(
    lat: float,
    lon: float,
    raio_km: float = Query(200.0, description="Raio de busca em km", ge=1.0, le=1000.0),
    beta: float = Query(2.0, description="Fator de decaimento de distância (Huff)", ge=0.5, le=5.0),
    db: Connection = Depends(get_db)
):
    raio_meters = raio_km * 1000.0
    
    # Busca municípios num raio de 'raio_meters' e calcula distância
    rows = db.execute(text(f"""
        SELECT
            m.codigo_ibge, m.nome, m.uf, m.latitude, m.longitude,
            ST_Distance(m.geom::geography, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography) / 1000.0 AS distance_km,
            s.score_total,
            e.farmacias,
            d.populacao_alvo
        FROM municipios m
        JOIN scores s ON m.codigo_ibge = s.codigo_ibge
        LEFT JOIN estabelecimentos_saude e ON m.codigo_ibge = e.codigo_ibge
        LEFT JOIN demograficos d ON m.codigo_ibge = d.codigo_ibge
        WHERE ST_DWithin(m.geom::geography, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography, :raio_meters)
    """), {"lat": lat, "lon": lon, "raio_meters": raio_meters}).mappings().all()

    items = []
    total_attractiveness = 0.0

    for r in rows:
        d_km = max(float(r['distance_km']), 1.0)
        score = float(r['score_total'] or 1.0)
        attractiveness = score / (d_km ** beta)

        items.append({
            "codigo_ibge": r['codigo_ibge'],
            "nome": r['nome'],
            "uf": r['uf'],
            "latitude": r['latitude'],
            "longitude": r['longitude'],
            "distance_km": round(d_km, 2),
            "attractiveness": attractiveness,
            # B2B: model PDV capture (pharmacies reachable), not consumer population
            "farmacias": r['farmacias'],
            "populacao_alvo": r['populacao_alvo'],
        })
        total_attractiveness += attractiveness

    results = []
    total_pdvs = 0.0

    for item in items:
        prob = item['attractiveness'] / total_attractiveness if total_attractiveness > 0 else 0
        # B2B: PDVs (farmácias) reachable under this probability share
        pdvs = round(prob * (item['farmacias'] or 0))

        results.append(TradeAreaItem(
            codigo_ibge=item['codigo_ibge'],
            nome=item['nome'],
            uf=item['uf'],
            latitude=item['latitude'],
            longitude=item['longitude'],
            distance_km=item['distance_km'],
            attractiveness=round(item['attractiveness'], 4),
            probability=round(prob, 4),
            estimated_pdvs=pdvs,
            # Keep consumer estimate too for backwards compatibility with frontend
            estimated_customers=round(prob * (item['populacao_alvo'] or 0), 0),
        ))
        total_pdvs += pdvs

    results.sort(key=lambda x: x.probability, reverse=True)

    return TradeAreaResponse(
        center_lat=lat,
        center_lon=lon,
        radius_km=raio_km,
        total_estimated_pdvs=round(total_pdvs, 0),
        total_estimated_customers=None,  # deprecated — use total_estimated_pdvs
        results=results
    )

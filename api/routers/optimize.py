"""
POST /optimize — filter and rank municipalities for best commercial fit.
Not cached: body parameters vary continuously, results are caller-specific.
"""
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.engine import Connection

from database import get_db
from schemas import OptimizeItem, OptimizeRequest, OptimizeResponse

router = APIRouter()


@router.post("/optimize", response_model=OptimizeResponse)
def optimize(body: OptimizeRequest, db: Connection = Depends(get_db)):
    filters = []
    params: dict = {"limit": body.limit}

    if body.min_score is not None:
        filters.append("s.score_total >= :min_score")
        params["min_score"] = body.min_score

    if body.min_populacao is not None:
        filters.append("d.populacao_total >= :min_pop")
        params["min_pop"] = body.min_populacao

    if body.ufs:
        # psycopg2 adapts a Python list to PostgreSQL ARRAY for ANY()
        filters.append("m.uf = ANY(:ufs)")
        params["ufs"] = body.ufs

    if body.tier:
        filters.append("s.tier = ANY(:tiers)")
        params["tiers"] = body.tier

    if body.max_distance_km is not None:
        filters.append("s.distance_campinas_km <= :max_dist")
        params["max_dist"] = body.max_distance_km

    where = "WHERE " + " AND ".join(filters) if filters else ""

    rows = db.execute(text(f"""
        SELECT
            m.codigo_ibge, m.nome, m.uf, m.latitude, m.longitude,
            s.score_total, s.tier, s.ranking_nacional, s.distance_campinas_km,
            d.populacao_total, d.renda_per_capita,
            e.farmacias, ec.cobertura_planos_pct, ec.idh,
            s.score_total AS fit_score
        FROM municipios m
        JOIN LATERAL (
            SELECT score_total, tier, ranking_nacional, distance_campinas_km
            FROM scores
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) s ON true
        LEFT JOIN LATERAL (
            SELECT populacao_total, renda_per_capita 
            FROM demograficos 
            WHERE codigo_ibge = m.codigo_ibge 
            ORDER BY id DESC LIMIT 1
        ) d ON true
        LEFT JOIN LATERAL (
            SELECT farmacias 
            FROM estabelecimentos_saude 
            WHERE codigo_ibge = m.codigo_ibge 
            ORDER BY id DESC LIMIT 1
        ) e ON true
        LEFT JOIN LATERAL (
            SELECT cobertura_planos_pct, idh 
            FROM indicadores_economicos 
            WHERE codigo_ibge = m.codigo_ibge 
            ORDER BY id DESC LIMIT 1
        ) ec ON true
        {where}
        ORDER BY s.score_total DESC NULLS LAST
        LIMIT :limit
    """), params).mappings().all()

    total = db.execute(text(f"""
        SELECT COUNT(*)
        FROM municipios m
        JOIN LATERAL (
            SELECT score_total, tier, distance_campinas_km
            FROM scores
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) s ON true
        LEFT JOIN LATERAL (
            SELECT populacao_total
            FROM demograficos
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) d ON true
        {where}
    """), params).scalar_one()

    results = [dict(r) for r in rows]

    if getattr(body, 'n_pontos', None) and len(results) > 0:
        import math

        def haversine(lat1, lon1, lat2, lon2):
            if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
                return 99999.0
            R = 6371.0
            lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
            c = 2 * math.asin(math.sqrt(a))
            return R * c

        n_pontos = min(body.n_pontos, len(results))
        centers = []
        
        for _ in range(n_pontos):
            best_candidate = None
            best_cost = float('inf')
            for candidate_idx, candidate in enumerate(results):
                if candidate_idx in centers:
                    continue
                
                current_cost = 0
                temp_centers = centers + [candidate_idx]
                for i, row in enumerate(results):
                    weight = float(row.get('fit_score') or 1.0)
                    min_dist = min([
                        haversine(row['latitude'], row['longitude'], 
                                  results[c]['latitude'], results[c]['longitude'])
                        for c in temp_centers
                    ])
                    current_cost += weight * min_dist
                
                if current_cost < best_cost:
                    best_cost = current_cost
                    best_candidate = candidate_idx
            
            if best_candidate is not None:
                centers.append(best_candidate)
                
        for i, row in enumerate(results):
            row['is_center'] = (i in centers)
            nearest_center = min(centers, key=lambda c: haversine(row['latitude'], row['longitude'], results[c]['latitude'], results[c]['longitude']))
            row['distance_to_center_km'] = haversine(row['latitude'], row['longitude'], results[nearest_center]['latitude'], results[nearest_center]['longitude'])
            row['cluster_center_codigo'] = results[nearest_center]['codigo_ibge']

    return OptimizeResponse(
        total_matching=total,
        criteria=body.model_dump(exclude_none=True),
        results=[OptimizeItem(**r) for r in results],
    )

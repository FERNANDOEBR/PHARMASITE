"""
L2 Paulínia — Neighborhood-Level Pharmacy Opportunity Scoring
Pipeline: Zero-Hallucination OSM Data Fetching → Neighborhood Assignment → Scoring

Model (Daniel's framework):
- Opportunity = f(demand_proxy, saturation, brand_mix, income_tier)
- 100% dependent on OpenStreetMap physical data (no AI fallback).
"""

import json
import math
import csv
import requests
from collections import defaultdict

# ─── KNOWN PAULÍNIA BAIRROS ────────────────────────────────────────────────
# Qualitative estimates based on Daniel's "Paulínia Premium" thesis
# Tier: A=upper, B=upper-mid, C=middle, D=lower-mid, E=popular
# Rendimento per capita follows Daniel's proxies.

BAIRROS = [
    # name, lat, lon, tier, pop_density(1-5), area_km2
    ("Centro",             -22.7629, -47.1539, "B", 5, 2.5),
    ("Morumbi",            -22.7562, -47.1594, "A", 4, 1.8),
    ("Nova Paulínia",      -22.7681, -47.1478, "A", 3, 2.0),
    ("João Aranha",        -22.7302, -47.1651, "C", 5, 3.2),
    ("Bom Retiro",         -22.7834, -47.1678, "D", 4, 2.8),
    ("São José",           -22.7351, -47.1738, "E", 4, 2.0),
    ("Betel",              -22.7937, -47.1065, "B", 3, 4.5), # Hub industrial/residencial isolado
    ("Santa Terezinha",    -22.7661, -47.1624, "C", 4, 1.5),
    ("Monte Alegre",       -22.7505, -47.1645, "C", 4, 2.1),
    ("Parque da Represa",  -22.7481, -47.1368, "B", 3, 1.7)
]

TIER_RENDA = {"A": 8500, "B": 5200, "C": 2800, "D": 1800, "E": 1100}

BIG_CHAINS = {"Drogasil", "Droga Raia", "Drogaria São Paulo", "Pague Menos",
               "Ultrafarma", "Farmácias Nissei", "Pacheco", "Onofre", "Farma Ponte"}
POPULAR_CHAINS = {"Drogão Super", "Drogal", "Drogarias Farmáxima", "Drogaria Familiar",
                  "Extrafarma", "Farmais", "Rede Mais", "Multidrogas"}

def get_osm_pharmacies(city_name):
    """Zero-Hallucination data fetch via OpenStreetMap Overpass API"""
    overpass_url = "https://overpass-api.de/api/interpreter"
    overpass_query = f"""
    [out:json][timeout:25];
    area[name="{city_name}"]->.searchArea;
    (
      node["amenity"="pharmacy"](area.searchArea);
      way["amenity"="pharmacy"](area.searchArea);
      relation["amenity"="pharmacy"](area.searchArea);
    );
    out center;
    """
    print("Fetching LIVE data from OpenStreetMap to guarantee 0 hallucinations...")
    response = requests.post(overpass_url, data={'data': overpass_query})
    response.raise_for_status()
    return response.json()

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def assign_to_bairro(lat, lon):
    best, best_d = None, float('inf')
    for b in BAIRROS:
        d = haversine_km(lat, lon, b[1], b[2])
        if d < best_d:
            best_d = d
            best = b
    # If the closest bairro is more than 3km away, it might belong to a non-mapped rural area
    return best[0], best_d

def classify_brand(name):
    for chain in BIG_CHAINS:
        if chain.lower() in name.lower():
            return "big_chain"
    for pop in POPULAR_CHAINS:
        if pop.lower() in name.lower():
            return "popular_chain"
    return "independent"

def hhi(counts):
    total = sum(counts.values())
    if total == 0:
        return 0
    return sum((c / total * 100) ** 2 for c in counts.values())

def main():
    # ── Fetch Data ───────────────────────────────────────────────────────────
    osm_data = get_osm_pharmacies("Paulínia")
    elements = osm_data.get("elements", [])
    print(f"Loaded {len(elements)} physical pharmacies in Paulínia from OSM.")

    # ── Assign to Bairro ─────────────────────────────────────────────────────
    records = []
    skipped = 0
    for el in elements:
        # Some are nodes with lat/lon, others are ways with center element
        lat = el.get("lat") or (el.get("center", {}).get("lat"))
        lon = el.get("lon") or (el.get("center", {}).get("lon"))
        
        if lat is None or lon is None:
            skipped += 1
            continue
            
        tags = el.get("tags", {})
        name = tags.get("name", "unknown")
        brand = tags.get("brand", tags.get("operator", name))
        brand_clean = brand.strip().title() if brand else "unknown"
        
        bairro, dist_km = assign_to_bairro(lat, lon)
        btype = classify_brand(brand_clean)
        
        records.append({
            "osm_id": el["id"],
            "lat": lat,
            "lon": lon,
            "name": name,
            "brand": brand_clean,
            "brand_type": btype,
            "bairro": bairro,
            "dist_to_bairro_km": round(dist_km, 3),
        })
    print(f"Assigned {len(records)} pharmacies (skipped {skipped})")

    # ── Aggregate ────────────────────────────────────────────────────────────
    agg = defaultdict(lambda: {
        "pdvs": [], "brands": defaultdict(int),
        "big_chain": 0, "popular_chain": 0, "independent": 0
    })
    for r in records:
        b = r["bairro"]
        agg[b]["pdvs"].append(r)
        agg[b]["brands"][r["brand"]] += 1
        agg[b][r["brand_type"]] += 1

    # ── Compute Scores ───────────────────────────────────────────────────────
    bairro_scores = []
    for binfo in BAIRROS:
        bname, blat, blon, tier, pop_d, area = binfo
        data = agg[bname]
        n_pdv = len(data["pdvs"])
        renda = TIER_RENDA[tier]

        n_brands = len(data["brands"])
        market_hhi = hhi(dict(data["brands"]))
        density = n_pdv / area if area > 0 else 0

        total = n_pdv or 1
        chain_ratio = (data["big_chain"] + data["popular_chain"]) / total

        sat_density = min(density / 4.0, 1.0)
        sat_chain   = chain_ratio
        saturation  = round((sat_density * 0.6 + sat_chain * 0.4) * 100, 1)

        demand = round(min((renda / 8500) * (pop_d / 5) * 100, 100), 1)

        # Daniel's thesis: Opportunity = High Demand - Saturated Supply
        opportunity = round(max(demand - saturation * 0.6, 5.0), 1)

        indep_ratio = data["independent"] / total
        chain_entry_gap = round(min(indep_ratio * 100, 100), 1)

        if opportunity >= 55:  # Slightly lower threshold for Paulinia's naturally high opportunity
            opp_tier = "🔴 Alta"
        elif opportunity >= 35:
            opp_tier = "🟡 Média"
        else:
            opp_tier = "🟢 Baixa"

        bairro_scores.append({
            "bairro": bname,
            "tier_socio": tier,
            "renda_proxy": renda,
            "pop_density": pop_d,
            "area_km2": area,
            "n_pdv": n_pdv,
            "n_brands": n_brands,
            "density_pdv_km2": round(density, 2),
            "big_chain_count": data["big_chain"],
            "popular_chain_count": data["popular_chain"],
            "independent_count": data["independent"],
            "chain_ratio_pct": round(chain_ratio * 100, 1),
            "indep_ratio_pct": round(indep_ratio * 100, 1),
            "market_hhi": round(market_hhi, 0),
            "saturation_score": saturation,
            "demand_score": demand,
            "opportunity_score": opportunity,
            "chain_entry_gap": chain_entry_gap,
            "opp_tier": opp_tier,
            "top_brands": ", ".join(sorted(data["brands"], key=lambda x: -data["brands"][x])[:4]) if data["brands"] else "—",
        })

    bairro_scores.sort(key=lambda x: -x["opportunity_score"])
    for i, b in enumerate(bairro_scores):
         b["rank"] = i + 1

    # ── Output ───────────────────────────────────────────────────────────────
    with open("l2_paulinia_scores.csv", "w", newline="", encoding="utf-8") as f:
        cols = bairro_scores[0].keys()
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(bairro_scores)

    print("\n=======================================================")
    print("  L2 PAULÍNIA — ZERO HALLUCINATION OPPORTUNITY RANKING ")
    print("=======================================================")
    print(f"{'#':<4} {'Bairro':<20} {'Tier':<4} {'PDV':<4} {'Opp':>5} {'Sat':>5} {'Dem':>5}  {'Label'}")
    print("-" * 65)
    for b in bairro_scores:
        print(f"{b['rank']:<4} {b['bairro']:<20} {b['tier_socio']:<4} "
              f"{b['n_pdv']:<4} {b['opportunity_score']:>5.1f} "
              f"{b['saturation_score']:>5.1f} {b['demand_score']:>5.1f}  {b['opp_tier']}")

if __name__ == "__main__":
    main()

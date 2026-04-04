"""
L2 Campinas — Neighborhood-Level Pharmacy Opportunity Scoring
Pipeline: OSM pharmacy data → neighborhood assignment → scoring → JSON output

Model (Daniel's framework):
  Opportunity = f(demand_proxy, saturation, brand_mix, income_tier)
  - PDV count = most decisive metric (saturação)
  - Mix pilar = chain ratio vs. independents
  - Marcas certas = concentration of premium vs. popular brands
"""

import json
import math
import csv
from collections import defaultdict
from missing_data_hunter import agentic_pharmacy_fallback

# ─── KNOWN CAMPINAS BAIRROS ─────────────────────────────────────────────────
# Curated list with approximate centroids (lat, lon) and socioeconomic tier
# Tier: A=upper, B=upper-mid, C=middle, D=lower-mid, E=popular
# pop_density: qualitative proxy (1=low … 5=high)

BAIRROS = [
    # name, lat, lon, tier, pop_density, area_km2
    ("Cambuí",                   -22.9022, -47.0509, "A", 4, 1.8),
    ("Jardim Flamboyant",        -22.9085, -47.1042, "A", 3, 2.1),
    ("Mansões Santo Antônio",    -22.9316, -47.1038, "A", 2, 3.5),
    ("Taquaral / Bosque",        -22.8924, -47.0559, "B", 3, 2.4),
    ("Barão Geraldo",            -22.8177, -47.0715, "B", 3, 5.2),
    ("Nova Campinas",            -22.8960, -47.1070, "B", 2, 2.8),
    ("Jardim Chapadão",          -22.8765, -47.0393, "C", 4, 1.9),
    ("Jardim Guanabara",         -22.8869, -47.0308, "C", 4, 1.7),
    ("Vila Itapura",             -22.8868, -47.0584, "C", 4, 1.5),
    ("Botafogo",                 -22.9089, -47.0665, "C", 5, 1.2),
    ("Centro",                   -22.9064, -47.0616, "C", 5, 1.0),
    ("Bonfim",                   -22.9012, -47.0751, "C", 4, 1.6),
    ("Castelo",                  -22.8978, -47.0762, "C", 3, 2.0),
    ("Ponte Preta",              -22.9050, -47.0850, "C", 4, 1.4),
    ("Jardim Brasil",            -22.9200, -47.0450, "D", 4, 2.1),
    ("Jardim Nossa Sra Auxiliadora", -22.9232, -47.0673, "D", 4, 1.8),
    ("Alto da Barra",            -22.9280, -47.0395, "D", 3, 2.3),
    ("Vila União",               -22.9150, -47.0550, "D", 4, 1.5),
    ("Novo Mundo",               -22.9154, -47.0800, "D", 4, 1.7),
    ("São Bernardo",             -22.8350, -47.0900, "D", 3, 3.0),
    ("Prainha",                  -22.8790, -47.0466, "D", 3, 1.6),
    ("Jardim do Trevo",          -22.8640, -47.0600, "D", 3, 2.4),
    ("DIC I / II / III",         -22.8700, -47.1100, "E", 4, 3.2),
    ("Campo Grande",             -22.9250, -47.1150, "E", 4, 2.8),
    ("Ouro Verde",               -22.8550, -47.1200, "E", 3, 3.5),
    ("Jardim Florence",          -22.8450, -47.0750, "E", 3, 2.9),
]

# Income tier → renda_proxy (R$ per capita approximate median)
TIER_RENDA = {"A": 8500, "B": 5200, "C": 2800, "D": 1800, "E": 1100}
# Known chain brands (big national chains = "premium" in Daniel's language)
BIG_CHAINS = {"Drogasil", "Droga Raia", "Drogaria São Paulo", "Pague Menos",
               "Ultrafarma", "Farmácias Nissei", "Pacheco", "Onofre"}
# "popular" brands (franchises / regional chains accessible to C/D/E)
POPULAR_CHAINS = {"Drogão Super", "Drogal", "Drogarias Farmáxima", "Drogaria Familiar",
                  "Extrafarma", "Big Ben Farma"}


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def assign_to_bairro(lat, lon):
    """Nearest bairro by haversine."""
    best, best_d = None, float('inf')
    for b in BAIRROS:
        d = haversine_km(lat, lon, b[1], b[2])
        if d < best_d:
            best_d = d
            best = b
    return best[0], best_d


def classify_brand(name):
    if name in BIG_CHAINS:
        return "big_chain"
    if name in POPULAR_CHAINS:
        return "popular_chain"
    return "independent"


def hhi(counts):
    """Herfindahl-Hirschman Index (0-10000). Higher = more concentrated."""
    total = sum(counts.values())
    if total == 0:
        return 0
    return sum((c / total * 100) ** 2 for c in counts.values())


def main():
    # ── Load OSM data ─────────────────────────────────────────────────────────
    with open("campinas_pharmacies_osm.json") as f:
        osm = json.load(f)
    elements = osm["elements"]
    print(f"Loaded {len(elements)} pharmacies from OSM")

    # ── Assign each pharmacy to a bairro ─────────────────────────────────────
    records = []
    skipped = 0
    for el in elements:
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            skipped += 1
            continue
        tags = el.get("tags", {})
        name = tags.get("name", "unknown")
        brand = tags.get("brand", tags.get("operator", name))
        # Clean brand: take first recognized form
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
    print(f"Assigned {len(records)} pharmacies (skipped {skipped} without coords)")

    # ── Aggregate per bairro ──────────────────────────────────────────────────
    agg = defaultdict(lambda: {
        "pdvs": [], "brands": defaultdict(int),
        "big_chain": 0, "popular_chain": 0, "independent": 0
    })
    for r in records:
        b = r["bairro"]
        agg[b]["pdvs"].append(r)
        agg[b]["brands"][r["brand"]] += 1
        agg[b][r["brand_type"]] += 1

    # ── Compute scores ────────────────────────────────────────────────────────
    bairro_scores = []
    for binfo in BAIRROS:
        bname, blat, blon, tier, pop_d, area = binfo
        data = agg[bname]
        n_pdv_real = len(data["pdvs"])
        renda = TIER_RENDA[tier]

        is_estimated = False
        fallback_evidence = ""
        
        if n_pdv_real == 0:
            # Wake up the Agentic Web Researcher
            print(f"OSM Data missing for {bname}. Asking Web Agent to research...")
            agent_data = agentic_pharmacy_fallback(bname, "Campinas", tier)
            
            n_pdv = agent_data["n_pdv"]
            data["big_chain"] = agent_data["big_chain"]
            data["popular_chain"] = agent_data["popular_chain"]
            data["independent"] = agent_data["independent"]
            
            n_brands = max(1, round(n_pdv * 0.6))  # Simple distinct brand proxy
            market_hhi = 2500  # Estimate mid-concentration for undocumented areas
            is_estimated = True
            fallback_evidence = agent_data["evidence"]
        else:
            n_pdv = n_pdv_real
            n_brands = len(data["brands"])
            market_hhi = hhi(dict(data["brands"]))

        # PDV density (per km²)
        density = n_pdv / area if area > 0 else 0

        # Market concentration (HHI): high HHI = dominated by 1-2 players
        # (calculated above)

        # Chain ratio (higher = more formal market)
        total = n_pdv or 1
        chain_ratio = (data["big_chain"] + data["popular_chain"]) / total

        # Saturation score (0-100): high density + high chain ratio = saturated
        sat_density = min(density / 4.0, 1.0)          # saturated at 4+ pdv/km²
        sat_chain   = chain_ratio                        # 0-1
        saturation  = round((sat_density * 0.6 + sat_chain * 0.4) * 100, 1)

        # Demand proxy (0-100): income × pop_density (normalized roughly)
        demand = round(min((renda / 8500) * (pop_d / 5) * 100, 100), 1)

        # Opportunity = demand minus saturation, floor at 0
        # Daniel's principle: high demand / low saturation = hot zone
        opportunity = round(max(demand - saturation * 0.6, 5.0), 1)

        # Independent opportunity: if market is independent-heavy → opening for chain
        indep_ratio = data["independent"] / total
        chain_entry_gap = round(min(indep_ratio * 100, 100), 1)

        # Tier label for opportunity
        if opportunity >= 70:
            opp_tier = "🔴 Alta"
        elif opportunity >= 45:
            opp_tier = "🟡 Média"
        else:
            opp_tier = "🟢 Baixa"

        bairro_scores.append({
            "bairro": bname,
            "is_estimated": is_estimated,
            "fallback_evidence": fallback_evidence,
            "lat": blat,
            "lon": blon,
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
            "top_brands": ", ".join(
                sorted(data["brands"], key=lambda x: -data["brands"][x])[:4]
            ) if data["brands"] else "—",
            "pdv_list": [
                {"name": p["name"], "brand": p["brand"],
                 "type": p["brand_type"], "lat": p["lat"], "lon": p["lon"]}
                for p in data["pdvs"]
            ]
        })

    # Sort by opportunity desc
    bairro_scores.sort(key=lambda x: -x["opportunity_score"])

    # Assign rank
    for i, b in enumerate(bairro_scores):
        b["rank"] = i + 1

    # ── Save outputs ─────────────────────────────────────────────────────────
    with open("l2_campinas_scores.json", "w", encoding="utf-8") as f:
        json.dump({"municipio": "Campinas", "n_pharmacies_osm": len(records),
                   "bairros": bairro_scores}, f, ensure_ascii=False, indent=2)

    # CSV (no pdv_list)
    with open("l2_campinas_scores.csv", "w", newline="", encoding="utf-8") as f:
        cols = [k for k in bairro_scores[0].keys() if k != "pdv_list"]
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in bairro_scores:
            writer.writerow({k: row[k] for k in cols})

    # Print summary
    print("\n=======================================================")
    print("  L2 CAMPINAS — OPPORTUNITY RANKING")
    print("=======================================================")
    print(f"{'#':<4} {'Bairro':<32} {'Tier':<3} {'PDV':<5} {'Opp':>5} {'Sat':>5} {'Dem':>5}  {'Label'}")
    print("-" * 75)
    for b in bairro_scores:
        print(f"{b['rank']:<4} {b['bairro']:<32} {b['tier_socio']:<3} "
              f"{b['n_pdv']:<5} {b['opportunity_score']:>5.1f} "
              f"{b['saturation_score']:>5.1f} {b['demand_score']:>5.1f}  {b['opp_tier']}")

    print(f"\nFiles: l2_campinas_scores.json, l2_campinas_scores.csv")
    return bairro_scores


if __name__ == "__main__":
    main()

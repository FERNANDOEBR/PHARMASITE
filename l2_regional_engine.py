import os
import time
import requests
import pandas as pd
import numpy as np
import folium
from math import radians, cos, sin, asin, sqrt
import urllib.parse

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers
    return c * r

def query_osm_overpass(query, retries=3):
    url = "http://overpass-api.de/api/interpreter"
    for i in range(retries):
        try:
            response = requests.get(url, params={'data': query}, timeout=60)
            response.raise_for_status()
            return response.json().get("elements", [])
        except Exception as e:
            print(f"        Retry {i+1}/{retries} - Error querying Overpass API: {e}")
            time.sleep((i + 1) * 5)
    return []

def get_city_bairros(city_name):
    # Fetch both suburbs and neighbourhoods
    # Area query requires searching for area by name
    query = f"""
    [out:json];
    area["name"="{city_name}"]["admin_level"="8"]->.searchArea;
    (
      node["place"="suburb"](area.searchArea);
      node["place"="neighbourhood"](area.searchArea);
      way["place"="suburb"](area.searchArea);
      way["place"="neighbourhood"](area.searchArea);
    );
    out center;
    """
    print(f"    Fetching microbairros for {city_name}...")
    elements = query_osm_overpass(query)
    
    bairros = []
    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name")
        if not name:
            continue
        
        if el["type"] == "node":
            lat, lon = el["lat"], el["lon"]
        else: # way or relation with 'center' out
            lat, lon = el.get("center", {}).get("lat"), el.get("center", {}).get("lon")
            
        if lat and lon:
            bairros.append({
                "city": city_name,
                "bairro": name,
                "lat": lat,
                "lon": lon
            })
    return bairros

def get_city_pharmacies(city_name):
    query = f"""
    [out:json];
    area["name"="{city_name}"]["admin_level"="8"]->.searchArea;
    (
      node["amenity"="pharmacy"](area.searchArea);
      way["amenity"="pharmacy"](area.searchArea);
    );
    out center;
    """
    print(f"    Fetching pharmacies for {city_name}...")
    elements = query_osm_overpass(query)
    
    pharmacies = []
    big_chains = ["drogasil", "raia", "são paulo", "pacheco", "venancio", "pague menos"]
    
    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name", "Farmácia Independente/Sem Nome").lower()
        
        # Categorize
        chain_type = "Independent"
        for bc in big_chains:
            if bc in name:
                chain_type = "Big Chain"
                break
                
        if el["type"] == "node":
            lat, lon = el["lat"], el["lon"]
        else:
            lat, lon = el.get("center", {}).get("lat"), el.get("center", {}).get("lon")
            
        if lat and lon:
            pharmacies.append({
                "name": tags.get("name", "Sem Nome"),
                "chain_type": chain_type,
                "lat": lat,
                "lon": lon
            })
    return pharmacies

def process_regional():
    print("====== L2 REGIONAL MICROBAIRROS PIPELINE ======")
    df = pd.read_csv("municipios_sp_scored.csv")
    
    # Filter constraints
    df_filtered = df[(df['distance_campinas_km'] <= 200) & (df['tier'].isin(['A', 'B', 'C']))].copy()
    print(f"Loaded {len(df)} cities. Filtered to {len(df_filtered)} within 200km and Tier A/B/C.")

    # Limit to Top 20 for API safety right now so we don't get banned by OSM
    top_cities = df_filtered.sort_values(by="score", ascending=False).head(20)
    
    master_bairro_data = []

    for _, city_row in top_cities.iterrows():
        city = city_row["nome"]
        base_demand_score = city_row["score_economico"] 
        renda_proxy = city_row["renda_per_capita"]
        
        print(f"\nProcessing {city} (Base Demand: {base_demand_score:.1f}, Renda: R${renda_proxy})")
        bairros = get_city_bairros(city)
        pharmacies = get_city_pharmacies(city)
        
        if not bairros:
            print(f"    WARNING: No bairros found mapped in OSM for {city}.")
            continue
            
        # Initialize counts
        bairro_pdv_map = {b['bairro']: {'total': 0, 'big_chains': 0, 'independents': 0, 'lat': b['lat'], 'lon': b['lon']} for b in bairros}
        
        # Match pharmacies to nearest bairro within 2km
        MAX_DIST_KM = 2.0
        
        for p in pharmacies:
            closest_bairro = None
            min_dist = float('inf')
            
            for b in bairros:
                d = haversine(p['lon'], p['lat'], b['lon'], b['lat'])
                if d < min_dist and d <= MAX_DIST_KM:
                    min_dist = d
                    closest_bairro = b['bairro']
            
            if closest_bairro:
                bairro_pdv_map[closest_bairro]['total'] += 1
                if p['chain_type'] == "Big Chain":
                    bairro_pdv_map[closest_bairro]['big_chains'] += 1
                else:
                    bairro_pdv_map[closest_bairro]['independents'] += 1
                    
        # Calculate Opportunity Scores
        for name, data in bairro_pdv_map.items():
            # Saturation Penalty
            # Big Chains penalize heavily (15 pts), Independents penalize less (5 pts)
            saturation_penalty = (data['big_chains'] * 15) + (data['independents'] * 5)
            
            # Suburb Demand Boost -> Inherits City Economic Score. We give a generic +10% boost for "Centro" or "Jardim" due to typical zoning.
            demand = base_demand_score
            name_lower = name.lower()
            if "centro" in name_lower or "jardim" in name_lower or "morumbi" in name_lower:
                demand *= 1.15
            
            # Final Opportunity
            opportunity = demand - saturation_penalty
            if opportunity < 0: opportunity = 0
            # Cap at 100 for normalization aesthetic
            opportunity = min(opportunity, 100)
            
            # Only append if opportunity is reasonably high (>50) or if it has NO pharmacies (Zero Hallucination core target)
            if opportunity > 40 or data['total'] == 0:
                master_bairro_data.append({
                    "City_Tier": city_row['tier'],
                    "City": city,
                    "Microbairro": name,
                    "Latitude": data['lat'],
                    "Longitude": data['lon'],
                    "City_Income_Proxy": renda_proxy,
                    "Mapped_Pharmacies": data['total'],
                    "Big_Chains_Mapped": data['big_chains'],
                    "Opportunity_Score": round(opportunity, 1)
                })
                
        time.sleep(5) # Be more polite to OSM API

    df_master = pd.DataFrame(master_bairro_data)
    df_master = df_master.sort_values(by="Opportunity_Score", ascending=False)
    df_master.to_csv("l2_regional_master.csv", index=False)
    print(f"\nSaved {len(df_master)} high-opportunity Microbairros to l2_regional_master.csv")
    
    generate_map(df_master)
    generate_pitch_prompt(df_master)

def generate_map(df):
    if len(df) == 0: return
    print("Generating Folium Heatmap...")
    # Center on Campinas
    m = folium.Map(location=[-22.91, -47.05], zoom_start=9, tiles="CartoDB positron")
    
    for _, row in df.iterrows():
        # Red/Orange for 90-100, Yellow for 70-90, Blue for rest
        color = "red" if row["Opportunity_Score"] >= 90 else ("orange" if row["Opportunity_Score"] >= 75 else "blue")
        
        if row["Mapped_Pharmacies"] == 0 and row['Opportunity_Score'] > 80:
             color = "darkred" # Pure gold gap
             
        popup_html = f"<b>{row['Microbairro']} ({row['City']})</b><br>" \
                     f"Opportunity: {row['Opportunity_Score']}<br>" \
                     f"Pharmacies: {row['Mapped_Pharmacies']}<br>" \
                     f"City Renda: R${row['City_Income_Proxy']}"
                     
        folium.CircleMarker(
            location=[row["Latitude"], row["Longitude"]],
            radius=6,
            popup=folium.Popup(popup_html, max_width=300),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7
        ).add_to(m)
        
    m.save("microbairros_map.html")
    print("Saved microbairros_map.html")

def generate_pitch_prompt(df):
    print("Generating AI Pitch Prompt...")
    with open("ai_pitch_prompt.txt", "w", encoding="utf-8") as f:
        f.write("You are an expert Real Estate and Pharmaceutical Market Analyst acting on behalf of PharmaSite.\n")
        f.write("Your task is to draft an executive pitch for pharmaceutical chain expansion teams based on our proprietary L2 Microbairro Opportunity data.\n\n")
        f.write("Below are the top 15 highest opportunity 'blindspots' (microbairros) located within the 200km radius of Campinas.\n")
        f.write("These specific neighborhoods have high baseline city income proxies, but ZERO or severely low formal pharmacy mappings.\n\n")
        
        top15 = df.head(15)
        for _, row in top15.iterrows():
            f.write(f"- {row['Microbairro']}, {row['City']} | Score: {row['Opportunity_Score']} | Renda Base: R${row['City_Income_Proxy']} | PDVs Fisicos: {row['Mapped_Pharmacies']} (Redes: {row['Big_Chains_Mapped']})\n")
    
    print("Saved ai_pitch_prompt.txt")

if __name__ == "__main__":
    process_regional()

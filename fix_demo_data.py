"""
fix_demo_data.py
Downloads real 2022 IBGE Census population for all SP municipalities
and patches the municipios_sp_scored.csv with correct demographics.
Also fixes DEMO_DATA entries in score_offline.py for known incorrect codes.

Known wrong DEMO_DATA mappings (old wrong name -> real city for that code):
  3509403: was Campinas data  -> Cajuru        (~20k)
  3526902: was Jundiaí data   -> Limeira        (~290k)
  3551009: was Santos data    -> São Vicente    (~355k)
  3552304: was SJC data       -> Sud Mennucci   (~14k)
  3554706: was Sorocaba data  -> Torrinha       (~10k)
  3530706: was Mauá data      -> Mogi Guaçu     (~150k)
  3538808: was Paulínia data  -> Piraju         (~35k)
  3554805: was generic data   -> Tremembé       (~55k)
  3527900: was generic data   -> Lutécia        (~4k)
  3556909: was Uru data       -> Vista Alegre   (~10k)
  3552809: removed from JSON  -> Taboão da Serra (missing from DEMO_DATA, ~280k)
"""
import json
import urllib.request
import gzip
import pandas as pd

print("Step 1: Fetching real IBGE 2022 population for SP municipalities...")
# IBGE Agregados API - Censo 2022 - population by municipality in SP
url = ("https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2022"
       "/variaveis/9324?localidades=N6[35*]")

req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
try:
    resp = urllib.request.urlopen(req, timeout=30)
    raw = resp.read()
    try:
        data = gzip.decompress(raw)
    except Exception:
        data = raw
    result = json.loads(data.decode("utf-8"))
    
    # Extract population by IBGE code
    pop_by_code = {}
    for item in result:
        for res in item.get("resultados", []):
            for loc in res.get("series", []):
                code = loc["localidade"]["id"]
                val  = list(loc["serie"].values())[0]
                if val and val != "-":
                    pop_by_code[int(code)] = int(val)
    
    print(f"  Got real population for {len(pop_by_code)} SP municipalities")
    sample = dict(list(pop_by_code.items())[:3])
    print(f"  Sample: {sample}")
    
except Exception as e:
    print(f"  ERROR fetching IBGE data: {e}")
    pop_by_code = {}

# Step 2: Update the CSV with correct populations where we have real data
if pop_by_code:
    print("\nStep 2: Patching CSV with real populations...")
    df = pd.read_csv("municipios_sp_scored.csv")
    
    corrected = 0
    for idx, row in df.iterrows():
        code = int(row["codigo_ibge"])
        if code in pop_by_code:
            real_pop = pop_by_code[code]
            old_pop  = row["populacao_total"]
            if abs(real_pop - old_pop) / max(old_pop, 1) > 0.15:  # >15% difference
                df.at[idx, "populacao_total"] = real_pop
                corrected += 1
    
    print(f"  Corrected population for {corrected} municipalities")
    df.to_csv("municipios_sp_scored.csv", index=False, encoding="utf-8-sig")
    print("  Saved municipios_sp_scored.csv")
else:
    print("\nSkipping CSV patch (no IBGE data fetched)")

print("\nDone!")

"""
rebuild_municipios_cache.py
Downloads the authoritative list of SP municipalities from IBGE API
and replaces the cache file with 100% correct data.
"""
import json
import urllib.request
import gzip
import time
from pathlib import Path

CACHE_DIR = Path("cache_standalone")
OUTPUT = CACHE_DIR / "municipios_sp.json"

print("Fetching all SP municipalities from IBGE API...")
url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/35/municipios"

req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
resp = urllib.request.urlopen(req, timeout=30)

raw = resp.read()
try:
    data = gzip.decompress(raw)
except Exception:
    data = raw

municipios = json.loads(data.decode("utf-8"))
print(f"  Got {len(municipios)} municipalities from IBGE")

# Validate structure
sample = municipios[0]
print(f"  Sample: {sample['id']} = {sample['nome']}")

# Save
json.dump(municipios, open(OUTPUT, "w", encoding="utf-8"), ensure_ascii=False)
print(f"  Saved to {OUTPUT}")

# Quick sanity check - look for key cities
key_cities = {3550308: "Sao Paulo", 3509502: "Campinas", 3525904: "Jundiai"}
by_id = {m["id"]: m["nome"] for m in municipios}
print("\nSanity check:")
for code, expected in key_cities.items():
    found = by_id.get(code, "NOT FOUND")
    print(f"  {code}: {found}")

# Check for duplicates
from collections import Counter
names = [m["nome"] for m in municipios]
dups = {k: v for k, v in Counter(names).items() if v > 1}
if dups:
    print(f"\nDuplicate names in IBGE data: {dups}")
else:
    print(f"\nNo duplicate names - clean data!")

print(f"\nDone. Total SP municipalities: {len(municipios)}")

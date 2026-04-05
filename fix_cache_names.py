"""
fix_municipios_cache.py - Final version
"""
import json

# Additional name corrections from IBGE API verification
CORRECT_NAMES = {
    3508900: "Caiabu",
    3509205: "Cajamar",
    3527900: "Lutecia",     # was "Limeira" - real Limeira = 3526902
    3557709: None,          # EMPTY - will be removed
}

# Ghost codes that return empty or don't exist in IBGE
GHOST_CODES = {3557709}

data = json.load(open("cache_standalone/municipios_sp.json", encoding="utf-8"))
print(f"Before: {len(data)} entries")

for m in data:
    if m["id"] in CORRECT_NAMES and CORRECT_NAMES[m["id"]] is not None:
        print(f"  Name fix {m['id']}: '{m['nome']}' -> '{CORRECT_NAMES[m['id']]}'")
        m["nome"] = CORRECT_NAMES[m["id"]]

before = len(data)
data = [m for m in data if m["id"] not in GHOST_CODES]
print(f"  Removed {before - len(data)} ghost codes")

from collections import defaultdict
by_name = defaultdict(list)
for m in data:
    by_name[m["nome"]].append(m["id"])
dups = {k: v for k, v in by_name.items() if len(v) > 1}
if dups:
    print(f"\nStill have name duplicates: {dups}")
else:
    print("\nNo name duplicates remaining!")

json.dump(data, open("cache_standalone/municipios_sp.json", "w", encoding="utf-8"), ensure_ascii=False)
print(f"After: {len(data)} entries saved.")

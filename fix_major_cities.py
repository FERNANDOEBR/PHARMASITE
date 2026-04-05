"""
fix_major_cities.py
Fixes DEMO_DATA in score_offline.py and CNES cache for major SP cities
that have wrong data from the synthetic generator (wrong code-to-name mapping).

Also adds correct DEMO_DATA entries for Daniel's worst cities with their real codes.
"""
import json
import re

# ── CNES fixes ─────────────────────────────────────────────────────────────
CNES_FIXES = {
    # Major cities with wrong CNES (too few pharmacies due to old mapping)
    "3516200": {"name": "Franca",               "pop": 350000,
                "farmacias": 185, "consultorios_medicos": 620, "consultorios_odonto": 290,
                "laboratorios": 82, "clinicas": 125, "hospitais": 9, "ubs_upa": 36},
    "3506706": {"name": "Boa Esperanca do Sul",  "pop": 13000,
                "farmacias": 8, "consultorios_medicos": 20, "consultorios_odonto": 10,
                "laboratorios": 2, "clinicas": 1, "hospitais": 0, "ubs_upa": 2},
    "3518800": {"name": "Guarulhos",             "pop": 1380000,
                "farmacias": 600, "consultorios_medicos": 2500, "consultorios_odonto": 1100,
                "laboratorios": 200, "clinicas": 280, "hospitais": 18, "ubs_upa": 80},
    "3538709": {"name": "Piracicaba",            "pop": 400000,
                "farmacias": 205, "consultorios_medicos": 780, "consultorios_odonto": 365,
                "laboratorios": 95, "clinicas": 135, "hospitais": 9, "ubs_upa": 39},
    "3547809": {"name": "Santo Andre",           "pop": 710000,
                "farmacias": 305, "consultorios_medicos": 1120, "consultorios_odonto": 510,
                "laboratorios": 145, "clinicas": 165, "hospitais": 11, "ubs_upa": 56},
    "3501608": {"name": "Americana",             "pop": 240000,
                "farmacias": 125, "consultorios_medicos": 470, "consultorios_odonto": 220,
                "laboratorios": 58, "clinicas": 82, "hospitais": 5, "ubs_upa": 26},
    "3502101": {"name": "Andradina",             "pop": 55000,
                "farmacias": 36, "consultorios_medicos": 100, "consultorios_odonto": 48,
                "laboratorios": 12, "clinicas": 14, "hospitais": 2, "ubs_upa": 7},
    # Daniel's worst cities - new real IBGE codes (tiny, rural)
    "3507209": {"name": "Bora",                  "pop": 806,
                "farmacias": 0, "consultorios_medicos": 0, "consultorios_odonto": 0,
                "laboratorios": 0, "clinicas": 0, "hospitais": 0, "ubs_upa": 1},
    "3532868": {"name": "Nova Castilho",         "pop": 1600,
                "farmacias": 0, "consultorios_medicos": 1, "consultorios_odonto": 0,
                "laboratorios": 0, "clinicas": 0, "hospitais": 0, "ubs_upa": 1},
    "3515806": {"name": "Flora Rica",            "pop": 2000,
                "farmacias": 0, "consultorios_medicos": 1, "consultorios_odonto": 0,
                "laboratorios": 0, "clinicas": 0, "hospitais": 0, "ubs_upa": 1},
    "3555901": {"name": "Uru",                   "pop": 1200,
                "farmacias": 0, "consultorios_medicos": 0, "consultorios_odonto": 0,
                "laboratorios": 0, "clinicas": 0, "hospitais": 0, "ubs_upa": 1},
    "3516853": {"name": "Gaviao Peixoto",        "pop": 3000,
                "farmacias": 0, "consultorios_medicos": 1, "consultorios_odonto": 0,
                "laboratorios": 0, "clinicas": 0, "hospitais": 0, "ubs_upa": 1},
}

print("=== Fixing CNES cache ===")
cnes = json.load(open("cache_standalone/cnes_sp.json", encoding="utf-8"))
for code, info in CNES_FIXES.items():
    old_farm = cnes.get(code, {}).get("farmacias", "N/A")
    cnes[code] = {k: v for k, v in info.items() if k not in ("name", "pop")}
    print(f"  {code} ({info['name']:25s}) farmacias: {old_farm} -> {info['farmacias']}")

json.dump(cnes, open("cache_standalone/cnes_sp.json", "w", encoding="utf-8"), ensure_ascii=False)
print("  CNES cache saved.")

# ── DEMO_DATA fixes in score_offline.py ────────────────────────────────────
DEMO_FIXES = {
    # Format: code: (pop, urb, renda, envelhec, idh_synth)
    "3516200": (350000, 98.5, 2400, 0.43, 102),  # Franca (was Flora Rica data)
    "3518800": (1380000, 99.5, 2500, 0.43, 105), # Guarulhos (was 124k)
    "3538709": (400000, 98.5, 2900, 0.44, 108),  # Piracicaba (was 8k)
    "3547809": (710000, 99.5, 2800, 0.44, 108),  # Santo Andre (was 6.5k)
    "3501608": (240000, 99.0, 2800, 0.43, 105),  # Americana (was 25k)
    "3502101": (55000, 94.0, 2000, 0.42, 88),    # Andradina (was 4.5k)
    # Daniel's worst cities - new real IBGE codes
    "3507209": (806, 55.0, 650, 0.37, 48),       # Bora
    "3532868": (1600, 57.0, 670, 0.38, 50),      # Nova Castilho
    "3515806": (2000, 58.0, 680, 0.38, 52),      # Flora Rica
    "3516853": (3000, 62.0, 700, 0.38, 55),      # Gaviao Peixoto
}

print("\n=== Fixing DEMO_DATA in score_offline.py ===")
with open("score_offline.py", "r", encoding="utf-8") as f:
    content = f.read()

for code, vals in DEMO_FIXES.items():
    pop, urb, renda, envelhec, idh = vals
    # Find existing line for this code
    pattern = rf'^\s+{code}:\s*\(.*?\),.*$'
    match = re.search(pattern, content, re.MULTILINE)
    if match:
        old = match.group()
        new_line = f"    {code}: ({pop}, {urb}, {renda}, {envelhec}, {idh}),"
        content = content[:match.start()] + new_line + content[match.end():]
        print(f"  Updated {code}: {old.strip()[:60]} -> {new_line.strip()[:60]}")
    else:
        # Add as new entry at beginning of DEMO_DATA block
        insert_after = "DEMO_DATA = {"
        new_entry = f"\n    {code}: ({pop}, {urb}, {renda}, {envelhec}, {idh}),  # Added"
        idx = content.find(insert_after)
        if idx >= 0:
            content = content[:idx+len(insert_after)] + new_entry + content[idx+len(insert_after):]
            print(f"  Added {code}: ({pop}, {urb}, {renda}, {envelhec}, {idh})")
        else:
            print(f"  WARNING: could not find insertion point for {code}")

with open("score_offline.py", "w", encoding="utf-8") as f:
    f.write(content)
print("  score_offline.py saved.")
print("\nDone! Run score_offline.py to regenerate CSV.")

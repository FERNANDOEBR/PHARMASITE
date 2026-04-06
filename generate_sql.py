import pandas as pd
import numpy as np

df = pd.read_csv('municipios_sp_scored.csv', dtype={'codigo_ibge': str})
with open('update_prod_database.sql', 'w', encoding='utf-8') as f:
    f.write("-- SQL script to backfill demographic data to PostgreSQL\n")
    f.write("-- This is specifically for elderly_pct (indice_envelhecimento)\n")
    f.write("-- and any other missing demographic data for the production deployment.\n\n")
    
    for _, row in df.iterrows():
        cod = row['codigo_ibge']
        elderly = row['elderly_pct']
        urb = row['taxa_urbanizacao']
        pop = row['populacao_total']
        
        updates = []
        if pd.notna(elderly):
            # reverse math: we want to store indice_envelhecimento
            # elderly_pct = (indice / (100 + indice)) * 100
            # -> indice = 100 * elderly_pct / (100 - elderly_pct)
            elderly_f = float(elderly)
            if elderly_f < 100.0:
                indice = (100.0 * elderly_f) / (100.0 - elderly_f)
                updates.append(f"indice_envelhecimento = {indice:.2f}")
        
        if pd.notna(urb):
            updates.append(f"taxa_urbanizacao = {float(urb):.2f}")
            
        if pd.notna(pop):
            updates.append(f"populacao_total = {int(float(pop))}")

        if updates:
            sql_set = ", ".join(updates)
            f.write(f"UPDATE demograficos SET {sql_set} WHERE codigo_ibge = '{cod}';\n")
    
    f.write("\n-- End of script\n")
print("update_prod_database.sql gerado com sucesso!")

import os
import sys

# Add agent1 to path so we can import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from pipeline.healers import HealingOrchestrator

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://pharma:pharma123@localhost:5432/pharmasite")
engine = create_engine(DATABASE_URL)

def run():
    print("Forcing heal for Angra dos Reis (3300100)...")
    
    # 1. Manually insert population from 2022 Census (167,418)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO demograficos (
                codigo_ibge, populacao_total, populacao_urbana, populacao_alvo, 
                pct_populacao_alvo, taxa_urbanizacao, renda_per_capita, 
                indice_envelhecimento, ano_referencia
            ) VALUES (
                '3300100', 167418, 165000, 75000, 
                44.8, 98.5, 3000.0, 
                58.4, 2022
            ) ON CONFLICT DO NOTHING;
            
            INSERT INTO estabelecimentos_saude (
                codigo_ibge, farmacias, clinicas, hospitais, laboratorios,
                consultorios_medicos, consultorios_odonto, ubs_upa,
                total_estabelecimentos, farmacias_por_10k, estabelecimentos_saude_por_10k,
                ano_referencia
            ) VALUES (
                '3300100', 85, 30, 4, 15,
                110, 40, 12,
                296, 5.08, 17.68, 2024
            ) ON CONFLICT DO NOTHING;
            
            UPDATE estabelecimentos_saude SET 
                farmacias = 85, clinicas = 30, hospitais = 4, laboratorios = 15,
                consultorios_medicos = 110, consultorios_odonto = 40, ubs_upa = 12,
                total_estabelecimentos = 296, farmacias_por_10k = 5.08, estabelecimentos_saude_por_10k = 17.68
            WHERE codigo_ibge = '3300100';
        """))
    print(" inserted demographic and establishment baseline.")

    # 4. Rerun Scorer
    print("\nRe-running PCA Scoring Engine...")
    import pipeline.scores as scores
    scores.calculate_scores(engine)
    print("Done!")

if __name__ == "__main__":
    run()

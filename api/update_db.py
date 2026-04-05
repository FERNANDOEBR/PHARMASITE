import pandas as pd
from sqlalchemy import create_engine, text

def main():
    print("Connecting to DB...")
    engine = create_engine('postgresql://pharma:pharma123@localhost:5432/pharmasite')
    df = pd.read_csv('municipios_sp_scored.csv')
    
    print(f"Read {len(df)} rows. Syncing to Postgres...")
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO scores (
                    codigo_ibge, score_demografico, score_infraestrutura_saude,
                    score_economico, score_logistico, score_competitividade,
                    score_total, tier, ranking_nacional, ranking_estadual,
                    distance_campinas_km, pca_component_1, pca_component_2, pca_component_3
                ) VALUES (
                    :codigo_ibge, :score_dem, :score_saude, :score_econ,
                    :score_log, :score_comp, :score_total, :tier,
                    :rank_nac, :rank_est, :dist_campinas, 0.0, 0.0, 0.0
                )
                ON CONFLICT (codigo_ibge) DO UPDATE SET
                    score_demografico           = EXCLUDED.score_demografico,
                    score_infraestrutura_saude  = EXCLUDED.score_infraestrutura_saude,
                    score_economico             = EXCLUDED.score_economico,
                    score_logistico             = EXCLUDED.score_logistico,
                    score_competitividade       = EXCLUDED.score_competitividade,
                    score_total                 = EXCLUDED.score_total,
                    tier                        = EXCLUDED.tier,
                    ranking_nacional            = EXCLUDED.ranking_nacional,
                    ranking_estadual            = EXCLUDED.ranking_estadual,
                    distance_campinas_km        = EXCLUDED.distance_campinas_km,
                    pca_component_1             = 0.0,
                    pca_component_2             = 0.0,
                    pca_component_3             = 0.0,
                    calculated_at               = NOW()
            """), {
                "codigo_ibge":  str(int(row["codigo_ibge"])),
                "score_dem":    float(row.get("score_demografico", 0)),
                "score_saude":  float(row.get("score_saude", 0)),
                "score_econ":   float(row.get("score_economico", 0)),
                "score_log":    float(row.get("score_logistica", 0)),
                "score_comp":   float(row.get("score_competitividade", 0)),
                "score_total":  float(row["score"]),
                "tier":         row["tier"],
                "rank_nac":     int(row["ranking"]),
                "rank_est":     int(row["ranking"]),
                "dist_campinas": float(row.get("distance_campinas_km", 200)),
            })
    print(f"✅ Successfully synced {len(df)} Offline Scores to PostgreSQL Database.")

if __name__ == "__main__":
    main()

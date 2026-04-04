"""
POST /insights/{codigo_ibge}
Calls Claude claude-sonnet-4-6 to generate a structured PT-BR market narrative
for a municipality. Response is cached for 24h (AI calls are expensive).
"""
import os
from datetime import datetime
from typing import Any, Dict, Optional

import anthropic
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Connection

from cache import TTL_INSIGHTS, cache_get, cache_set
from database import get_db
from schemas import InsightsResponse

router = APIRouter()

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 1500


def _fmt(val: Any, suffix: str = "", prefix: str = "", ndigits: int = 1) -> str:
    """Null-safe formatter — renders None/NaN as 'dado não disponível'."""
    if val is None:
        return "dado não disponível"
    if isinstance(val, float):
        if val != val:  # NaN is the only float where x != x
            return "dado não disponível"
        return f"{prefix}{val:.{ndigits}f}{suffix}"
    return f"{prefix}{val}{suffix}"


def build_insights_prompt(data: Dict[str, Any]) -> str:
    """
    B2B prompt: avalia o município como destino de rotas logísticas de distribuição farmacêutica
    com HQ em Campinas-SP. KPI principal: volume absoluto de farmácias (PDVs).
    """
    nome    = data.get("nome", "Município")
    uf      = data.get("uf", "")
    regiao  = data.get("regiao", "")
    tier    = data.get("tier", "N/D")
    dist_campinas = data.get("distance_campinas_km")
    dist_str = f"{dist_campinas:.0f} km" if dist_campinas is not None else "dado não disponível"

    return f"""Você é um **Especialista em Estratégia de Distribuição Farmacêutica B2B no Brasil**.

Avalie o município abaixo como potencial **destino de rotas logísticas** de um distribuidor de medicamentos e correlatos com sede em **Campinas-SP**.
A pergunta que você deve responder é: **"{nome} deve ser incluído na malha de distribuição? Com qual prioridade?"**

O KPI mais importante é a **quantidade absoluta de farmácias (PDVs)** registradas. Quanto mais PDVs e mais próximo do CD, mais eficiente a rota.

---
## MUNICÍPIO: {nome} — {uf} ({regiao})

### Classificação B2B e Distância Logística
- Score B2B de Atratividade: {_fmt(data.get("score_total"), suffix="/100")}
- Tier: {tier} (A = Top 25% nacional | B = 25-50% | C = 50-75% | D = Bottom 25%)
- **Distância do CD (Campinas-SP)**: {dist_str} ← KPI logístico crítico (limite atual: 200 km)
- Score Logístico: {_fmt(data.get("score_logistico"), suffix="/100")} (100 = Campinas; 0 = 200+ km)
- Ranking Nacional: {_fmt(data.get("ranking_nacional"), suffix="º de 5.570")}
- Ranking Estadual ({uf}): {_fmt(data.get("ranking_estadual"), suffix="º")}

### Volume de PDVs (KPI Decisivo)
- **Farmácias (PDVs) — CNES**: {_fmt(data.get("farmacias"))} ← denominador do Índice de Positivação
- Farmácias por 10.000 hab.: {_fmt(data.get("farmacias_por_10k"), ndigits=2)}
- Laboratórios Clínicos: {_fmt(data.get("laboratorios"))}
- Consultórios Odontológicos: {_fmt(data.get("consultorios_odonto"))}
- Hospitais: {_fmt(data.get("hospitais"))}

### Capacidade de Compra dos PDVs (Perfil Econômico do Mercado Local)
- Renda Per Capita: {_fmt(data.get("renda_per_capita"), prefix="R$ ", ndigits=2)}
- PIB Per Capita: {_fmt(data.get("pib_per_capita"), prefix="R$ ", ndigits=2)}
- Cobertura de Planos de Saúde: {_fmt(data.get("cobertura_planos_pct"), suffix="%")}
- IDH: {_fmt(data.get("idh"), ndigits=3)}
- CNPJs Ativos — Farmácias (CNAE 4771): {_fmt(data.get("cnpjs_farmacias"))}

### Perfil Demográfico (contexto secundário)
- População Total: {_fmt(data.get("populacao_total"))}
- População-Alvo 30-64 anos: {_fmt(data.get("populacao_alvo"))}
- Taxa de Urbanização: {_fmt(data.get("taxa_urbanizacao"), suffix="%")}
- Índice de Envelhecimento: {_fmt(data.get("indice_envelhecimento"))} (>100 = mais idosos)

---
## ANÁLISE SOLICITADA (responda em markdown, português brasileiro)

### 1. Resumo Executivo B2B
2-3 frases sobre o potencial do município como destino de distribuição farmacêutica.
Mencione o volume de PDVs, a distância do CD e o Tier.

### 2. Oportunidade Logística: Densidade e Volume de PDVs
Avalie o número absoluto de farmácias como principal driver de eficiência logística.
Uma rota é rentável quando há muitos PDVs próximos uns dos outros.
Considere: o município sozinho justifica uma rota? Ou deve ser combinado com municípios vizinhos?

### 3. Capacidade de Compra dos PDVs e Mix de Produtos
Com base na renda per capita e IDH, qual mix de produto tende a girar melhor?
- Alta renda → genéricos menor participação; dermocosméticos premium, suplementos, higiene especializada
- Baixa/média renda → genéricos, higiene básica, OTCs de volume
Isso afeta o ticket médio por PDV e a rentabilidade da rota.

### 4. Índice de Positivação — Potencial de Cobertura
O município tem {_fmt(data.get("farmacias"))} PDVs registrados (CNES) = universo de positivação disponível.
Qual seria a meta realista de cobertura (% de PDVs a atender) dado o porte do município?
Nota: os PDVs já atendidos pelo distribuidor são dados internos e não estão disponíveis nesta análise.

### 5. Riscos Logísticos
Liste 1 a 3 riscos ou barreiras específicas para a operação de distribuição neste município
(distância, trânsito, concentração de PDVs em área única, baixo volume de PDVs, etc.).

### 6. Recomendação de Malha
Decisão direta: **INCLUIR NA ROTA (PRIORITÁRIO)** / **INCLUIR NA ROTA (SECUNDÁRIO)** / **POSTERGAR** / **EXCLUIR**.
Justifique em 2-3 frases considerando volume de PDVs, distância e potencial de ticket médio.

Seja direto e objetivo. Se algum dado estiver ausente, indique a limitação mas não omita a análise.
"""


@router.post("/insights/{codigo_ibge}", response_model=InsightsResponse)
def get_insights(codigo_ibge: str, db: Connection = Depends(get_db)):
    cache_key = f"insights:{codigo_ibge}"
    cached = cache_get(cache_key)
    if cached:
        return InsightsResponse(**cached)

    row = db.execute(text("""
        SELECT
            m.codigo_ibge, m.nome, m.uf, m.regiao,
            s.score_total, s.tier, s.ranking_nacional, s.ranking_estadual,
            s.score_logistico, s.distance_campinas_km,
            d.populacao_total, d.populacao_alvo, d.renda_per_capita,
            d.taxa_urbanizacao, d.indice_envelhecimento,
            e.farmacias, e.farmacias_por_10k, e.laboratorios,
            e.consultorios_odonto, e.hospitais, e.leitos_total,
            ec.pib_per_capita, ec.cobertura_planos_pct, ec.cnpjs_farmacias, ec.idh
        FROM municipios m
        LEFT JOIN LATERAL (
            SELECT * FROM scores
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) s ON true
        LEFT JOIN LATERAL (
            SELECT * FROM demograficos
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) d ON true
        LEFT JOIN LATERAL (
            SELECT * FROM estabelecimentos_saude
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) e ON true
        LEFT JOIN LATERAL (
            SELECT * FROM indicadores_economicos
            WHERE codigo_ibge = m.codigo_ibge
            ORDER BY id DESC LIMIT 1
        ) ec ON true
        WHERE m.codigo_ibge = :cod
    """), {"cod": codigo_ibge}).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"Município {codigo_ibge} não encontrado")

    data = dict(row)
    prompt = build_insights_prompt(data)

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY não configurada no servidor")

    try:
        client = anthropic.Anthropic(api_key=api_key)
        logger.info(f"Chamando {MODEL} para insights: {codigo_ibge} ({data['nome']})")

        message = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        narrative = message.content[0].text
        logger.success(f"Insights gerados para {codigo_ibge} — {len(narrative)} chars")

    except anthropic.APIStatusError as e:
        logger.error(f"Anthropic API error {codigo_ibge}: {e.status_code} — {e.message}")
        raise HTTPException(status_code=502, detail=f"Erro na API Claude: {e.message}")
    except Exception as e:
        logger.exception(f"Erro inesperado ao gerar insights para {codigo_ibge}: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao gerar análise")

    result = InsightsResponse(
        codigo_ibge=data["codigo_ibge"],
        nome=data["nome"],
        uf=data["uf"],
        tier=data.get("tier"),
        score_total=data.get("score_total"),
        narrative=narrative,
        model_used=MODEL,
        generated_at=datetime.utcnow().isoformat() + "Z",
    )
    cache_set(cache_key, result.model_dump(), TTL_INSIGHTS)
    return result

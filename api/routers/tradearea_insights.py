"""
POST /insights/tradearea — Agent 4: Especialista em Logística B2B Farmacêutica.

Accepts a Huff Gravity Model result (trade area simulation) plus the target
municipality's codigo_ibge, fetches contextual data from the DB, and calls
Claude to generate a B2B logistics expansion analysis for a pharmaceutical
distributor based in Campinas-SP.

B2B Pivot: the simulation models PDV (pharmacy) capture, not consumer visits.
The AI evaluates the municipality as a distribution node, not a retail store location.

Response cached for 24 h per (municipio, radius_km) pair.
"""
import os
from datetime import datetime
from typing import Any, Dict

import anthropic
from fastapi import APIRouter, Depends, HTTPException
from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Connection

from cache import TTL_INSIGHTS, cache_get, cache_set
from database import get_db
from schemas import TradeAreaInsightsRequest, TradeAreaInsightsResponse

router = APIRouter()

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 1600


# ── Null-safe formatter ────────────────────────────────────────────────────────
def _fmt(val: Any, suffix: str = "", prefix: str = "", ndigits: int = 1) -> str:
    """Renders None/NaN as 'dado não disponível'."""
    if val is None:
        return "dado não disponível"
    if isinstance(val, float) and val != val:
        return "dado não disponível"
    if isinstance(val, float):
        return f"{prefix}{val:.{ndigits}f}{suffix}"
    return f"{prefix}{val}{suffix}"


# ── Prompt builder ─────────────────────────────────────────────────────────────
def build_tradearea_prompt(
    municipio: Dict[str, Any],
    request: TradeAreaInsightsRequest,
) -> str:
    nome    = municipio.get("nome", "Município")
    uf      = municipio.get("uf", "")
    regiao  = municipio.get("regiao") or "N/D"
    tier    = municipio.get("tier") or "N/D"
    farmacias_total = municipio.get("farmacias") or 0
    dist_campinas   = municipio.get("distance_campinas_km")

    # Top 10 municipalities by probability
    top_items = sorted(request.items, key=lambda x: x.probability, reverse=True)[:10]
    table_lines = []
    for i, item in enumerate(top_items, 1):
        pdvs = _fmt(item.estimated_pdvs, ndigits=0) if item.estimated_pdvs is not None else "N/D"
        table_lines.append(
            f"{i:2}. {item.nome} ({item.uf}) — "
            f"Dist: {item.distance_km:.0f} km | "
            f"Prob: {item.probability * 100:.1f}% | "
            f"PDVs alcançáveis: {pdvs}"
        )
    table_str = "\n".join(table_lines) if table_lines else "Nenhum dado disponível"
    total_pdvs = _fmt(request.total_estimated_pdvs, ndigits=0)
    dist_str   = f"{dist_campinas:.0f} km" if dist_campinas is not None else "dado não disponível"

    return f"""Você é um **Especialista em Logística e Expansão B2B de Distribuição Farmacêutica**.

Seu papel é avaliar municípios como potenciais **nós de expansão logística** para um distribuidor de medicamentos e correlatos com sede em Campinas-SP.
A pergunta central **não é** "devo abrir uma loja aqui?" — é: **"este município é um bom destino de rota de entrega para meus PDVs (farmácias clientes)?"**

---
## CONTEXTO OPERACIONAL

- **Distribuidor**: HQ em Campinas-SP
- **Raio logístico atual**: ~200 km (em expansão)
- **KPI principal**: volume absoluto de PDVs (farmácias) atendidos — quanto maior a densidade de PDVs, mais eficiente a rota
- **Índice de Positivação**: PDVs atendidos pelo distribuidor ÷ total de PDVs disponíveis no município
  - O município tem **{farmacias_total} farmácias registradas (CNES)** = denominador do Índice de Positivação
  - O numerador (PDVs já atendidos) é dado interno do distribuidor e não está disponível nesta análise

---
## MUNICÍPIO-ALVO: {nome} — {uf}

### Indicadores do Município
- Score de Atratividade B2B: {_fmt(municipio.get("score_total"), suffix="/100")}
- Tier: {tier} (A = Top 25% nacional | B = 25–50% | C = 50–75% | D = Bottom 25%)
- **Distância do CD (Campinas-SP)**: {dist_str}
- **Total de PDVs disponíveis (farmácias CNES)**: {_fmt(municipio.get("farmacias"))}
- Densidade: {_fmt(municipio.get("farmacias_por_10k"), suffix=" farmácias/10k hab", ndigits=2)}
- PIB per Capita: {_fmt(municipio.get("pib_per_capita"), prefix="R$ ", ndigits=2)}
- IDH: {_fmt(municipio.get("idh"), ndigits=3)}
- Cobertura de Planos de Saúde: {_fmt(municipio.get("cobertura_planos_pct"), suffix="%")}
- População-Alvo 30–64 anos: {_fmt(municipio.get("populacao_alvo"))}

---
## RESULTADO DA SIMULAÇÃO GRAVITACIONAL (Raio: {request.radius_km:.0f} km)

**Total de PDVs (farmácias) alcançáveis na área de influência:** {total_pdvs}

> Este número representa o universo de PDVs que o nó logístico em {nome} poderia abastecer,
> ponderado pela probabilidade gravitacional de cada município da área de influência.

### Top {len(top_items)} Municípios na Área de Influência:
{table_str}

---
## ANÁLISE ESTRATÉGICA SOLICITADA

Responda em português brasileiro com as seguintes seções em Markdown:

### 1. Viabilidade Logística
Avalie {nome} como **destino de rota de distribuição**, não como local de abertura de loja varejista.
Considere: distância do CD, volume absoluto de PDVs, densidade por rota e eficiência logística.
Leve em conta o raio de 200 km (limite atual do distribuidor) e o fato de que esse raio pode crescer.

### 2. Análise da Área de Influência
Interprete os resultados do modelo gravitacional: quais municípios vizinhos possuem mais PDVs?
Há concentração geográfica que permitiria uma rota eficiente? Qual o potencial total de PDVs a positivar?

### 3. Eficiência de Rota e Índice de Positivação
Com {farmacias_total} PDVs disponíveis em {nome} e {total_pdvs} PDVs na área de influência total:
- Qual seria a eficiência estimada de uma rota neste território?
- Como o distribuidor deveria priorizar este município em relação à positivação (% de PDVs atendidos)?
- Quais municípios adjacentes formam um cluster logístico eficiente?

### 4. Mix e Segmentação Regional
Com base no perfil econômico do município (renda, IDH, cobertura de planos):
- Qual mix de produtos tende a girar melhor nesta região (genéricos, premium, dermocosméticos)?
- Isso afeta o ticket médio por PDV e a viabilidade da rota?

### 5. Decisão Logística
Recomendação direta: **INCLUIR NA ROTA** / **INCLUIR COM RESSALVAS** / **POSTERGAR** / **EXCLUIR**.
Justifique em 2–3 frases com base no volume de PDVs, distância e eficiência da rota.

Seja direto e objetivo. Se algum dado estiver ausente, indique a limitação mas não omita a análise.
"""


# ── Endpoint ───────────────────────────────────────────────────────────────────
@router.post("/insights/tradearea", response_model=TradeAreaInsightsResponse)
def get_tradearea_insights(
    request: TradeAreaInsightsRequest,
    db: Connection = Depends(get_db),
):
    cache_key = f"tradearea_insights:{request.codigo_ibge}:{request.radius_km:.0f}"
    cached = cache_get(cache_key)
    if cached:
        return TradeAreaInsightsResponse(**cached)

    # Fetch municipality context from DB (latest row per table via LATERAL)
    row = db.execute(text("""
        SELECT
            m.codigo_ibge, m.nome, m.uf, m.regiao,
            s.score_total, s.tier, s.distance_campinas_km,
            d.populacao_alvo,
            e.farmacias, e.farmacias_por_10k,
            ec.pib_per_capita, ec.idh, ec.cobertura_planos_pct
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
    """), {"cod": request.codigo_ibge}).mappings().first()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Município {request.codigo_ibge} não encontrado",
        )

    municipio = dict(row)
    prompt = build_tradearea_prompt(municipio, request)

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY não configurada no servidor")

    try:
        client = anthropic.Anthropic(api_key=api_key)
        logger.info(
            f"Chamando {MODEL} para trade area insights: {request.codigo_ibge} "
            f"({municipio['nome']}) — raio {request.radius_km:.0f} km, "
            f"{len(request.items)} municípios na área"
        )
        message = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        narrative = message.content[0].text
        logger.success(
            f"Trade area insights gerados para {request.codigo_ibge} — {len(narrative)} chars"
        )

    except anthropic.APIStatusError as e:
        logger.error(f"Anthropic API error {request.codigo_ibge}: {e.status_code} — {e.message}")
        raise HTTPException(status_code=502, detail=f"Erro na API Claude: {e.message}")
    except Exception as e:
        logger.exception(f"Erro inesperado ao gerar trade area insights para {request.codigo_ibge}: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao gerar análise estratégica")

    result = TradeAreaInsightsResponse(
        codigo_ibge=municipio["codigo_ibge"],
        nome=municipio["nome"],
        uf=municipio["uf"],
        narrative=narrative,
        model_used=MODEL,
        generated_at=datetime.utcnow().isoformat() + "Z",
    )
    cache_set(cache_key, result.model_dump(), TTL_INSIGHTS)
    return result

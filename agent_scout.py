import os
import time
import json
import logging
from typing import Dict, Any

from anthropic import Anthropic
from duckduckgo_search import DDGS
env_file = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(env_file):
    with open(env_file, 'r', encoding='utf-8') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip("""'" """)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    logger.warning("ANTHROPIC_API_KEY não definido no .env!")

client = Anthropic(api_key=ANTHROPIC_API_KEY)


def fetch_web_context(bairro: str, cidade: str) -> str:
    """Busca contexto recente sobre urbanização futura e demografia (envelhecimento)."""
    logger.info(f"Iniciando busca web avançada para {bairro}, {cidade}...")
    queries = [
        f"lançamentos imobiliários condomínios alvarás de construção {bairro} {cidade} 2024",
        f"novos comércios shoppings parques industriais logística {bairro} {cidade}",
        f"clínica geriátrica casa de repouso asilo terceira idade {bairro} {cidade}"
    ]
    
    combined_results = []
    
    with DDGS() as ddgs:
        for q in queries:
            logger.info(f"Buscando: {q}")
            try:
                # Top 4 para cobrir as 3 queries sem estourar tempo
                results = list(ddgs.text(q, max_results=4, region="br-pt", safesearch="off", timelimit="y"))
                for r in results:
                    combined_results.append(f"Fonte: {r['title']} | URL: {r['href']} | Snippet: {r['body']}")
            except Exception as e:
                logger.error(f"Erro na busca do DDGS para '{q}': {e}")
                
            time.sleep(1) 
            
    if not combined_results:
        return "Nenhum dado recente encontrado na web."
        
    return "\n\n".join(combined_results)


def evaluate_growth_signals(bairro: str, cidade: str, context: str) -> Dict[str, Any]:
    """Chama o Claude para interpretar os sinais de crescimento e envelhecimento extraídos da web."""
    logger.info(f"Enviando dados para o Claude analisar o potencial de {bairro}...")
    
    prompt = f"""Você é um analista experiente em Inteligência de Mercado e Expansão de Varejo (Farma/Saúde).
Sua missão é extrair indicadores projetivos ("forward-looking") de URBANIZAÇÃO e dados reais de ENVELHECIMENTO para um bairro específico.

Bairro em análise: {bairro}, {cidade}

=== RESULTADOS DE BUSCA NA WEB RECENTES SOBRE O BAIRRO ===
{context}
==========================================================
""" + """

Com base UNICAMENTE nos snippets da web fornecidos acima, faça as seguintes extrações.

Responda ESTRITAMENTE em formato JSON com a seguinte estrutura:
{
  "growth_score": 0 a 100,
  "verdict": "Em Expansão" ou "Estável" ou "Estagnado",
  "real_estate_launches": ["Lista sucinta"],
  "commercial_growth": ["Lista sucinta"],
  "senior_demographics": ["Lista evidenciando envelhecimento"],
  "analysis_markdown": "Um parágrafo de 3 a 5 linhas em Markdown",
  "sources": [{"title": "Nome da fonte original", "url": "https://..."}]
}

IMPORTANTE E REGRAS ESTRITAS: 
- NÃO INVENTE DADOS ("DO NOT CHEAT"). NÃO USE "HARD CODE" DE NÚMEROS ARBITRÁRIOS E NÃO ALUCINE.
- Sua análise e as listas de projetos DEVEM FAZER CITAÇÃO DIRETA das URLs fornecidas. Mapeie rigorosamente as `sources` para provar que a análise é real! Se não houver fontes reais que embasam o fato, não cite!
- Para "Envelhecimento", assuma os dados base do IBGE, mas CRUZE-OS ativamente com indicadores reais encontrados nas buscas (ex: inauguração de clínicas geriátricas, casas de repouso). Se não encontrar evidências da web para sustentar, não chute, deixe a lista vazia.
- Retorne APENAS o JSON válido.
- Extraia os nomes exatos de condomínios, empresas ou asilos se confirmados nos snippets."""

    try:
        model_name = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-haiku-latest")
        response = client.messages.create(
            model=model_name,
            max_tokens=1000,
            temperature=0.2,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # O modelo deve retornar um JSON
        content = response.content[0].text.strip()
        # Fallback caso o Claude inclua markdown codeblocks
        if content.startswith("```json"):
            content = content[7:-3]
        elif content.startswith("```"):
            content = content[3:-3]
            
        return json.loads(content.strip())
        
    except Exception as e:
        logger.error(f"Erro na avaliação do LLM: {e}")
        return {
            "error": str(e),
            "growth_score": 50,
            "verdict": "Erro na Análise",
            "real_estate_launches": [],
            "commercial_growth": [],
            "senior_demographics": [],
            "analysis_markdown": "Não foi possível gerar a análise devido a um erro de comunicação com a IA.",
            "sources": []
        }


def run_scout(bairro: str, cidade: str) -> Dict[str, Any]:
    """Orquestrador do Agent Scout"""
    t0 = time.time()
    context = fetch_web_context(bairro, cidade)
    result = evaluate_growth_signals(bairro, cidade, context)
    
    result["scout_metadata"] = {
        "bairro": bairro,
        "cidade": cidade,
        "execution_time_s": round(time.time() - t0, 2),
        "search_results_found": len(context.split("\n\n")) if context != "Nenhum dado recente encontrado na web." else 0
    }
    
    return result

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Agent Scout CLI")
    parser.add_argument("bairro", type=str, help="Nome do bairro")
    parser.add_argument("--cidade", type=str, default="Campinas, SP", help="Cidade/UF")
    args = parser.parse_args()
    
    res = run_scout(args.bairro, args.cidade)
    print("\n--- SCOUT RESULT ---")
    print(json.dumps(res, indent=2, ensure_ascii=False))

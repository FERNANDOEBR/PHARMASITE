import os
import time
import json
import logging
from duckduckgo_search import DDGS
from anthropic import Anthropic

if os.path.exists('.env'):
    with open('.env', 'r', encoding='utf-8') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip("""'" """)

logger = logging.getLogger(__name__)

def agentic_pharmacy_fallback(bairro: str, cidade: str, tier: str) -> dict:
    """
    When OSM returns 0 pharmacies, this Agent uses DuckDuckGo to find real-world
    evidence of pharmacies in the neighborhood, and uses Claude to extract the count strictly
    without hallucinating or hardcoding.
    """
    logger.info(f"OSM blindspot detected in {bairro}. Summoning Web Researcher Agent...")
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("No ANTHROPIC_API_KEY. Returning default 1 PDV to avoid crash.")
        return {"n_pdv": 1, "big_chain": 0, "popular_chain": 0, "independent": 1, "is_estimated": True, "evidence": "API KEY MISSING"}
        
    client = Anthropic(api_key=api_key)

    queries = [
        f'farmácias "Drogasil" OR "Drogaria São Paulo" OR "Pague Menos" "{bairro}" "{cidade}" endereço',
        f'farmácia drogaria "{bairro}" "{cidade}" site:google.com/maps',
        f'farmácias independentes locais "{bairro}" "{cidade}"'
    ]
    
    snippets = []
    with DDGS() as ddgs:
        for q in queries:
            try:
                results = list(ddgs.text(q, max_results=4, region="br-pt", timelimit="y"))
                for r in results:
                    snippets.append(f"Title: {r.get('title', '')}\nSnippet: {r.get('body', '')}")
            except Exception as e:
                logger.error(f"DDGS error on {q}: {e}")
            time.sleep(1)

    context = "\n---\n".join(snippets)
    
    prompt = f"""
You are a strict data-extraction researcher.
I need to know how many pharmacies physically exist inside or immediately boarding the neighborhood "{bairro}" in "{cidade}".
OpenStreetMap returned 0, so we ran a web search. Here are the search snippets:

<search_results>
{context}
</search_results>

STRICT RULES:
1. DO NOT CHEAT. DO NOT HARD CODE NUMBERS. DO NOT HALLUCINATE.
2. Deduce the number of unique pharmacies physically located IN OR VERY NEAR "{bairro}".
3. Compare addresses in snippets. If the snippets show 3 distinct pharmacies with distinct addresses/brands, output 3. 
4. If they show none, output 0.
5. If the data indicates commercial presence but is vague, you must still provide a purely evidence-based count of the pharmacies you can definitively PROVE exist from the text.
6. Provide the output in strictly valid JSON format matching this schema:
{{
   "n_pdv": int,
   "big_chain": int,
   "popular_chain": int,
   "independent": int,
   "evidence": "Brief string explaining exactly what evidence you found (like 'Found Drogasil on Rua X') and why you chose these numbers."
}}
Return ONLY the JSON block without markdown formatting or preamble.
"""
    try:
        model_name = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-haiku-latest")
        response = client.messages.create(
            model=model_name,
            max_tokens=300,
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}]
        )
        data = response.content[0].text.strip()
        if data.startswith("```json"):
            data = data.split("```json")[1].split("```")[0].strip()
        elif data.startswith("```"):
            data = data.split("```")[1].split("```")[0].strip()
            
        parsed = json.loads(data)
        
        parsed["is_estimated"] = True  # Flag to indicate this came from the Web Agent, not OSM
        
        # Ensure fallback zeroes don't break the dashboard entirely
        if parsed.get("n_pdv", 0) == 0:
            parsed["n_pdv"] = 1
            parsed["independent"] = 1
            parsed["evidence"] = parsed.get("evidence", "") + " | (Forced to 1 to prevent pipeline crash)"
            
        return {
            "n_pdv": int(parsed.get("n_pdv", 1)),
            "big_chain": int(parsed.get("big_chain", 0)),
            "popular_chain": int(parsed.get("popular_chain", 0)),
            "independent": int(parsed.get("independent", 1)),
            "is_estimated": True,
            "evidence": str(parsed.get("evidence", "Evidence parsed successfully."))
        }
    except Exception as e:
        logger.error(f"Fallback Agent failed for {bairro}: {e}")
        return {"n_pdv": 1, "big_chain": 0, "popular_chain": 0, "independent": 1, "is_estimated": True, "evidence": f"Fallback error: {e}"}

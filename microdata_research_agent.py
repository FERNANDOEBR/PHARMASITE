import subprocess
import argparse
try:
    from duckduckgo_search import DDGS
except ImportError:
    print("Please install duckduckgo_search: pip install duckduckgo_search")
    DDGS = None

class DataResearcherAgent:
    """
    Subagent: Master Researcher and Data Finding Expert.
    Scope: 
    - Health facilities (Pharmacies, Dentists, UPAs)
    - Demographics (Taxa de envelhecimento, Age distribution)
    - Real Estate Permits (Alvarás, residential, logistical centers)
    """
    def __init__(self, wsl_command="hermes"):
        self.wsl_command = wsl_command
        
    def think_with_hermes(self, prompt: str) -> str:
        full_command = ["wsl", self.wsl_command, prompt]
        print(f"\n[Research Agent] Thinking with local model '{self.wsl_command}'...")
        try:
            result = subprocess.run(
                full_command, capture_output=True, text=True, check=True, encoding='utf-8'
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            return f"Error executing {self.wsl_command}: {e.stderr}"
        except FileNotFoundError:
            return "WSL not found."

    def search_web(self, query: str, max_results=4) -> str:
        print(f"\n[Research Agent] Searching web for: {query}")
        if DDGS is None: return "Search module unavailable."
        results = []
        try:
            with DDGS() as ddgs:
                for r in ddgs.text(query, max_results=max_results):
                    results.append(f"Title: {r.get('title')}\nLink: {r.get('href')}\nSnippet: {r.get('body')}\n")
            return "\n".join(results)
        except Exception as e:
            return f"Search failed: {e}"

    def run_investigation(self):
        print("=========================================================")
        print("  MICRODATA RESEARCHER AGENT INITIALIZED                 ")
        print("=========================================================")
        
        # 1. Health Data (CNES)
        health_results = self.search_web("Base dos dados CNES microdados farmacias UPAs dentistas por bairro download")
        
        # 2. Demographics (IBGE - Censo Age Distribution)
        demographics_results = self.search_web("IBGE Censo 2022 microdados idade taxa de envelhecimento por setor censitario bairro")
        
        # 3. Real Estate Permits (Alvarás / Habite-se)
        real_estate_results = self.search_web("dados abertos prefeitura alvará de construção habite-se bairro logistica residencial")

        prompt = f"""
You are an Expert Data Researcher subagent.
Your goal is to find microdata to evaluate the attractiveness of a 'bairro' (neighborhood) for opening Point of Sales (PDVs), specifically pharmacies.
I have gathered search results across 3 main pillars:

--- 1. HEALTH FACILITIES (Pharmacies, Dentists, UPAs) ---
{health_results}

--- 2. DEMOGRAPHICS (Age distribution, Taxa de envelhecimento) ---
{demographics_results}

--- 3. REAL ESTATE DYNAMICS (Permits, New residential buildings, Logistical centers) ---
{real_estate_results}

Based on these results:
1. Explain exactly WHERE and HOW to extract the Health data (e.g., CNES via Base dos Dados).
2. Explain exactly WHERE and HOW to extract the Demographics data (e.g., IBGE SIDRA or Censo 2022 API).
3. Explain exactly WHERE and HOW to extract Real Estate permit data (e.g., municipal open data portals like GeoSampa or Data.Rio).
4. Provide a structured plan for combining these 3 layers of data into a single DataFrame per bairro.
        """
        synthesis = self.think_with_hermes(prompt)
        print("\n--- HERMES SYNTHESIS & EXTRACTION PLAN ---")
        print(synthesis)
        print("------------------------------------------\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--command", default="hermes", help="Command to run Hermes in WSL")
    args = parser.parse_args()
    agent = DataResearcherAgent(wsl_command=args.command)
    agent.run_investigation()

"""
Agent 3: The Researcher
=======================
A LangChain ReAct agent equipped with DuckDuckGo search that estimates the
real number of health establishments in a Brazilian municipality when the
official CNES/DataSUS API returns zero.

Backend: Claude Haiku (fast + cheap) via langchain-anthropic.
Search:  DuckDuckGoSearchRun from langchain-community.

The agent is lazy-initialised — no LLM calls are made until research_field()
is first invoked, so importing this module is free.
"""
from __future__ import annotations

import os
import re
from typing import Optional

from loguru import logger
from langchain_anthropic import ChatAnthropic
from langchain_community.tools import DuckDuckGoSearchRun
from langchain.agents import AgentExecutor, create_react_agent
from langchain.prompts import PromptTemplate


# Human-readable labels for the search prompt
FIELD_LABELS: dict[str, str] = {
    "farmacias":             "farmácias (pharmacies / drugstores)",
    "clinicas":              "clínicas médicas (medical clinics / polyclinics)",
    "hospitais":             "hospitais (hospitals)",
    "laboratorios":          "laboratórios clínicos (clinical analysis labs)",
    "consultorios_medicos":  "consultórios médicos (medical offices)",
    "consultorios_odonto":   "consultórios odontológicos (dental offices)",
    "ubs_upa":               "UBS / UPA (public primary-care health centers)",
}

# Standard ReAct prompt template (defined locally — no Hub network call)
_REACT_TEMPLATE = """\
You are a meticulous healthcare data researcher specialising in Brazil.
The official government database (CNES/DataSUS) claims there are 0 {field_label} \
in {city_name}, {uf} (population: {population}).

Your task:
1. Search for {field_label} in this specific Brazilian city using web directories.
2. Check sources such as Doctoralia, Google Maps summaries, municipal health pages, \
or news articles.
3. Estimate the realistic minimum number of active facilities.

IMPORTANT: Return ONLY a single integer in your Final Answer. No text, no explanation.

You have access to the following tools:
{tools}

Use this exact format:

Question: {input}
Thought: I need to search for information about {field_label} in {city_name}, {uf}.
Action: {tool_names}
Action Input: {city_name} {uf} Brasil {field_label}
Observation: <search result>
Thought: Based on the results I can estimate...
Final Answer: <integer>

Begin!

Question: {input}
{agent_scratchpad}"""

_PROMPT = PromptTemplate.from_template(_REACT_TEMPLATE)


class DataResearcher:
    """
    Agent 3: The Researcher.

    Deploys a LangChain ReAct agent with DuckDuckGo search to estimate
    missing healthcare establishment counts for a single municipality field.

    Parameters
    ----------
    model : str
        Claude model ID.  Defaults to Haiku (fastest / cheapest) to keep
        per-municipality search costs low.
    max_iterations : int
        Maximum search rounds the agent may take before giving up.
    """

    def __init__(
        self,
        model: str = os.getenv("HEALER_MODEL", "claude-3-haiku-20240307"),
        max_iterations: int = 4,
    ):
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is required for DataResearcher")

        self._llm = ChatAnthropic(
            model=model,
            api_key=api_key,
            temperature=0.1,
            max_tokens=256,
        )
        self._search = DuckDuckGoSearchRun()
        self._tools = [self._search]

        agent = create_react_agent(self._llm, self._tools, _PROMPT)
        self._executor = AgentExecutor(
            agent=agent,
            tools=self._tools,
            verbose=False,          # Set to True for debug output
            handle_parsing_errors=True,
            max_iterations=max_iterations,
            return_intermediate_steps=True,
        )

    def research_field(
        self,
        city_name: str,
        uf: str,
        field: str,
        population: Optional[int] = None,
    ) -> tuple[int, list[str]]:
        """
        Estimate the count for one establishment field in one municipality.

        Returns
        -------
        (estimated_count, source_snippets)
            estimated_count  : integer returned by the agent (0 on failure)
            source_snippets  : list of raw search result excerpts for the
                               verifier to cross-reference
        """
        field_label = FIELD_LABELS.get(field, field.replace("_", " "))
        pop_str = f"{population:,}" if population else "desconhecida"
        question = (
            f"How many {field_label} are officially operating in "
            f"{city_name}, {uf}, Brazil?"
        )

        logger.info(f"  🔍 Pesquisando {field} em {city_name}-{uf} (pop: {pop_str})")

        try:
            result = self._executor.invoke({
                "input": question,
                "city_name": city_name,
                "uf": uf,
                "field_label": field_label,
                "population": pop_str,
            })

            raw_output: str = result.get("output", "0")

            # Extract the first integer found in the output
            digits = re.findall(r"\b\d+\b", raw_output.strip())
            estimated = int(digits[0]) if digits else 0

            # Collect search snippets from intermediate steps
            sources: list[str] = []
            for _action, observation in result.get("intermediate_steps", []):
                obs_str = str(observation)
                if len(obs_str) > 20:
                    sources.append(obs_str[:300])

            logger.success(f"    → Estimativa: {estimated} {field} em {city_name}-{uf}")
            return estimated, sources

        except Exception as exc:
            logger.error(f"    DataResearcher falhou ({city_name}/{field}): {exc}")
            return 0, []

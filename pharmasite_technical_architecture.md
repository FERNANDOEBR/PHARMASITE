# PharmaSite Intelligence - Arquitetura Técnica e Pipeline de Dados

## 1. Arquitetura do Sistema
O PharmaSite Intelligence opera em uma arquitetura de nuvem moderna e desacoplada, projetada para alta disponibilidade e consultas geográficas rápidas.

**Frontend (Camada de Apresentação ao Cliente)**
* **Framework:** Next.js 14+ (React App Router), hospedado na rede global edge da Vercel.
* **Motor Geoespacial:** MapLibre GL JS + Deck.gl. Essa stack com aceleração de hardware WebGL permite que o navegador renderize dezenas de milhares de pontos de dados geográficos simultaneamente sem travamentos.
* **UI/UX Design:** Construído usando Tailwind CSS e ícones Lucide React, com uma estética moderna desenvolvida especificamente para painéis executivos densos em dados.
* **Gerenciamento de Estado:** React Hooks assíncronos orquestram o fluxo de dados entre a seleção no mapa, o ranking da barra lateral e o painel de detalhes focado em IA.

**Backend (Serviços de Dados e Analytics)**
* **Framework:** Python 3.10+ rodando FastAPI, implantado como um Web Service no Render.com.
* **Motor de Processamento:** NumPy e Pandas. O backend lida com a carga pesada de normalização estatística e subdivisões de dados.
* **Simulador Vetorizado de Área de Influência:** A matemática original (linha por linha) foi reescrita em uma fórmula vetorizada de array do NumPy. Quando um usuário consulta uma cidade, o backend calcula a fórmula de Haversine contra todos os 645 municípios do Estado de São Paulo em milissegundos para isolar as cidades dentro de um raio de 50 km.

## 2. Integração com Inteligência Artificial
* **Agente Principal:** Modelo Claude 3 Haiku da Anthropic, através de integração direta via API.
* **Fluxo de Execução:** 
  1. O usuário seleciona um município.
  2. O backend em Python compila todos os dados estatísticos brutos daquela cidade (população, PIB, infraestrutura de saúde, métricas de crescimento).
  3. Esses dados são injetados em um Prompt Template rigoroso, projetado para forçar a IA a agir como uma analista especialista em inteligência de mercado.
  4. A IA retorna uma análise SWOT (Forças, Fraquezas, Oportunidades, Ameaças) sintetizada e conversacional sobre o potencial de estabelecimento de novas farmácias, reduzindo drasticamente o tempo de análise manual.

## 3. Fontes de Dados e Metodologia
O motor agrega conjuntos de dados governamentais multifacetados para construir seus níveis (Tiers):

* **Indicadores Econométricos e Demográficos (IBGE):**
  * **PIB e PIB Per Capita:** Estabelece o poder de compra econômico central do local.
  * **Dinâmica Populacional:** Rastreia a população total, taxas de urbanização e, crucialmente, a *Taxa de Envelhecimento*, um indicador altamente correlacionado com o consumo de medicamentos crônicos.
  * **IDHM:** Índices de Desenvolvimento Humano Municipal.
* **Infraestrutura Comercial e de Saúde (DataSUS / Dados Customizados):**
  * Rastreia estabelecimentos de saúde existentes e pontos de venda (PDVs) de farmácias georreferenciados.
  * Calcula a saturação atual do mercado (número de cidadãos por farmácia).
* **Algoritmo de Pontuação Proprietário:**
  * As cidades são ranqueadas em Tiers (1 a 5) por um sistema de notas ponderadas. Um alto poder de compra e uma população envelhecida, combinados com uma baixa saturação farmacêutica, elevam um município específico ao status de "Tier 1", alertando os desenvolvedores sobre "vácuos de expansão".

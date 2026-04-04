# Pharmasite Intelligence — Guia de Execução Local

Pipeline standalone para gerar `municipios_sp_scored.csv` e rodar o backend.
Sem Docker, sem Postgres. Funciona direto na sua máquina com Python 3.10+.

---

## Pré-requisitos

```bash
python --version   # 3.10 ou superior
pip install requests pandas numpy scikit-learn loguru tenacity fastapi uvicorn anthropic python-dotenv
```

---

## DIA 1 — Coletar dados e gerar o CSV

```bash
cd pharmasite2
python run_standalone.py
```

Tempo estimado: **15–45 minutos** (depende da velocidade das APIs públicas).

O script coleta automaticamente de 5 fontes:

| Fonte | Dados | URL |
|---|---|---|
| IBGE Localidades | 645 municípios SP + coordenadas | servicodados.ibge.gov.br |
| IBGE Agregados v3 | Pop. total, faixas etárias, renda | servicodados.ibge.gov.br |
| CNES DataSUS | Farmácias, clínicas, laboratórios | cnes.datasus.gov.br |
| IPEADATA | PIB per capita, IDH | ipeadata.gov.br |
| ANS | Beneficiários planos de saúde | ans.gov.br |

**Cache automático:** se uma API falhar, o script usa o cache local (`cache_standalone/`).
Reexecutar o script pula etapas já coletadas (sem refazer requests).

**Output:**
- `municipios_sp_scored.csv` — 645 municípios com score, tier e todos indicadores
- `pipeline_standalone.log` — log completo da execução

---

## Modelo de Scoring

Score aditivo por 4 pilares (total 370 pts → normalizado 0-100):

```
Score = 27.0% × Demo  +  27.0% × Logística  +  24.3% × Economia  +  21.6% × Saúde
```

| Pilar | Peso | Sub-indicadores |
|---|---|---|
| Demográfico | 27% | Pop. total (40%), pop. 30-64 (35%), urbanização (15%), envelhecimento (10%) |
| Logístico | 27% | Dist. Campinas (45%), nº farmácias (40%), farmácias/10k hab. (15%) |
| Econômico | 24% | Renda per capita (40%), PIB per capita (25%), IDH (20%), cobertura planos (15%) |
| Saúde | 22% | Farmácias (55%), consultórios odonto (20%), laboratórios (15%), clínicas (10%) |

**Tiers:** A (top 25%), B (25-50%), C (50-75%), D (bottom 25%)

**Validação empírica:** Campinas deve ficar top 5%, Borá no bottom 10%.

---

## DIA 2 — Rodar a API e abrir o mapa

### 1. Configurar chave da API

Adicione ao `.env` (já existe no projeto):

```
ANTHROPIC_API_KEY=sk-ant-...   # para o botão "Gerar análise IA"
```

### 2. Iniciar a API

```bash
python api.py
```

Endpoints disponíveis em `http://localhost:8002`:

| Método | Endpoint | Descrição |
|---|---|---|
| GET | `/municipios` | Todos os 645 municípios com scores |
| GET | `/busca?q=Campinas` | Busca por nome |
| GET | `/ranking?tier=A` | Ranking por tier |
| POST | `/analise/3509502` | Análise IA do município (Claude Sonnet) |
| GET | `/stats` | Estatísticas gerais do dataset |

### 3. Abrir o frontend

Abra `frontend/index.html` diretamente no browser (ou `python -m http.server 3000`).

---

## Estrutura dos arquivos gerados

```
pharmasite2/
├── run_standalone.py        ← Pipeline Dia 1 (NOVO)
├── api.py                   ← Backend FastAPI Dia 2 (NOVO)
├── municipios_sp_scored.csv ← Output do pipeline
├── pipeline_standalone.log  ← Log de execução
├── cache_standalone/        ← Cache intermediário (JSON/CSV por fonte)
│   ├── municipios_sp.json
│   ├── coords_sp.json
│   ├── cnes_sp.json
│   ├── ipeadata_pib.csv
│   ├── ipeadata_idh.csv
│   └── ans_beneficiarios.json
├── agent1/                  ← Pipeline Docker/Postgres (infraestrutura completa)
└── frontend/                ← index.html (mapa Leaflet)
```

---

## Troubleshooting

**API CNES retornou 0 farmácias:**
O DataSUS tem instabilidade frequente. O script tenta 2 endpoints diferentes e cacheia.
Aguarde 30 minutos e delete `cache_standalone/cnes_sp.json` para retentar.

**IBGE pop. total zerada para muitos municípios:**
Os agregados do Censo 2022 são publicados em fases. O script tenta 3 combinações de
agregado/variável. Dado parcial não impede o scoring — pilares sem dados recebem peso zero.

**Timeout na ANS:**
O arquivo CSV da ANS (~50MB) pode demorar. Se falhar, `cobertura_planos_pct` ficará 0
e o score econômico usará apenas PIB/IDH/renda.

**Para limpar o cache e recolectar tudo:**
```bash
rm -rf cache_standalone/
python run_standalone.py
```

---

## Colunas do CSV gerado

| Coluna | Descrição |
|---|---|
| codigo_ibge | Código IBGE 7 dígitos |
| nome | Nome do município |
| score | Score final 0-100 |
| tier | A / B / C / D |
| ranking | Posição entre 645 |
| score_demografico | Sub-score pilar Demo (0-100) |
| score_logistica | Sub-score pilar Logística (0-100) |
| score_economico | Sub-score pilar Economia (0-100) |
| score_saude | Sub-score pilar Saúde (0-100) |
| farmacias | Nº farmácias ativas (CNES) |
| consultorios_odonto | Nº consultórios odontológicos |
| laboratorios | Nº laboratórios |
| clinicas | Nº clínicas/centros especialidade |
| farmacias_por_10k | Farmácias por 10k habitantes |
| populacao_total | Pop. residente 2022 (IBGE) |
| populacao_alvo | Pop. 30-64 anos (core buyer) |
| renda_per_capita | Renda domiciliar per capita (R$) |
| pib_per_capita | PIB per capita municipal (R$) |
| idh | IDH municipal (0-1) |
| cobertura_planos_pct | % pop. com plano de saúde |
| distance_campinas_km | Distância em linha reta de Campinas |
| latitude / longitude | Centróide do município |

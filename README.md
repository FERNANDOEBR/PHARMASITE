# 🏥 PharmaSite Intelligence

### Plataforma de Geomarketing para Correlatos Farmacêuticos

---

## ✅ Pré-requisitos

Antes de começar, certifique-se de ter instalado:

| Ferramenta | Versão mínima | Instalação |
|------------|---------------|------------|
| Docker | 24+ | [docs.docker.com/get-docker](https://docs.docker.com/get-docker/) |
| Docker Compose | 2.20+ | Incluído no Docker Desktop |
| Make | Qualquer | `brew install make` / `apt install make` |
| Git | Qualquer | [git-scm.com](https://git-scm.com/) |

> 💡 **Nota:** Python e Node.js **não precisam** ser instalados localmente — tudo roda de forma isolada dentro dos containers Docker.

---

## 🔑 Variáveis de Ambiente

O projeto utiliza um arquivo `.env` para gerenciar as configurações.

| Variável | Obrigatória | Padrão | Descrição |
|----------|-------------|--------|-----------|
| `ANTHROPIC_API_KEY` | Sim | - | Chave da API Claude ([Console Anthropic](https://console.anthropic.com/)) |
| `GEMINI_API_KEY` | Sim | - | Chave da API Gemini ([AI Studio](https://aistudio.google.com/)) |
| `POSTGRES_DB` | Não | `pharmasite` | Nome do banco de dados |
| `POSTGRES_USER` | Não | `pharma` | Usuário do banco |
| `POSTGRES_PASSWORD`| Não | `pharma123` | Senha do banco |
| `DATABASE_URL` | Não | (Auto-gerada) | URL completa de conexão |
| `REDIS_URL` | Não | `redis://redis:6379`| URL de conexão do Redis |
| `ENVIRONMENT` | Não | `development` | `development` ou `production` |
| `LOG_LEVEL` | Não | `INFO` | `DEBUG`, `INFO` ou `WARNING` |

> \* Os valores padrão já estão configurados no `.env.example`. Altere-os apenas se sua infraestrutura exigir.

---

## ⚡ Quick Start

Para iniciar o projeto rapidamente, siga os 3 passos abaixo:

```bash
# 1. Configure as variáveis de ambiente
cp .env.example .env
# Edite o arquivo .env com suas API keys (ANTHROPIC_API_KEY, GEMINI_API_KEY)

# 2. Suba a infraestrutura (Banco de Dados, Redis, etc)
make up

# 3. Rode o pipeline de dados (~30-60min para o Brasil todo)
make agent1
```

## 🔍 Verificar Progresso

Caso queira acompanhar a execução do pipeline ou verificar o status do banco:

```bash
# Em outro terminal, para verificar os dados no banco:
make db-check

# Ou acompanhe os logs em tempo real:
make logs
```

---

## 📊 Stack Tecnológica

| Componente | Tecnologia |
|-----------|-----------|
| **Banco de Dados**| PostgreSQL 15 + PostGIS (Dados geoespaciais) |
| **Cache / Fila** | Redis 7 |
| **Data Pipeline** | Python + pandas + geopandas |
| **Modelagem/Scoring**| scikit-learn (PCA) |
| **API Backend** | FastAPI |
| **Frontend web** | Next.js 14 |

## 🗂️ Fontes de Dados

| Fonte | Dados extraídos | Cobertura |
|-------|-----------------|-----------|
| **IBGE SIDRA** | Censo 2022 (população, renda, faixas etárias) | 5.570 municípios |
| **CNES DataSUS** | Estabelecimentos de saúde por tipo e especialidade | Atualização mensal |
| **IPEADATA** | PIB per capita, IDH municipal | Anual |
| **ANS** | Beneficiários de planos de saúde | Trimestral |

## 🎯 Modelo de Scoring

O algoritmo de scoring classifica os municípios brasileiros considerando os seguintes pesos no PCA:

- **25%** `populacao_alvo` (30-64 anos) — *Core buyer para correlatos*
- **20%** `renda_per_capita` — *Poder de compra local*
- **15%** `populacao_total` — *Tamanho absoluto do mercado*
- **15%** `farmacias` — *Proxy para saturação e tamanho do mercado existente*
- **15%** `cobertura_planos_pct` — *Acesso à saúde privada*
- **10%** `consultorios_odonto` — *Key metric para correlatos odontológicos*

> **Output gerado:** Score de 0 a 100 + Classificação de Tier (A/B/C/D) + Rankings a nível Nacional e Estadual.

## 📁 Estrutura do Projeto

```text
pharmasite/
├── docker-compose.yml
├── .env.example
├── Makefile
├── shared/
│   └── init.sql          # Schema DB inicial completo
├── agent1/               # Pipeline de dados (Agente 1)
│   ├── main.py           # Orquestrador do pipeline
│   ├── pipeline/
│   │   ├── municipios.py  # Dados IBGE (5570 municípios)
│   │   ├── demograficos.py# Dados do Censo 2022
│   │   ├── cnes.py        # Dados de estabelecimentos DataSUS
│   │   ├── economicos.py  # PIB, IDH, ANS
│   │   └── scores.py      # Lógica de scoring PCA
│   └── db/
│       └── connection.py
├── api/                  # FastAPI (Agente 2 - Próxima etapa)
└── frontend/             # Next.js (Agente 3 - Próxima etapa)
```

## 🔄 Roadmap & Status

- [x] **Agente 1**: Data Pipeline — 5.570 municípios, PostGIS, scoring
- [x] **Agente 2**: API FastAPI — `/municipios`, `/score`, `/ranking`, `/optimize`, `/insights`, `/tradearea`
- [x] **Agente 3**: Frontend Next.js + Deck.gl — mapa interativo, ranking sidebar, detalhe de cidade
- [x] **Agente 4**: IA Insights — narrativas B2B por Claude em PT-BR
- [x] **Entrega 1 — Modelo v3.0**: Score Aditivo por Pilares (pesos do cliente: Demo 100 + Log 100 + Eco 90 + Saúde 80)
- [x] **Entrega 2 — Microeconomia por Bairro**: Setores censitários IBGE 2022 + PDVs OpenStreetMap (SP)
  - Endpoints: `GET /setores/{codigo_ibge}` e `GET /setores/{codigo_ibge}/hotspots`
  - Resolve o problema central: *"os dois mundos dentro de Campinas"*
- [x] **Entrega 3**: Agent Scout — sinais de crescimento por CEP (delta CNPJs, lançamentos imobiliários)
- [ ] **Agente 5**: QA e Deploy para Produção

## ▶️ Como rodar (Entrega 2 inclusa)

```bash
make up       # sobe DB + Redis
make agent1   # pipeline completo: municípios + scores + setores SP + PDVs OSM
make api      # API em http://localhost:8000/docs
```

Novos endpoints após pipeline:
- `GET /setores/3509502` → setores de Campinas com PDVs por bairro
- `GET /setores/3509502/hotspots` → top bairros por concentração de farmácias

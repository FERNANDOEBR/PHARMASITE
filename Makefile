.PHONY: help up down db agent1 api api-logs api-stop logs clean reset

help:
	@echo "──────────────────────────────────────"
	@echo "  PHARMASITE INTELLIGENCE - COMANDOS  "
	@echo "──────────────────────────────────────"
	@echo "  make setup     → Copia .env e prepara"
	@echo "  make up        → Sobe DB + Redis"
	@echo "  make agent1    → Roda pipeline dados"
	@echo "  make api       → Sobe API FastAPI (porta 8000)"
	@echo "  make api-logs  → Logs da API em tempo real"
	@echo "  make logs      → Logs do agent1"
	@echo "  make db-check  → Verifica dados no DB"
	@echo "  make clean     → Para e remove containers"
	@echo "  make reset     → Apaga tudo e recomeça"
	@echo "──────────────────────────────────────"

setup:
	@if [ ! -f .env ]; then cp .env.example .env && echo "✅ .env criado — adicione suas API keys"; fi
	@mkdir -p agent1/data

up:
	docker compose up -d db redis
	@echo "⏳ Aguardando DB ficar pronto..."
	@sleep 5
	@docker compose ps

agent1:
	docker compose up --build agent1

api:
	docker compose up --build -d api
	@echo "✅ API rodando em http://localhost:8000"
	@echo "   Docs:  http://localhost:8000/docs"
	@echo "   ReDoc: http://localhost:8000/redoc"

api-logs:
	docker compose logs -f api

api-stop:
	docker compose stop api

logs:
	docker compose logs -f agent1

db-shell:
	docker compose exec db psql -U pharma -d pharmasite

db-check:
	@echo "── Status do pipeline ──"
	docker compose exec db psql -U pharma -d pharmasite -c \
		"SELECT etapa, status, municipios_processados, mensagem FROM pipeline_log ORDER BY id;"
	@echo ""
	@echo "── Contagem de registros ──"
	docker compose exec db psql -U pharma -d pharmasite -c \
		"SELECT 'municipios' as tabela, count(*) FROM municipios \
		 UNION ALL SELECT 'demograficos', count(*) FROM demograficos \
		 UNION ALL SELECT 'estabelecimentos', count(*) FROM estabelecimentos_saude \
		 UNION ALL SELECT 'economicos', count(*) FROM indicadores_economicos \
		 UNION ALL SELECT 'scores', count(*) FROM scores;"
	@echo ""
	@echo "── Top 10 municípios ──"
	docker compose exec db psql -U pharma -d pharmasite -c \
		"SELECT m.nome, m.uf, s.score_total, s.tier, s.ranking_nacional \
		 FROM scores s JOIN municipios m ON s.codigo_ibge = m.codigo_ibge \
		 ORDER BY s.score_total DESC LIMIT 10;"

clean:
	docker compose down

reset:
	docker compose down -v
	@echo "⚠️  Volumes apagados. Rode 'make up' para recomeçar."

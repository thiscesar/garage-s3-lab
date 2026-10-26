# Laboratório Garage S3. Alvos: venv, env, up, down, reset, status, test, test-integration, painel, report, all-experiments.
SHELL   := /bin/bash
PY      := .venv/bin/python
COMPOSE := docker compose
NOS     := garage1 garage2 garage3

.PHONY: venv env up down reset status test test-integration painel report all-experiments

## venv: cria .venv e instala o pacote lab (editável) com as ferramentas de teste
venv: .venv/.instalado
.venv/.instalado: pyproject.toml
	test -x $(PY) || python3 -m venv .venv   # recriar falha em Pythons que instalam os activate* só leitura
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -e '.[dev]'
	touch $@

## env: cria/completa o .env (segredos, UID/GID) e verifica colisão das faixas de rede
env: venv
	$(PY) -m lab.config init-env
	$(PY) -m lab.config check-redes

## up: sobe do zero ou retoma o cluster em modo consistent (render + compose + iniciar.sh)
up: env
	mkdir -p dados/g1 dados/g2 dados/g3
	$(PY) -m lab.config render --mode consistent
	$(COMPOSE) up -d --build
	./scripts/iniciar.sh

## down: para e remove os contêineres (mantém dados/)
down:
	$(COMPOSE) down

## reset: derruba tudo e apaga dados/ e generated/
reset:
	./scripts/reset.sh

## status: nós, rede, saúde, modo, quóruns e layout
status: venv
	@.venv/bin/labctl status

## test: testes unitários, sem Docker
test: venv
	$(PY) -m pytest -m "not integration" -q

## test-integration: smoke test com o cluster real no ar
test-integration: venv
	$(PY) -m pytest -m integration -q

## painel: painel web da demo em http://127.0.0.1:8080; Ctrl+C para sair
painel: venv
	.venv/bin/labctl painel

## report: copia as execuções de referência para results/final/ e gera results/RELATORIO_DADOS.md
report: venv
	.venv/bin/labctl report

## all-experiments: E1 a E9 em sequência (~4,5 h; QUICK=1 roda todos em --quick, ~17 min); para no 1º erro
all-experiments: venv
	for e in E1 E2 E3 E4 E5 E6 E7 E8 E9; do .venv/bin/labctl run $$e $(if $(QUICK),--quick,) || exit 1; done

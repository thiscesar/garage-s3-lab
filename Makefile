# Laboratório Garage S3. Alvos: venv, env, up, down, reset, status, test, test-integration, painel.
SHELL   := /bin/bash
PY      := .venv/bin/python
COMPOSE := docker compose
NOS     := garage1 garage2 garage3

.PHONY: venv env up down reset status test test-integration painel

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

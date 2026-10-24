# Laboratório Garage S3. Alvos: venv, env, up, down, reset, status.
SHELL   := /bin/bash
PY      := .venv/bin/python
COMPOSE := docker compose
NOS     := garage1 garage2 garage3

.PHONY: venv env up down reset status

## venv: cria .venv e instala o pacote lab (editável)
venv: .venv/.instalado
.venv/.instalado: pyproject.toml
	python3 -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -e .
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

## status: contêineres, modo e visão do cluster pelo garage1
status:
	@$(COMPOSE) ps
	@echo "modo: $$(cat generated/MODE 2>/dev/null || echo '?')"
	@docker exec garage1 /garage status 2>/dev/null || echo "garage1 indisponível"

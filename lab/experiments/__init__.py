"""Experimentos E1–E9. Cada um é uma subclasse de `base.Experiment`."""

from __future__ import annotations

from importlib import import_module

# nome → "módulo:Classe" (importados só quando usados)
EXPERIMENTOS = {
    "E1": "lab.experiments.e1_topologia:E1",
    "E2": "lab.experiments.e2_caminho:E2",
    "E3": "lab.experiments.e3_replicacao:E3",
    "E4": "lab.experiments.e4_consistencia:E4",
}


def carregar(nome: str):
    nome = nome.upper()
    if nome not in EXPERIMENTOS:
        raise ValueError(f"experimento desconhecido: {nome} (disponíveis: {', '.join(EXPERIMENTOS)})")
    modulo, classe = EXPERIMENTOS[nome].split(":")
    return getattr(import_module(modulo), classe)

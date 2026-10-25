"""Figuras dos experimentos: Matplotlib, PNG + SVG, mesmas cores por modo em todas."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # sem tela: gera arquivos
import matplotlib.pyplot as plt

CORES_MODO = {"consistent": "#2b6cb0", "degraded": "#dd8a1b", "dangerous": "#c53030"}
CORES_CLASSE = {"fresh": "#2f855a", "stale": "#dd8a1b", "error": "#c53030", "anomaly": "#6b46c1"}
CORES_NO = {1: "#2b6cb0", 2: "#2f855a", 3: "#805ad5"}

plt.rcParams.update(
    {
        "figure.dpi": 110,
        "savefig.bbox": "tight",
        "axes.grid": True,
        "grid.alpha": 0.3,
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)


def salvar(fig, destino: Path, nome: str) -> list[Path]:
    """Grava `nome`.png e `nome`.svg em `destino` e fecha a figura."""
    destino.mkdir(parents=True, exist_ok=True)
    arquivos = [destino / f"{nome}.png", destino / f"{nome}.svg"]
    for arq in arquivos:
        fig.savefig(arq)
    plt.close(fig)
    return arquivos


def barras_com_erro(
    rotulos: list[str],
    medias: list[float],
    erros: list[float],
    titulo: str,
    eixo_y: str,
    cores: list[str] | None = None,
    linha_limite: tuple[float, str] | None = None,
):
    """Barras com barra de erro (ex.: média ± desvio ou IC95%); `linha_limite` = (valor, rótulo)."""
    fig, ax = plt.subplots(figsize=(6, 3.6))
    ax.bar(rotulos, medias, yerr=erros, capsize=5, color=cores or "#4a5568", alpha=0.9)
    if linha_limite:
        valor, rotulo = linha_limite
        ax.axhline(valor, color="#c53030", linestyle="--", linewidth=1)
        ax.text(len(rotulos) - 0.5, valor, f" {rotulo}", color="#c53030", va="bottom", ha="right", fontsize=9)
    ax.set_title(titulo)
    ax.set_ylabel(eixo_y)
    return fig


def barras_agrupadas(
    grupos: list[str],
    series: dict[str, tuple[list[float], list[float]]],
    titulo: str,
    eixo_y: str,
    cores: dict[str, str] | None = None,
):
    """Barras agrupadas: `series` = {nome: (valores por grupo, erros por grupo)}."""
    import numpy as np

    fig, ax = plt.subplots(figsize=(7, 3.8))
    largura = 0.8 / max(1, len(series))
    x = np.arange(len(grupos))
    for k, (nome, (valores, erros)) in enumerate(series.items()):
        ax.bar(
            x + (k - (len(series) - 1) / 2) * largura,
            valores,
            largura,
            yerr=erros,
            capsize=4,
            label=nome,
            color=(cores or {}).get(nome),
        )
    ax.set_xticks(x, grupos)
    ax.set_title(titulo)
    ax.set_ylabel(eixo_y)
    ax.legend()
    return fig

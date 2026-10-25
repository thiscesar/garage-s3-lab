"""Estatísticas das medições (roteiro R7: ≥ 5 repetições com média e desvio)."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
from scipy import stats as sps


@dataclass(frozen=True)
class Resumo:
    n: int
    media: float
    desvio: float  # desvio-padrão amostral (n − 1)
    ic95: tuple[float, float]  # intervalo de confiança de 95% da média (t de Student)
    p50: float
    p95: float
    p99: float
    minimo: float
    maximo: float


def resumir(valores: Sequence[float]) -> Resumo:
    """Resumo de uma amostra. Com n = 1 o desvio e o IC ficam indefinidos (NaN)."""
    x = np.asarray(valores, dtype=float)
    if x.size == 0:
        raise ValueError("amostra vazia")
    media = float(x.mean())
    if x.size > 1:
        desvio = float(x.std(ddof=1))
        meia = float(sps.t.ppf(0.975, df=x.size - 1) * desvio / math.sqrt(x.size))
        ic = (media - meia, media + meia)
    else:
        desvio, ic = math.nan, (math.nan, math.nan)
    p50, p95, p99 = (float(v) for v in np.percentile(x, [50, 95, 99]))
    return Resumo(int(x.size), media, desvio, ic, p50, p95, p99, float(x.min()), float(x.max()))


@dataclass(frozen=True)
class Proporcao:
    sucessos: int
    total: int
    p: float
    ic95: tuple[float, float]  # intervalo de Wilson


def wilson(sucessos: int, total: int, z: float = 1.959963984540054) -> Proporcao:
    """Proporção com intervalo de Wilson (bom para p perto de 0 ou 1, ex.: taxa de leituras desatualizadas)."""
    if total <= 0:
        raise ValueError("total deve ser positivo")
    if not 0 <= sucessos <= total:
        raise ValueError("sucessos fora de [0, total]")
    p = sucessos / total
    den = 1 + z**2 / total
    centro = (p + z**2 / (2 * total)) / den
    meia = z * math.sqrt(p * (1 - p) / total + z**2 / (4 * total**2)) / den
    return Proporcao(sucessos, total, p, (max(0.0, centro - meia), min(1.0, centro + meia)))

"""lab.stats comparado com numpy/scipy."""

import math

import numpy as np
import pytest
from scipy import stats as sps

from lab.stats import resumir, wilson

AMOSTRA = [12.1, 9.8, 11.4, 10.2, 13.7, 10.9, 9.5]


def test_resumo_bate_com_numpy_e_scipy():
    r = resumir(AMOSTRA)
    x = np.array(AMOSTRA)
    assert r.n == len(AMOSTRA)
    assert r.media == pytest.approx(x.mean())
    assert r.desvio == pytest.approx(x.std(ddof=1))  # amostral
    ic = sps.t.interval(0.95, df=len(x) - 1, loc=x.mean(), scale=sps.sem(x))
    assert r.ic95 == pytest.approx(ic)
    assert (r.p50, r.p95, r.p99) == pytest.approx(tuple(np.percentile(x, [50, 95, 99])))
    assert (r.minimo, r.maximo) == (min(AMOSTRA), max(AMOSTRA))


def test_uma_medida_tem_desvio_e_ic_indefinidos():
    r = resumir([5.0])
    assert r.media == 5.0
    assert math.isnan(r.desvio)
    assert all(math.isnan(v) for v in r.ic95)


def test_amostra_vazia_falha():
    with pytest.raises(ValueError):
        resumir([])


@pytest.mark.parametrize("k,n", [(0, 10), (1, 50), (5, 10), (10, 10), (37, 200)])
def test_wilson_bate_com_scipy(k, n):
    p = wilson(k, n)
    esperado = sps.binomtest(k, n).proportion_ci(confidence_level=0.95, method="wilson")
    assert p.p == pytest.approx(k / n)
    assert p.ic95 == pytest.approx((esperado.low, esperado.high), abs=1e-9)


@pytest.mark.parametrize("k,n", [(-1, 10), (11, 10), (0, 0)])
def test_wilson_rejeita_entradas_invalidas(k, n):
    with pytest.raises(ValueError):
        wilson(k, n)


def test_wilson_contem_p_nos_extremos():
    # sem a proteção, o limite caía 1e-16 para dentro (ex.: n = 3) e a barra de erro ficava negativa
    for n in range(1, 500):
        assert wilson(n, n).ic95[1] == 1.0
        assert wilson(0, n).ic95[0] == 0.0
        w = wilson(n // 2, n)
        assert w.ic95[0] <= w.p <= w.ic95[1]

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

"""Sondagem de propagação do E3 (sem Docker): cliente criado antes da escrita e atraso da 1ª leitura registrado."""

import time
from types import SimpleNamespace

from lab import s3 as lab_s3
from lab.experiments.e3_replicacao import E3


def test_sondar_usa_cliente_pronto_e_registra_atraso_da_1a_leitura(monkeypatch):
    def proibido(*a, **k):
        raise AssertionError("cliente criado depois da confirmação do PUT")

    monkeypatch.setattr(lab_s3, "cliente_s3", proibido)
    leituras = iter([b"velho", b"novo"])

    def get(c, n, chave):
        t = get.t
        get.t += 0.004
        return SimpleNamespace(ok=True, dados=next(leituras), t_inicio=t, t_fim=t + 0.001)

    t_ack = time.monotonic()
    get.t = t_ack + 0.003
    monkeypatch.setattr(lab_s3, "get", get)
    por_no: dict = {}
    E3._sondar(object(), 2, "k", b"novo", t_ack, por_no)
    r = por_no["garage2"]
    assert r["leituras_antigas"] == 1
    assert round(r["atraso_1a_leitura_ms"], 1) == 3.0  # 1ª leitura 3 ms depois da confirmação
    assert round(r["propagacao_ms"], 1) == 7.0
